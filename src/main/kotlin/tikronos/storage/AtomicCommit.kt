package tikronos.storage

import java.util.concurrent.TimeUnit
import java.util.concurrent.Executor
import java.util.logging.Level
import java.util.logging.Logger
import org.tikv.common.TiSession
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.exception.KeyException
import org.tikv.kvproto.Kvrpcpb
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.txn.exception.WriteConflictException
import tikronos.invalid
import tikronos.model.OnePhaseBounds

/** A 1PC prewrite or the 2PC primary commit can decide the transaction; never replay ambiguity. */
@Suppress("LongParameterList") // Protocol components and bounds remain independently injectable.
internal class AtomicCommit(
    private val session: TiSession,
    private val cleanup: Executor = Executor { it.run() },
    private val timestamp: () -> TiTimestamp = { session.timestamp },
    private val prewriter: Prewrite = Prewrite(session),
    private val onePhase: Boolean = true,
    private val primaryCommit: SingleKeyCommit = SingleKeyCommit(session, timestamp),
) {
    @Suppress("TooGenericExceptionCaught", "ReturnCount", "ThrowsCount") // Distinct safe aborts and unknown outcomes.
    fun commit(
        start: Long,
        values: Map<String, ByteArray?>,
        guards: Set<String> = emptySet(),
        started: Long = System.nanoTime(),
        ceiling: Long = Long.MAX_VALUE,
    ): Boolean {
        // The primary must be an ordinary, conflict-checked key, never a blind fence write.
        val entries =
            (values.keys + guards).sortedWith(compareBy(MetadataFences::isFence).thenBy { it })
        if (entries.any { it.encodeToByteArray().size > MAX_KEY_BYTES })
            invalid("Encoded key exceeds storage limit")
        val primary = bytes(entries.first())
        check(!MetadataFences.isFence(entries.first())) {
            "A fence cannot be the primary decision key"
        }
        // TiKV measures TTL from startTs, including time spent reading before prewrite.
        val lockTtl =
            TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - started).coerceAtLeast(0)) +
                LOCK_TTL_MS
        val mutations = mutations(entries, values, guards)
        // Obtain a fresh PD lower bound only when routing yields the complete mutation set.
        val bounds = if (eligible(values, guards)) lazy(LazyThreadSafetyMode.NONE) {
            OnePhaseBounds(maxOf(start, timestamp().version) + 1, ceiling)
        } else null
        try {
            val decision = prewrite(mutations, values, primary, start, lockTtl, bounds)
            if (decision > 0) return true
        } catch (_: ExpiredBeforePrewrite) {
            return false
        } catch (unknown: CommitUnknown) {
            // A lost 1PC response may conceal a durable commit. No rollback or fresh replay.
            throw unknown
        } catch (error: Exception) {
            if (bounds?.isInitialized() == true && !explicitKeyError(error)) throw CommitUnknown(error)
            // Explicit key errors and ordinary 2PC prewrite failures have no commit decision.
            resolve(entries, start, 0)
            if (prewriteConflict(error)) return false
            throw error
        }
        val commitTs = try {
            timestamp().version
        } catch (error: Exception) {
            // No commit RPC has been attempted, so aborting remains unambiguous.
            resolve(entries, start, 0)
            throw error
        }
        val committed = try {
            primaryCommit.commit(primary, start, commitTs, ceiling)
        } catch (_: CommitRefused) {
            // Explicit refusal means no primary decision exists. Never do this after transport ambiguity.
            resolve(entries, start, 0)
            return false
        } catch (error: CommitNotApplied) {
            // No decision was applied; clean up without replaying a failed storage operation.
            resolve(entries, start, 0)
            throw error
        }
        // Failure here cannot undo the durable primary decision. Readers resolve remaining locks.
        val secondaries = entries.drop(1)
        if (secondaries.isNotEmpty()) cleanup.execute { resolve(secondaries, start, committed) }
        return true
    }

    private fun mutations(
        entries: List<String>, values: Map<String, ByteArray?>, guards: Set<String>,
    ): Map<ByteString, Kvrpcpb.Mutation> =
            entries.associate { key ->
                val value = values[key]
                val mutation =
                    Kvrpcpb.Mutation.newBuilder()
                        .setKey(bytes(key))
                        .setOp(
                            when {
                                key in guards -> Kvrpcpb.Op.Lock
                                value == null -> Kvrpcpb.Op.Del
                                else -> Kvrpcpb.Op.Put
                            }
                        )
                        .setValue(ByteString.copyFrom(value ?: byteArrayOf()))
                        .build()
                mutation.key to mutation
            }

    private fun eligible(values: Map<String, ByteArray?>, guards: Set<String>): Boolean {
        if (!onePhase || guards.isNotEmpty()) return false
        val indexes = "${Storage.ITEM}index/"
        return values.keys.all { it.startsWith(Storage.ITEM) } &&
            values.keys.count { !it.startsWith(indexes) } == 1
    }

    private class ExpiredBeforePrewrite : RuntimeException()

    @Suppress("LongParameterList") // Native prewrite fields and optional 1PC bounds.
    private fun prewrite(
        mutations: Map<ByteString, Kvrpcpb.Mutation>,
        values: Map<String, ByteArray?>,
        primary: ByteString,
        start: Long,
        lockTtl: Long,
        bounds: Lazy<OnePhaseBounds>?,
    ): Long {
        var committed = 0L
        val blindFences = values.asSequence().filter { (key, value) -> value != null && MetadataFences.isFence(key) }
            .map { bytes(it.key) }.toSet()
        RegionBatches(session).execute(
            mutations.keys.toList(),
            size = { mutations.getValue(it).serializedSize },
        ) { region, batch, budget ->
            var remaining = batch
            while (remaining.isNotEmpty()) {
                val blind = remaining.first() in blindFences
                val group = remaining.takeWhile { (it in blindFences) == blind }
                // Preserve lock order even when a metadata change also reads other metadata.
                // Only constant fence writes skip committed-version checks; live locks remain
                // checked.
                val onePhase = if (group.size == mutations.size) bounds?.value else null
                if (onePhase != null && onePhase.minimum > onePhase.maximum) throw ExpiredBeforePrewrite()
                committed = prewriter.send(
                    region, budget, primary, group.map(mutations::getValue),
                    start, lockTtl, blind, mutations.size, onePhase,
                )
                remaining = remaining.drop(group.size)
            }
        }
        return committed
    }

    private fun explicitKeyError(error: Exception) = error is KeyException || error is WriteConflictException

    private fun prewriteConflict(error: Throwable): Boolean =
        generateSequence(error) { it.cause }
            .any {
                it is WriteConflictException ||
                    it is KeyException &&
                        (it.keyErr?.hasConflict() == true ||
                            it.keyErr?.hasLocked() == true ||
                            !it.keyErr?.retryable.isNullOrEmpty() ||
                            it.message.orEmpty().startsWith("retryable:"))
            }

    @Suppress("TooGenericExceptionCaught") // Cleanup is best effort; the primary decides recovery.
    private fun resolve(keys: List<String>, start: Long, committed: Long) {
        try {
            val resolver = TransactionResolution(session)
            resolver.resolve(keys.map(::bytes), start, committed)
        } catch (error: Exception) {
            logger.log(Level.WARNING, "Deferred transaction lock cleanup to readers", error)
        }
    }

    companion object {
        // Leave room for TiKV memcomparable encoding and the MVCC version under its 8 KiB limit.
        private const val MAX_KEY_BYTES = 7 * 1024
        private const val LOCK_TTL_MS = 10_000L
        private val logger = Logger.getLogger(AtomicCommit::class.java.name)
    }
}
