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

/** Primary commit is the only decision point; never replay after attempting it. */
internal class AtomicCommit(
    private val session: TiSession,
    private val cleanup: Executor = Executor { it.run() },
    private val timestamp: () -> TiTimestamp = { session.timestamp },
) {
    @Suppress("TooGenericExceptionCaught", "ReturnCount") // Distinct safe aborts before a primary decision.
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
        val mutations =
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
        try {
            prewrite(mutations, values, primary, start, lockTtl)
        } catch (error: Exception) {
            // Even a transport-ambiguous prewrite is safe to abort: commit was never attempted.
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
            SingleKeyCommit(session, timestamp).commit(primary, start, commitTs, ceiling)
        } catch (_: CommitRefused) {
            // Explicit refusal means no primary decision exists. Never do this after transport ambiguity.
            resolve(entries, start, 0)
            return false
        }
        // Failure here cannot undo the durable primary decision. Readers resolve remaining locks.
        val secondaries = entries.drop(1)
        if (secondaries.isNotEmpty()) cleanup.execute { resolve(secondaries, start, committed) }
        return true
    }

    private fun prewrite(
        mutations: Map<ByteString, Kvrpcpb.Mutation>,
        values: Map<String, ByteArray?>,
        primary: ByteString,
        start: Long,
        lockTtl: Long,
    ) {
        val blindFences = values.keys.filter(MetadataFences::isFence).map(::bytes).toSet()
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
                Prewrite(session).send(
                    region, budget, primary, group.map(mutations::getValue),
                    start, lockTtl, blind, mutations.size,
                )
                remaining = remaining.drop(group.size)
            }
        }
    }

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
