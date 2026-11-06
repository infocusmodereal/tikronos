package tikronos.storage

import java.util.concurrent.TimeUnit
import tikronos.invalid
import java.util.logging.Level
import java.util.logging.Logger
import org.tikv.common.TiSession
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.RegionException
import org.tikv.common.util.BackOffFunction.BackOffFuncType
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb
import org.tikv.txn.exception.WriteConflictException

/** Primary commit is the only decision point; never replay after attempting it. */
internal class AtomicCommit(private val session: TiSession) {
    @Suppress("TooGenericExceptionCaught") // Abort every prewrite failure before a commit decision.
    fun commit(start: Long, values: Map<String, ByteArray?>, guards: Set<String> = emptySet()): Boolean {
        val entries = (values.keys + guards).sorted()
        if (entries.any { it.encodeToByteArray().size > MAX_KEY_BYTES })
            invalid("Encoded key exceeds storage limit")
        val primary = bytes(entries.first())
        // TiKV measures TTL from startTs, including time spent reading before prewrite.
        val lockTtl = maxOf(0, timestampMillis(session.timestamp.version) - timestampMillis(start)) + LOCK_TTL_MS
        val retry = ConcreteBackOffer.newCustomBackOff(BUDGET_MS)
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BUDGET_MS.toLong())
        try {
            for (key in entries) {
                val value = values[key]
                check(System.nanoTime() < deadline) { "Prewrite deadline exceeded" }
                val mutation = Kvrpcpb.Mutation.newBuilder()
                    .setKey(bytes(key))
                    .setOp(when {
                        key in guards -> Kvrpcpb.Op.Lock
                        value == null -> Kvrpcpb.Op.Del
                        else -> Kvrpcpb.Op.Put
                    })
                    .setValue(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(value ?: byteArrayOf()))
                    .build()
                prewrite(primary, mutation, start, retry, lockTtl)
            }
        } catch (error: Exception) {
            // Even a transport-ambiguous prewrite is safe to abort: commit was never attempted.
            resolve(entries, start, 0)
            if (prewriteConflict(error)) return false
            throw error
        }
        val committed = SingleKeyCommit(session).commit(primary, start, session.timestamp.version)
        // Failure here cannot undo the durable primary decision. Readers resolve remaining locks.
        resolve(entries.drop(1), start, committed)
        return true
    }

    private fun prewriteConflict(error: Throwable): Boolean =
        generateSequence(error) { it.cause }.any {
            it is WriteConflictException || it is KeyException &&
                (it.keyErr?.hasConflict() == true || it.keyErr?.hasLocked() == true ||
                    it.message.orEmpty().startsWith("retryable:"))
        }

    private fun prewrite(
        primary: org.tikv.shade.com.google.protobuf.ByteString,
        mutation: Kvrpcpb.Mutation,
        start: Long,
        retry: ConcreteBackOffer,
        lockTtl: Long,
    ) {
        while (true) {
            try {
                session.regionStoreClientBuilder.build(mutation.key, retry).use {
                    it.prewrite(retry, primary, listOf(mutation), start, lockTtl)
                }
                return
            } catch (error: RegionException) {
                retry.doBackOff(BackOffFuncType.BoRegionMiss, error)
            }
        }
    }

    @Suppress("TooGenericExceptionCaught") // Cleanup is best effort; the primary decides recovery.
    private fun resolve(keys: List<String>, start: Long, committed: Long) {
        try {
            val resolver = TransactionResolution(session)
            keys.forEach { resolver.resolve(bytes(it), start, committed) }
        } catch (error: Exception) {
            logger.log(Level.WARNING, "Deferred transaction lock cleanup to readers", error)
        }
    }

    companion object {
        // Leave room for TiKV memcomparable encoding and the MVCC version under its 8 KiB limit.
        private const val MAX_KEY_BYTES = 7 * 1024
        private const val LOCK_TTL_MS = 10_000L
        private const val BUDGET_MS = 3000
        private val logger = Logger.getLogger(AtomicCommit::class.java.name)
    }
}
