package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import org.tikv.common.Snapshot
import org.tikv.common.TiSession
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.TiBatchWriteException
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.txn.TwoPhaseCommitter
import org.tikv.txn.exception.WriteConflictException

internal class Storage(private val session: TiSession) {
    fun snapshot(): Snapshot = session.createSnapshot()

    fun read(snapshot: Snapshot, key: String): ObjectNode? =
        snapshot.get(key.encodeToByteArray()).takeIf { it.isNotEmpty() }?.let { json.readTree(it) as ObjectNode }

    fun tables(after: String, limit: Int): List<String> {
        val start = CATALOG + after + if (after.isEmpty()) "" else "\u0000"
        return snapshot().scan(ByteString.copyFromUtf8(start)).asSequence()
            .map { it.key.toStringUtf8() }.takeWhile { it.startsWith(CATALOG) }
            .map { it.removePrefix(CATALOG) }.take(limit).toList()
    }

    // Only retry a rejected prewrite. A commit failure may already have taken effect.
    fun mutate(key: String, change: (ObjectNode?) -> ObjectNode?): ObjectNode? {
        val bytes = key.encodeToByteArray()
        repeat(MAX_ATTEMPTS) {
            val snapshot = snapshot()
            val old = read(snapshot, key)
            val value = change(old)?.let(json::writeValueAsBytes) ?: byteArrayOf()
            TwoPhaseCommitter(session, snapshot.version).use { txn ->
                val budget = ConcreteBackOffer.newCustomBackOff(BACKOFF_MS)
                try {
                    txn.prewritePrimaryKey(budget, bytes, value)
                } catch (error: TiBatchWriteException) {
                    if (!error.isConflict()) throw error
                    return@repeat
                }
                txn.commitPrimaryKey(budget, bytes, session.timestamp.version)
                return old
            }
        }
        throw ApiError("ProvisionedThroughputExceededException", "Concurrent writes exceeded retry budget")
    }

    private fun Throwable.isConflict() = generateSequence(this) { it.cause }.any {
        it is WriteConflictException || it is KeyException && it.isRetryablePrewrite()
    }

    // Client 3.3.5 flattens some prewrite KeyError protobufs into exception messages.
    private fun KeyException.isRetryablePrewrite() =
        keyErr?.hasConflict() == true || message.orEmpty().startsWith("retryable:")

    companion object {
        const val CATALOG = "tikronos/v1/table/"
        private const val MAX_ATTEMPTS = 8
        private const val BACKOFF_MS = 3000
    }
}
