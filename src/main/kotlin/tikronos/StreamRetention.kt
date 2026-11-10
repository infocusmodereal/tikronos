package tikronos

import java.time.Instant
import tikronos.model.ScanOptions
import tikronos.model.StreamState
import tikronos.storage.Storage
import tikronos.storage.StreamRecords
import tikronos.storage.bytes
import tikronos.storage.prefixEnd
import tikronos.storage.after

/** Small transactions keep retention work bounded while readers retain MVCC-consistent pages. */
internal class StreamRetention(private val db: Storage) {
    private var cursor = ""

    fun tick() {
        val prefix = StreamRecords.PREFIX
        val first = if (cursor.isEmpty()) bytes(prefix) else after(bytes(cursor))
        val descriptors = db.ranges.scan(first, prefixEnd(prefix), true,
            options = ScanOptions(limit = DESCRIPTOR_BATCH)).toList()
        cursor = if (descriptors.size < DESCRIPTOR_BATCH) "" else descriptors.last().key.toStringUtf8()
        val cutoff = Instant.now().epochSecond - STREAM_RETENTION_SECONDS
        for (row in descriptors) maintenanceAttempt("stream retention") {
            prune(StreamRecords.decode(row.value.toByteArray()), cutoff)
        }
    }

    private fun prune(state: StreamState, cutoff: Long) {
        val events = StreamRecords.events(state)
        val expired = db.ranges.scan(bytes(events), prefixEnd(events), true,
            options = ScanOptions(limit = BATCH_SIZE)).takeWhile {
                json.readTree(it.value.toByteArray()).path("dynamodb")
                    .path("ApproximateCreationDateTime").longValue() <= cutoff
            }.toList()
        if (expired.isEmpty()) return
        db.atomic { tx ->
            val current = tx.read(StreamRecords.descriptor(state.arn))?.let(StreamRecords::decode)
            if (current != null) {
                expired.forEach { tx.write(it.key.toStringUtf8(), null) }
                val last = expired.last().key.toStringUtf8().substringAfterLast('/').toLong()
                tx.write(StreamRecords.descriptor(state.arn),
                    StreamRecords.encode(current.copy(trimmed = maxOf(current.trimmed, last))))
            }
        }
    }

    companion object {
        private const val BATCH_SIZE = 64
        private const val DESCRIPTOR_BATCH = 16
    }
}
