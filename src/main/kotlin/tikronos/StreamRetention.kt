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
        val key = StreamRecords.trimKey(state.arn)
        val last = expired.last().key.toStringUtf8().substringAfterLast('/').toLong()
        db.atomic { tx ->
            // Competing cleaners serialize here. Events are immutable and their namespace is never reused.
            if (last > StreamRecords.trimmed(state, tx.read(key))) {
                expired.forEach { tx.write(it.key.toStringUtf8(), null) }
                tx.write(key, StreamRecords.sequence(last).encodeToByteArray())
            }
        }
    }

    companion object {
        private const val BATCH_SIZE = 64
        private const val DESCRIPTOR_BATCH = 16
    }
}
