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

    internal fun prune(state: StreamState, cutoff: Long) {
        val events = StreamRecords.events(state)
        val rows = db.ranges.scan(bytes(events), prefixEnd(events), true,
            options = ScanOptions(limit = BATCH_SIZE)).iterator()
        if (!rows.hasNext()) {
            // A closed generation cannot append. Finish only after its last event is gone.
            if (state.closedAt != null && state.closedAt <= cutoff) finishClosed(state)
            return
        }
        val expired = rows.asSequence().takeWhile {
                json.readTree(it.value.toByteArray()).path("dynamodb")
                    .path("ApproximateCreationDateTime").longValue() <= cutoff
            }.map { it.key.toStringUtf8().substringAfterLast('/').toLong() }.toList()
        deleteBatch(state, expired)
    }

    private fun finishClosed(state: StreamState) {
        db.atomic { tx ->
            val descriptor = StreamRecords.descriptor(state.arn)
            val config = StreamRecords.config(state.tableId)
            if (tx.read(descriptor) != null) {
                if (tx.read(config)?.decodeToString() == state.arn) {
                    // DescribeTable still exposes the latest disabled stream of a live generation.
                    if (tx.table(state.tableName)?.id == state.tableId) return@atomic
                    tx.write(config, null)
                }
                tx.write(descriptor, null)
                tx.write(StreamRecords.trimKey(state.arn), null)
            }
        }
    }

    /** A slower cleaner can submit an expired prefix after a newer batch has already committed. */
    internal fun deleteBatch(state: StreamState, expired: List<Long>) {
        if (expired.isEmpty()) return
        val key = StreamRecords.trimKey(state.arn)
        val prefix = StreamRecords.events(state)
        val last = expired.last()
        db.atomic { tx ->
            // Competing cleaners serialize here. Events are immutable and their namespace is never reused.
            val watermark = tx.read(key)
            if (last > StreamRecords.trimmed(state, watermark) &&
                (watermark != null || tx.read(prefix + StreamRecords.sequence(last)) != null)) {
                expired.forEach { tx.write(prefix + StreamRecords.sequence(it), null) }
                tx.write(key, StreamRecords.sequence(last).encodeToByteArray())
            }
        }
    }

    companion object {
        private const val BATCH_SIZE = 64
        private const val DESCRIPTOR_BATCH = 16
    }
}
