package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import org.tikv.common.Snapshot
import tikronos.model.ScanOptions
import tikronos.model.StreamPosition
import tikronos.model.StreamState
import tikronos.storage.Storage
import tikronos.storage.StreamRecords
import tikronos.storage.shard
import tikronos.storage.after
import tikronos.storage.timestampStartOfSecond
import tikronos.storage.bytes
import tikronos.storage.readBatch
import tikronos.storage.prefixEnd

internal class StreamReads(private val db: Storage) {
    private val tokens = StreamTokens(db)

    fun list(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "Limit", "ExclusiveStartStreamArn")
        val name = if (req.has("TableName")) Tables.tableName(req) else null
        val limit = req.integer("Limit", 1..MAX_STREAMS) ?: MAX_STREAMS
        val prefix = StreamRecords.PREFIX
        val start = req.get("ExclusiveStartStreamArn")?.let { req.string("ExclusiveStartStreamArn") }
        val first = if (start == null) bytes(prefix) else after(bytes(StreamRecords.descriptor(start)))
        val snap = db.snapshot()
        val states = db.ranges.scan(first, prefixEnd(prefix), true, snap)
            .map { StreamRecords.decode(it.value.toByteArray()) }
            .filter { (name == null || it.tableName == name) && retained(it) }
            .take(limit + 1).toList()
        return obj("Streams" to states.take(limit).map {
            obj("StreamArn" to it.arn, "TableName" to it.tableName, "StreamLabel" to it.label)
        }).apply { if (states.size > limit) put("LastEvaluatedStreamArn", states[limit - 1].arn) }
    }

    fun describe(req: ObjectNode): ObjectNode {
        req.fieldsOnly("StreamArn", "Limit", "ExclusiveStartShardId")
        req.integer("Limit", 1..MAX_STREAMS)
        val state = find(req.string("StreamArn"), db.snapshot())
        val after = if (req.has("ExclusiveStartShardId")) req.string("ExclusiveStartShardId") else null
        if (after != null && after != state.shard) invalid("Invalid ExclusiveStartShardId")
        val first = if (state.enabled) state.trimmed + 1 else minOf(state.trimmed + 1, state.head)
        val range = obj("StartingSequenceNumber" to StreamRecords.sequence(first))
        if (!state.enabled) range.put("EndingSequenceNumber", StreamRecords.sequence(state.head))
        val shard = obj("ShardId" to state.shard, "SequenceNumberRange" to range)
        return obj("StreamDescription" to obj("StreamArn" to state.arn, "StreamLabel" to state.label,
            "StreamStatus" to if (state.enabled) "ENABLED" else "DISABLED",
            "StreamViewType" to state.view, "CreationRequestDateTime" to state.createdAt,
            "TableName" to state.tableName, "KeySchema" to state.keys.map {
                obj("AttributeName" to it.first, "KeyType" to it.second)
            }, "Shards" to if (after == null) listOf(shard) else emptyList<ObjectNode>()))
    }

    fun iterator(req: ObjectNode): ObjectNode {
        req.fieldsOnly("StreamArn", "ShardId", "ShardIteratorType", "SequenceNumber")
        val state = find(req.string("StreamArn"), db.snapshot())
        if (req.string("ShardId") != state.shard) missingStream()
        val type = req.string("ShardIteratorType")
        val sequence = if (req.has("SequenceNumber")) {
            req.string("SequenceNumber").toLongOrNull()?.takeIf { it >= 0 } ?: invalid("Invalid SequenceNumber")
        } else null
        if ((type in setOf("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER")) != (sequence != null))
            invalid("SequenceNumber must accompany sequence iterator types")
        val position = when (type) {
            "TRIM_HORIZON" -> state.trimmed
            "LATEST" -> state.head
            "AT_SEQUENCE_NUMBER" -> requireNotNull(sequence) - 1
            "AFTER_SEQUENCE_NUMBER" -> requireNotNull(sequence)
            else -> invalid("Invalid ShardIteratorType")
        }
        if (position < state.trimmed) trimmed()
        if (position > state.head) invalid("SequenceNumber is beyond the stream")
        return obj("ShardIterator" to token(state.arn, position))
    }

    fun records(req: ObjectNode): ObjectNode {
        req.fieldsOnly("ShardIterator", "Limit")
        val limit = req.integer("Limit", 1..MAX_RECORDS) ?: MAX_RECORDS
        val position = tokens.decode(req.string("ShardIterator"), Instant.now().epochSecond)
        val snap = db.snapshot()
        val state = find(position.arn, snap)
        if (position.after < state.trimmed) trimmed()
        val prefix = StreamRecords.events(state)
        val first = after(bytes(prefix + StreamRecords.sequence(position.after)))
        val end = after(bytes(prefix + StreamRecords.sequence(state.head)))
        val rows = db.ranges.scan(first, end, true, snap, ScanOptions(limit = limit)).iterator()
        val records = mutableListOf<ObjectNode>()
        var cursor = position.after
        var size = 0
        while (records.size < limit && size < PAGE_BYTES && rows.hasNext()) {
            val row = rows.next()
            val data = json.readTree(row.value.toByteArray()) as ObjectNode
            val recordBytes = data.path("dynamodb").path("SizeBytes").intValue()
            if (size + recordBytes > PAGE_BYTES && records.isNotEmpty()) break
            records += data
            size += recordBytes
            cursor = data.path("dynamodb").path("SequenceNumber").asText().toLong()
        }
        return obj("Records" to records).apply {
            if (state.enabled || cursor < state.head) put("NextShardIterator", token(state.arn, cursor))
        }
    }

    private fun find(arn: String, snapshot: Snapshot): StreamState {
        val descriptor = StreamRecords.descriptor(arn)
        val trimKey = StreamRecords.trimKey(arn)
        val metadata = snapshot.readBatch(listOf(descriptor, trimKey))
        val state = metadata[descriptor]?.let(StreamRecords::decode)
            ?.takeIf(::retained) ?: missingStream()
        // Enforce logical retention even if physical cleanup is behind. The sequence embeds PD time.
        val cutoff = timestampStartOfSecond(Instant.now().epochSecond - STREAM_RETENTION_SECONDS + 1)
        val prefix = StreamRecords.events(state)
        val expired = db.ranges.scan(bytes(prefix), bytes(prefix + StreamRecords.sequence(cutoff)),
            false, snapshot, ScanOptions(limit = 1, keyOnly = true))
            .firstOrNull()?.key?.toStringUtf8()?.substringAfterLast('/')?.toLong() ?: 0
        return state.copy(trimmed = maxOf(StreamRecords.trimmed(state, metadata[trimKey]), expired))
    }

    private fun retained(state: StreamState) = state.closedAt == null ||
        state.closedAt > Instant.now().epochSecond - STREAM_RETENTION_SECONDS

    private fun token(arn: String, position: Long) = tokens.encode(
        StreamPosition(arn, position, Instant.now().epochSecond + ITERATOR_SECONDS))

    companion object {
        private const val MAX_STREAMS = 100
        private const val MAX_RECORDS = 1000
        private const val ITERATOR_SECONDS = 15 * 60L
    }
}

internal const val STREAM_RETENTION_SECONDS = 24 * 60 * 60L

private fun missingStream(): Nothing = throw ApiError("ResourceNotFoundException", "Stream or shard does not exist")
private fun trimmed(): Nothing = throw ApiError("TrimmedDataAccessException", "Requested records have expired")
