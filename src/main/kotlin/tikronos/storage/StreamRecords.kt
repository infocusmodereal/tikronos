package tikronos.storage

import tikronos.json
import tikronos.model.StreamState
import tikronos.obj

internal object StreamRecords {
    const val CONFIGS = "tikronos/v1/stream-config/"
    fun encode(state: StreamState): ByteArray = json.writeValueAsBytes(obj(
        "arn" to state.arn, "label" to state.label,
        "tableName" to state.tableName, "tableId" to state.tableId,
        "view" to state.view, "createdAt" to state.createdAt,
        "keys" to state.keys.map { obj("name" to it.first, "role" to it.second) },
        "enabled" to state.enabled, "head" to state.head,
        "trimmed" to state.trimmed, "closedAt" to (state.closedAt ?: 0),
    ))

    fun decode(bytes: ByteArray): StreamState {
        val n = json.readTree(bytes)
        return StreamState(n["arn"].textValue(), n["label"].textValue(),
            n["tableName"].textValue(), n["tableId"].textValue(),
            n["view"].textValue(), n["createdAt"].longValue(),
            n["keys"].map { it["name"].textValue() to it["role"].textValue() },
            n["enabled"].booleanValue(), n["head"].longValue(), n["trimmed"].longValue(),
            n["closedAt"].longValue().takeIf { it != 0L })
    }

    const val PREFIX = "tikronos/v1/streams/"
    fun config(tableId: String) = CONFIGS + tableId
    fun descriptor(arn: String) = PREFIX + arn
    fun trimKey(arn: String) = "tikronos/v1/stream-trim/$arn"

    /** Older descriptors carry the trim floor at upgrade. Subsequent progress lives in its own key. */
    fun trimmed(state: StreamState, watermark: ByteArray?): Long =
        maxOf(state.trimmed, watermark?.decodeToString()?.toLong() ?: 0L)

    fun events(state: StreamState) = "tikronos/v1/events/${state.label}/"
    fun sequence(value: Long) = value.toString().padStart(SEQUENCE_WIDTH, '0')
    private const val SEQUENCE_WIDTH = 20
}

/** Shard identity belongs to a stream generation, including disable/re-enable on one table. */
internal val StreamState.shard: String
    get() = "shardId-" + keyDigest(label).take(SHARD_DIGEST_CHARS)

private const val SHARD_DIGEST_CHARS = 32
