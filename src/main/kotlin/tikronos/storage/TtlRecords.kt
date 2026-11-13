package tikronos.storage

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.json
import tikronos.model.ExpiryCandidate
import tikronos.model.TtlState
import tikronos.obj

internal object TtlRecords {
    const val CONFIGS = "tikronos/v1/ttl-config/"
    const val CANDIDATES = "tikronos/v1/expiry/"
    fun config(id: String) = CONFIGS + id
    fun address(candidate: ExpiryCandidate, itemAddress: String) =
        CANDIDATES + candidate.expiry.toString().padStart(TIME_WIDTH, '0') + "/" +
            candidate.tableId + "/" + candidate.generation + "/" +
            keyDigest(itemAddress)

    fun encode(state: TtlState): ByteArray = json.writeValueAsBytes(obj(
        "tableName" to state.tableName, "tableId" to state.tableId,
        "attribute" to state.attribute, "enabled" to state.enabled,
        "generation" to state.generation, "done" to (state.cursor == null),
        "cursor" to (state.cursor ?: "")))

    fun state(bytes: ByteArray): TtlState {
        val n = json.readTree(bytes)
        return TtlState(n["tableName"].textValue(), n["tableId"].textValue(),
            n["attribute"].textValue(), n["enabled"].booleanValue(), n["generation"].textValue(),
            n["cursor"].textValue().takeUnless { n["done"].booleanValue() })
    }

    fun encode(candidate: ExpiryCandidate): ByteArray = json.writeValueAsBytes(obj(
        "tableName" to candidate.tableName, "tableId" to candidate.tableId,
        "generation" to candidate.generation, "expiry" to candidate.expiry).apply {
            set<ObjectNode>("key", wireItem(candidate.key))
        })

    fun candidate(bytes: ByteArray): ExpiryCandidate {
        val n = json.readTree(bytes)
        return ExpiryCandidate(n["tableName"].textValue(), n["tableId"].textValue(),
            n["generation"].textValue(), n["expiry"].longValue(), parseItem(n["key"]))
    }

    fun dueEnd(now: Long) = CANDIDATES + (now + 1).toString().padStart(TIME_WIDTH, '0')
    private const val TIME_WIDTH = 20
}
