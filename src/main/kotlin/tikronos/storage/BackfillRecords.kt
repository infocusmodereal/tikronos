package tikronos.storage

import tikronos.json
import tikronos.model.BackfillProgress
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.TtlState
import tikronos.obj

/** Operational progress is generation-scoped and never participates in metadata fences. */
internal object BackfillRecords {
    private const val PREFIX = "tikronos/v1/backfill/"
    fun index(table: Table, index: Index) = "${PREFIX}index/${table.id}/${index.id}"
    fun ttl(state: TtlState) = "${PREFIX}ttl/${state.tableId}/${state.generation}"

    fun encode(progress: BackfillProgress): ByteArray = json.writeValueAsBytes(obj(
        "done" to (progress.cursor == null), "cursor" to (progress.cursor ?: "")))

    /** A missing record resumes the embedded cursor written by earlier versions. */
    fun decode(bytes: ByteArray?, legacyCursor: String?): BackfillProgress {
        if (bytes == null) return BackfillProgress(legacyCursor)
        val node = json.readTree(bytes)
        check(node.path("done").isBoolean && node.path("cursor").isTextual) { "Invalid backfill checkpoint" }
        return BackfillProgress(node["cursor"].textValue().takeUnless { node["done"].booleanValue() })
    }
}
