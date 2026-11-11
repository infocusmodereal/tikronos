package tikronos

import tikronos.model.Item
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.projected
import tikronos.storage.projectedTo
import tikronos.storage.Transaction
import tikronos.storage.entry
import tikronos.storage.existingEntry

/** An ACTIVE entry is materialized; a CREATING index may not have reached this row yet. */
internal fun Transaction.maintainIndexes(
    table: Table, old: Item?, new: Item?, encoded: ByteArray?, projectedIndexes: Boolean,
) {
    for (index in table.indexes) {
        val before = old?.let { index.existingEntry(table, it) }
        val after = new?.let { index.entry(table, it) }
        if (before != null && before != after) write(before, null)
        if (after == null) continue
        val attributes = index.projected(table)
        val entry = new.projectedTo(attributes)
        val materialized = index.status == "ACTIVE" && (attributes == null || projectedIndexes)
        if (!materialized || before != after || old.projectedTo(attributes) != entry) {
            write(after, if (attributes == null) requireNotNull(encoded) else Records.encodeItem(entry))
        }
    }
}
