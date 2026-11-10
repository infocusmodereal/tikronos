package tikronos

import tikronos.model.Item
import tikronos.model.Table
import tikronos.storage.Transaction
import tikronos.storage.entry
import tikronos.storage.existingEntry

/** Each index retains the full new item, using the same encoded bytes as the base row. */
internal fun maintainIndexes(tx: Transaction, table: Table, old: Item?, new: Item?, encoded: ByteArray?) {
    for (index in table.indexes) {
        val before = old?.let { index.existingEntry(table, it) }
        val after = new?.let { index.entry(table, it) }
        if (before != null && before != after) tx.write(before, null)
        if (after != null) tx.write(after, requireNotNull(encoded))
    }
}
