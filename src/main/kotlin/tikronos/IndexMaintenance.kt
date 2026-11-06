package tikronos

import tikronos.model.Index
import tikronos.model.Item
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.Transaction
import tikronos.storage.entry
import tikronos.storage.existingEntry

internal class IndexMaintenance {
    fun update(tx: Transaction, table: Table, old: Item?, new: Item?) {
        for (index in table.indexes) maintain(tx, table, index, old, new)
    }

    private fun maintain(tx: Transaction, table: Table, index: Index, old: Item?, new: Item?) {
        val before = old?.let { index.existingEntry(table, it) }
        val after = new?.let { index.entry(table, it) }
        if (before != null && before != after) tx.write(before, null)
        if (after != null) tx.write(after, Records.encodeItem(requireNotNull(new)))
    }
}
