package tikronos

import tikronos.model.Index
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.after
import tikronos.storage.bytes
import tikronos.storage.existingEntry
import tikronos.storage.prefix
import tikronos.storage.prefixEnd

internal class IndexBackfill(private val db: Storage) {
    private var afterTable = ""

    fun tick() {
        val names = db.tables(afterTable, TABLES_PER_TICK)
        afterTable = if (names.size < TABLES_PER_TICK) "" else names.last()
        for (name in names) maintenanceAttempt("Index backfill for $name") {
            val table = db.read(db.snapshot(), Storage.CATALOG + name)?.let(Records::table)
            table?.indexes?.filter { it.status == "CREATING" }?.forEach { backfill(table, it) }
        }
    }

    private fun backfill(table: Table, index: Index) {
        val prefix = table.prefix()
        val first = index.cursor?.let { after(bytes(it)) } ?: bytes(prefix)
        val rows = db.ranges.scan(first, prefixEnd(prefix), true)
            .take(ROWS_PER_TICK).toList()
        var cursor = index.cursor
        for (row in rows) {
            val expected = cursor
            val advanced = db.atomic { tx ->
                val current = current(tx, table, index, expected) ?: return@atomic false
                val item = tx.read(row.key.toStringUtf8())?.let(Records::item)
                if (item != null) index.existingEntry(current, item)?.let {
                    tx.write(it, Records.encodeItem(item))
                }
                checkpoint(tx, current, index.id, row.key.toStringUtf8(), false)
                true
            }
            if (!advanced) return
            cursor = row.key.toStringUtf8()
        }
        if (rows.size < ROWS_PER_TICK) db.atomic { tx ->
            val current = current(tx, table, index, cursor) ?: return@atomic
            checkpoint(tx, current, index.id, null, true)
        }
    }

    private fun current(tx: Transaction, table: Table, index: Index, cursor: String?): Table? {
        val current = tx.read(Storage.CATALOG + table.name)?.let(Records::table)
        return current?.takeIf { it.id == table.id && it.indexes.any { candidate ->
            candidate.id == index.id && candidate.status == "CREATING" && candidate.cursor == cursor
        } }
    }

    private fun checkpoint(tx: Transaction, table: Table, id: String, cursor: String?, done: Boolean) {
        val changed = table.copy(indexes = table.indexes.map {
            if (it.id == id) it.copy(cursor = cursor, status = if (done) "ACTIVE" else "CREATING") else it
        })
        tx.write(Storage.CATALOG + table.name, Records.encodeTable(changed))
    }

    companion object {
        private const val TABLES_PER_TICK = 20
        private const val ROWS_PER_TICK = 32
    }
}
