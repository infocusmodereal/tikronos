package tikronos

import tikronos.model.BackfillProgress
import tikronos.model.Index
import tikronos.model.ScanOptions
import tikronos.model.Table
import tikronos.storage.BackfillRecords
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.after
import tikronos.storage.bytes
import tikronos.storage.existingEntry
import tikronos.storage.prefix
import tikronos.storage.projected
import tikronos.storage.projectedTo
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

    internal fun backfill(table: Table, index: Index) {
        val progressKey = BackfillRecords.index(table, index)
        val expected = BackfillRecords.decode(db.read(db.snapshot(), progressKey), index.cursor ?: "")
        advance(table, index, expected)
    }

    internal fun advance(table: Table, index: Index, expected: BackfillProgress) {
        val progressKey = BackfillRecords.index(table, index)
        val cursor = checkNotNull(expected.cursor) { "Creating index has a completed checkpoint" }
        val prefix = table.prefix()
        val first = if (cursor.isEmpty()) bytes(prefix) else after(bytes(cursor))
        val rows = db.ranges.scan(first, prefixEnd(prefix), true,
            options = ScanOptions(limit = ROWS_PER_TICK, keyOnly = true)).map { it.key.toStringUtf8() }.toList()
        db.atomic { tx ->
            val current = tx.table(table.name)?.takeIf { it.id == table.id } ?: return@atomic
            val live = current.indexes.find { it.id == index.id && it.status == "CREATING" } ?: return@atomic
            if (BackfillRecords.decode(tx.read(progressKey), live.cursor ?: "") != expected) return@atomic
            val last = writeBatch(tx, current, live, rows)
            if (rows.size < ROWS_PER_TICK && last == rows.lastOrNull()) {
                val changed = current.copy(indexes = current.indexes.map {
                    if (it.id == index.id) it.copy(cursor = null, status = "ACTIVE") else it
                })
                tx.write(Storage.CATALOG + current.name, Records.encodeTable(changed))
                tx.write(progressKey, null)
            } else {
                tx.write(progressKey, BackfillRecords.encode(BackfillProgress(checkNotNull(last))))
            }
        }
    }

    /** Bound derived writes by bytes; small prefetch windows limit unused read dependencies. */
    private fun writeBatch(tx: Transaction, table: Table, index: Index, rows: List<String>): String? {
        val attributes = index.projected(table)
        var used = 0
        var last: String? = null
        for ((offset, address) in rows.withIndex()) {
            if (offset % PREFETCH_ROWS == 0)
                tx.prefetch(rows.subList(offset, minOf(offset + PREFETCH_ROWS, rows.size)))
            val item = tx.read(address)?.let { Records.itemAt(table, address, it) }
            val entry = item?.let { index.existingEntry(table, it) }
            if (entry != null) {
                val encoded = Records.encodeItem(checkNotNull(item).projectedTo(attributes))
                if (used > 0 && used + encoded.size > WRITE_BYTES) return last
                tx.write(entry, encoded)
                used += encoded.size
            }
            last = address
        }
        return last
    }

    companion object {
        private const val TABLES_PER_TICK = 20
        private const val ROWS_PER_TICK = 32
        private const val PREFETCH_ROWS = 8
        private const val WRITE_BYTES = 1024 * 1024
    }
}
