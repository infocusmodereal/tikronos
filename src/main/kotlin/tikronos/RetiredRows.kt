package tikronos

import tikronos.model.Index
import tikronos.model.ScanOptions
import tikronos.model.Table
import tikronos.storage.BackfillRecords
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.after
import tikronos.storage.bytes
import tikronos.storage.prefix
import tikronos.storage.prefixEnd
import tikronos.storage.schema

/** Only the DDL transaction that removes a generation may enqueue its immutable row prefix. */
internal class RetiredRows(private val db: Storage) {
    private var afterJob = ""

    fun tick() {
        val first = if (afterJob.isEmpty()) bytes(JOBS) else after(bytes(afterJob))
        val jobs = db.ranges.scan(first, prefixEnd(JOBS), true, options = ScanOptions(limit = JOB_BATCH)).toList()
        for (job in jobs) maintenanceAttempt("retired rows") {
            advance(job.key.toStringUtf8(), job.value.toStringUtf8())
        }
        afterJob = if (jobs.size < JOB_BATCH) "" else jobs.last().key.toStringUtf8()
    }

    internal fun advance(job: String, first: String) {
        val prefix = Storage.itemKey(job.removePrefix(JOBS), "")
        require(job.startsWith(JOBS) && first.startsWith(prefix))
        val rows = db.ranges.scan(bytes(first), prefixEnd(prefix), true,
            options = ScanOptions(limit = ROW_BATCH, keyOnly = true)).map { it.key.toStringUtf8() }.toList()
        db.atomic { tx ->
            if (tx.read(job)?.decodeToString() == first) {
                rows.forEach { tx.write(it, null) }
                val next = if (rows.size < ROW_BATCH) null else (rows.last() + "\u0000").encodeToByteArray()
                tx.write(job, next)
            }
        }
    }

    companion object {
        internal const val JOBS = "tikronos/v1/retired-rows/"
        private const val JOB_BATCH = 4
        private const val ROW_BATCH = 64

        fun enqueue(tx: Transaction, table: Table) = tx.write(JOBS + table.id, table.prefix().encodeToByteArray())

        fun retireIndex(tx: Transaction, table: Table, index: Index) {
            enqueue(tx, index.schema(table))
            tx.write(BackfillRecords.index(table, index), null)
        }
    }
}
