package tikronos

import java.util.UUID
import java.util.concurrent.TimeUnit
import tikronos.model.Index
import tikronos.model.ScanOptions
import tikronos.model.Table
import tikronos.storage.BackfillRecords
import tikronos.storage.MetadataFences
import tikronos.storage.Storage
import tikronos.storage.StreamRecords
import tikronos.storage.TtlRecords
import tikronos.storage.Transaction
import tikronos.storage.after
import tikronos.storage.bytes
import tikronos.storage.keyDigest
import tikronos.storage.prefix
import tikronos.storage.prefixEnd
import tikronos.storage.schema

/** DDL enqueues only immutable namespaces. A job's value is the next inclusive scan key. */
internal class RetiredRows(private val db: Storage) {
    private var afterJob = JOBS + keyDigest(UUID.randomUUID().toString())

    fun tick() {
        val start = System.nanoTime()
        do {
            val first = if (afterJob.isEmpty()) bytes(JOBS) else after(bytes(afterJob))
            val jobs = db.ranges.scan(first, prefixEnd(JOBS), true, options = ScanOptions(limit = JOB_BATCH))
                .toList()
            val wrapped = afterJob.isEmpty()
            for (job in jobs) {
                afterJob = job.key.toStringUtf8()
                maintenanceAttempt("retired rows") { advance(afterJob, job.value.toStringUtf8()) }
                if (System.nanoTime() - start >= BUDGET) return
            }
            if (jobs.size < JOB_BATCH) afterJob = ""
            if (wrapped && jobs.isEmpty()) return
        } while (System.nanoTime() - start < BUDGET)
    }

    internal fun advance(job: String, first: String) {
        val prefix = job.removePrefix(JOBS).substringAfter('/')
        require(job == jobKey(prefix) && first.startsWith(prefix))
        require((prefix.startsWith(Storage.ITEM) && prefix.length > Storage.ITEM.length) ||
            MetadataFences.isFenceNamespace(prefix))
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
        private const val JOBS = "tikronos/v1/retired-rows/"
        private const val JOB_BATCH = 4
        private const val ROW_BATCH = 512
        private val BUDGET = TimeUnit.MILLISECONDS.toNanos(150)

        internal fun jobKey(prefix: String) = JOBS + keyDigest(prefix) + "/" + prefix
        private fun enqueue(tx: Transaction, prefix: String) = tx.write(jobKey(prefix), prefix.encodeToByteArray())
        fun enqueue(tx: Transaction, table: Table) = enqueue(tx, table.prefix())

        fun retireConfig(tx: Transaction, key: String) {
            require(key.startsWith(StreamRecords.CONFIGS) ||
                key.startsWith(TtlRecords.CONFIGS))
            tx.write(key, null)
            enqueue(tx, MetadataFences.namespace(key))
        }

        fun retireIndex(tx: Transaction, table: Table, index: Index) {
            enqueue(tx, index.schema(table))
            tx.write(BackfillRecords.index(table, index), null)
        }
    }
}
