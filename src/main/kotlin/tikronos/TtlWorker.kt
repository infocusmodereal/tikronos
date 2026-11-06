package tikronos

import java.time.Instant
import tikronos.model.TtlState
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.TtlRecords
import tikronos.storage.after
import tikronos.storage.bytes
import tikronos.storage.prefixEnd
import tikronos.storage.prefix
import tikronos.storage.address

internal class TtlWorker(private val db: Storage, private val ttl: Ttl) {
    private var configAfter = ""
    private var candidateAfter = ""

    fun tick(writer: ItemWriter) {
        val configs = TtlRecords.CONFIGS
        val first = if (configAfter.isEmpty()) bytes(configs) else after(bytes(configAfter))
        val page = db.ranges.scan(first, prefixEnd(configs), true)
            .take(BATCH_SIZE).toList()
        configAfter = if (page.size < BATCH_SIZE) "" else page.last().key.toStringUtf8()
        for (row in page) maintenanceAttempt("TTL backfill") {
            val state = TtlRecords.state(row.value.toByteArray())
            if (state.enabled && state.cursor != null) backfill(state)
        }
        val now = Instant.now().epochSecond
        val low = if (candidateAfter.isEmpty()) bytes(TtlRecords.CANDIDATES) else after(bytes(candidateAfter))
        val candidates = db.ranges.scan(low, bytes(TtlRecords.dueEnd(now)), true)
            .take(BATCH_SIZE).toList()
        candidateAfter = if (candidates.size < BATCH_SIZE) "" else candidates.last().key.toStringUtf8()
        for (row in candidates) maintenanceAttempt("TTL candidate") {
            expire(row.key.toStringUtf8(), TtlRecords.candidate(row.value.toByteArray()), now, writer)
        }
    }

    private fun expire(address: String, candidate: tikronos.model.ExpiryCandidate, now: Long, writer: ItemWriter) {
        db.atomic { tx ->
            val table = tx.read(Storage.CATALOG + candidate.tableName)?.let(Records::table)
            val config = tx.read(TtlRecords.config(candidate.tableId))?.let(TtlRecords::state)
            if (table?.id == candidate.tableId && config?.enabled == true &&
                config.generation == candidate.generation) {
                val item = tx.read(table.address(candidate.key))?.let(Records::item)
                val expiry = item?.let { Ttl.expiration(it, config.attribute, now) }
                if (expiry == candidate.expiry && expiry <= now)
                    writer.change(tx, table, candidate.key, ttl = true) { null }
            }
            tx.write(address, null)
        }
    }

    private fun backfill(state: TtlState) {
        val table = db.read(db.snapshot(), Storage.CATALOG + state.tableName)?.let(Records::table)
            ?.takeIf { it.id == state.tableId } ?: return
        val prefix = table.prefix()
        val first = state.cursor?.takeIf { it.isNotEmpty() }?.let { after(bytes(it)) } ?: bytes(prefix)
        val rows = db.ranges.scan(first, prefixEnd(prefix), true).take(BATCH_SIZE).toList()
        db.atomic { tx ->
            val live = tx.read(Storage.CATALOG + table.name)?.let(Records::table)
            val current = tx.read(TtlRecords.config(table.id))?.let(TtlRecords::state)
            if (live?.id == table.id && current == state) {
                for (row in rows) {
                    val item = tx.read(row.key.toStringUtf8())?.let(Records::item)
                    ttl.track(tx, table, null, item)
                }
                val next = if (rows.size < BATCH_SIZE) null else rows.last().key.toStringUtf8()
                tx.write(TtlRecords.config(table.id), TtlRecords.encode(state.copy(cursor = next)))
            }
        }
    }

    companion object {
        private const val BATCH_SIZE = 32
    }
}
