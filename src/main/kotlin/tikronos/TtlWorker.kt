package tikronos

import java.time.Instant
import tikronos.model.BackfillProgress
import tikronos.model.ScanOptions
import tikronos.model.TtlState
import tikronos.storage.BackfillRecords
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.TtlRecords
import tikronos.storage.address
import tikronos.storage.after
import tikronos.storage.bytes
import tikronos.storage.prefix
import tikronos.storage.prefixEnd
import tikronos.storage.readBatch

internal class TtlWorker(private val db: Storage, private val ttl: Ttl) {
    private var configAfter = ""
    private var candidateAfter = ""

    fun tick(writer: ItemWriter) {
        maintenanceAttempt("TTL backfill page") { backfillPage() }
        val now = Instant.now().epochSecond
        val low = if (candidateAfter.isEmpty()) bytes(TtlRecords.CANDIDATES) else after(bytes(candidateAfter))
        val candidates = db.ranges.scan(low, bytes(TtlRecords.dueEnd(now)), true,
            options = ScanOptions(limit = BATCH_SIZE)).toList()
        candidateAfter = if (candidates.size < BATCH_SIZE) "" else candidates.last().key.toStringUtf8()
        for (row in candidates) maintenanceAttempt("TTL candidate") {
            expire(row.key.toStringUtf8(), TtlRecords.candidate(row.value.toByteArray()), now, writer)
        }
    }

    private fun backfillPage() {
        val snapshot = db.snapshot()
        val configs = TtlRecords.CONFIGS
        val first = if (configAfter.isEmpty()) bytes(configs) else after(bytes(configAfter))
        val page = db.ranges.scan(first, prefixEnd(configs), true, snapshot,
            ScanOptions(limit = BATCH_SIZE)).toList()
        configAfter = if (page.size < BATCH_SIZE) "" else page.last().key.toStringUtf8()
        val states = mutableListOf<TtlState>()
        for (row in page) maintenanceAttempt("TTL configuration") {
            TtlRecords.state(row.value.toByteArray()).takeIf { it.enabled }?.let(states::add)
        }
        val progress = snapshot.readBatch(states.map(BackfillRecords::ttl))
        for (state in states) maintenanceAttempt("TTL backfill") {
            advance(state, BackfillRecords.decode(progress[BackfillRecords.ttl(state)], state.cursor))
        }
    }

    private fun expire(address: String, candidate: tikronos.model.ExpiryCandidate, now: Long, writer: ItemWriter) {
        db.atomic { tx ->
            val table = tx.read(Storage.CATALOG + candidate.tableName)?.let(Records::table)
            val config = tx.read(TtlRecords.config(candidate.tableId))?.let(TtlRecords::state)
            if (table?.id == candidate.tableId && config?.enabled == true &&
                config.generation == candidate.generation) {
                val itemAddress = table.address(candidate.key)
                val item = tx.read(itemAddress)?.let { Records.itemAt(table, itemAddress, it) }
                val expiry = item?.let { Ttl.expiration(it, config.attribute, now) }
                if (expiry == candidate.expiry && expiry <= now)
                    writer.change(tx, table, candidate.key, ttl = true) { null }
            }
            tx.write(address, null)
        }
    }

    internal fun backfill(state: TtlState) {
        val progressKey = BackfillRecords.ttl(state)
        val expected = BackfillRecords.decode(db.read(db.snapshot(), progressKey), state.cursor)
        advance(state, expected)
    }

    internal fun advance(state: TtlState, expected: BackfillProgress) {
        val progressKey = BackfillRecords.ttl(state)
        val cursor = expected.cursor ?: return
        val table = db.read(db.snapshot(), Storage.CATALOG + state.tableName)?.let(Records::table)
            ?.takeIf { it.id == state.tableId } ?: return
        val prefix = table.prefix()
        val first = cursor.takeIf { it.isNotEmpty() }?.let { after(bytes(it)) } ?: bytes(prefix)
        val rows = db.ranges.scan(first, prefixEnd(prefix), true,
            options = ScanOptions(limit = BATCH_SIZE, keyOnly = true)).toList()
        db.atomic { tx ->
            val live = tx.read(Storage.CATALOG + table.name)?.let(Records::table)
            val current = tx.read(TtlRecords.config(table.id))?.let(TtlRecords::state)
            if (live?.id == table.id && current == state &&
                BackfillRecords.decode(tx.read(progressKey), current.cursor) == expected) {
                tx.prefetch(rows.map { it.key.toStringUtf8() })
                for (row in rows) {
                    val address = row.key.toStringUtf8()
                    val item = tx.read(address)?.let { Records.itemAt(table, address, it) }
                    ttl.track(tx, table, null, item)
                }
                val next = if (rows.size < BATCH_SIZE) null else rows.last().key.toStringUtf8()
                tx.write(progressKey, BackfillRecords.encode(BackfillProgress(next)))
            }
        }
    }

    companion object {
        private const val BATCH_SIZE = 32
    }
}
