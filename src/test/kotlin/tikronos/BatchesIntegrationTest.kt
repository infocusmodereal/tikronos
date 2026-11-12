package tikronos

import kotlinx.coroutines.sync.Semaphore
import tikronos.diagnostics.RequestEvent
import tikronos.diagnostics.RequestEvents
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.Value
import tikronos.model.itemSize
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.key
import tikronos.storage.bytes
import tikronos.storage.prefix
import tikronos.storage.prefixEnd

@Tag("tikv")
class BatchesIntegrationTest {
    @Test
    fun `projected batch keeps the partition budget and missing key capacity`() = fixture { db, table ->
        val rows = (0 until 4).map { row("same", it) }
        store(db, table, rows)
        val keys = rows.map { table.key(it, false) } + mapOf("pk" to Value.text("absent"), "sk" to Value.text("0"))
        val group = obj("Keys" to keys.map(::wireItem), "ConsistentRead" to true,
            "ProjectionExpression" to "#p, sk", "ExpressionAttributeNames" to mapOf("#p" to "pk"))
        val batch = Batches(db, Tables(db))
        val response = batch.get(obj("RequestItems" to obj(table.name to group), "ReturnConsumedCapacity" to "TOTAL"))
        assertEquals(keys.take(3), response["Responses"][table.name].map(::parseItem))
        val pending = response["UnprocessedKeys"][table.name]
        assertEquals(listOf(keys[3]), pending["Keys"].map(::parseItem))
        assertEquals(group["ConsistentRead"], pending["ConsistentRead"])
        assertEquals(group["ProjectionExpression"], pending["ProjectionExpression"])
        assertEquals(group["ExpressionAttributeNames"], pending["ExpressionAttributeNames"])
        assertEquals(rows.take(3).sumOf { readUnits(itemSize(it), true) } + 1,
            response["ConsumedCapacity"].single()["CapacityUnits"].doubleValue())
        val retried = batch.get(obj("RequestItems" to response["UnprocessedKeys"]))
        assertEquals(listOf(keys[3]), retried["Responses"][table.name].map(::parseItem))
        assertTrue(retried["UnprocessedKeys"].isEmpty)
    }

    @Test
    fun `batch response budget spans native chunks and returns every omitted key`() = fixture { db, table ->
        val rows = (0 until 60).map { row("partition-$it", it) }
        store(db, table, rows)
        val keys = rows.map { table.key(it, false) }
        val batch = Batches(db, Tables(db))
        val response = batch.get(obj("RequestItems" to obj(table.name to obj("Keys" to keys.map(::wireItem)))))
        val returned = response["Responses"][table.name].map(::parseItem)
        val pending = response["UnprocessedKeys"][table.name]["Keys"].map(::parseItem)
        val used = returned.sumOf { json.writeValueAsBytes(wireItem(it)).size + 1 }
        assertTrue(returned.size > 16 && pending.isNotEmpty())
        assertTrue(used <= 16 * PAGE_BYTES)
        assertTrue(used + json.writeValueAsBytes(wireItem(rows[returned.size])).size + 1 > 16 * PAGE_BYTES)
        assertEquals(keys, returned.map { table.key(it, false) } + pending)
        val retried = batch.get(obj("RequestItems" to response["UnprocessedKeys"]))
        assertEquals(rows.drop(returned.size), retried["Responses"][table.name].map(::parseItem))
        assertTrue(retried["UnprocessedKeys"].isEmpty)
    }

    @Test
    fun `oversized put rejects the whole batch before any write`() = fixture { db, table ->
        val valid = row("valid", 0)
        val oversized = row("invalid", 1) + ("payload" to Value.text("x".repeat(400 * 1024)))
        val request = obj("RequestItems" to obj(table.name to listOf(valid, oversized).map {
            obj("PutRequest" to obj("Item" to wireItem(it)))
        }))
        assertEquals("ValidationException", assertFailsWith<ApiError> {
            Batches(db, Tables(db)).write(request)
        }.code)
        assertEquals(null, db.read(db.snapshot(), table.address(valid)))
    }

    @Test
    fun `independent commits share storage admission and account both successful items`() = fixture { db, table ->
        val items = (1..2).map { mapOf("pk" to Value.text("p"), "sk" to Value.text(it.toString())) }
        val commands = items.map { obj("PutRequest" to obj("Item" to wireItem(it))) }
        val req = obj("RequestItems" to obj(table.name to commands), "ReturnConsumedCapacity" to "TOTAL")
        val tables = Tables(db)
        tables.find(table.name) // Populate the existing write-plan lease.
        val spare = Semaphore(1)
        val batches = Batches(db, tables, spare)
        for (parallel in listOf(true, false)) {
            if (!parallel) check(spare.tryAcquire())
            val event = RequestEvent()
            val result = RequestEvents.within(event) { batches.write(req) }
            assertEquals(if (parallel) 1 else 0, event.batchPairs)
            assertTrue(result["UnprocessedItems"].isEmpty)
            assertEquals(2.0, result["ConsumedCapacity"].single()["CapacityUnits"].doubleValue())
            if (!parallel) spare.release()
        }
        items.forEach { assertEquals(it, Records.item(checkNotNull(db.read(db.snapshot(), table.address(it))))) }
    }

    private fun row(partition: String, sort: Int): Item = mapOf(
        "pk" to Value.text(partition), "sk" to Value.text(sort.toString()),
        "payload" to Value.text("x".repeat(300 * 1024)),
    )

    private fun store(db: Storage, table: Table, rows: List<Item>) {
        rows.chunked(8).forEach { chunk -> db.atomic { tx ->
            chunk.forEach { tx.write(table.address(it), Records.encodeItem(it)) }
        } }
    }

    private fun fixture(action: (Storage, Table) -> Unit) {
        withStorageSession { _, db ->
            val id = UUID.randomUUID().toString()
            val table = Table("Batch-$id", id, "pk", 0, null, sortKey = "sk", sortType = "S")
            db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
            try {
                action(db, table)
            } finally {
                val prefix = table.prefix()
                val keys = db.ranges.scan(bytes(prefix), prefixEnd(prefix), true)
                    .map { it.key.toStringUtf8() }.toList() + (Storage.CATALOG + table.name)
                keys.chunked(8).forEach { chunk -> db.atomic { tx -> chunk.forEach { tx.write(it, null) } } }
            }
        }
    }
}
