package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import org.junit.jupiter.api.Tag
import tikronos.codec.parseItem
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.Value
import tikronos.model.itemSize
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address

@Tag("tikv")
class ReadPagesIntegrationTest {
    @Test
    fun `exact byte boundary precedes projection and filtering`() = fixture { db, table ->
        val rows = (0 until 5).map { row(it, if (it < 4) PAGE_BYTES / 4 else 100) }
        store(db, table, rows)
        val reads = Reads(db, Tables(db))
        val query = obj("TableName" to table.name, "KeyConditionExpression" to "pk = :p",
            "ExpressionAttributeValues" to obj(":p" to obj("S" to "partition")), "ProjectionExpression" to "sk")
        val first = reads.read(query, true)
        assertEquals(4, first["ScannedCount"].intValue())
        assertEquals(rows.take(4).map { mapOf("sk" to it.getValue("sk")) }, first["Items"].map(::parseItem))
        query.set<com.fasterxml.jackson.databind.JsonNode>("ExclusiveStartKey", first["LastEvaluatedKey"])
        val next = reads.read(query, true)
        assertEquals(listOf(mapOf("sk" to rows.last().getValue("sk"))), next["Items"].map(::parseItem))
        assertFalse(next.has("LastEvaluatedKey"))
        val scan = obj("TableName" to table.name, "FilterExpression" to "attribute_exists(absent)")
        val filtered = reads.read(scan, false)
        assertEquals(0, filtered["Count"].intValue())
        assertEquals(4, filtered["ScannedCount"].intValue())
        assertEquals(first["LastEvaluatedKey"], filtered["LastEvaluatedKey"])
        scan.set<com.fasterxml.jackson.databind.JsonNode>("ExclusiveStartKey", filtered["LastEvaluatedKey"])
        val tail = reads.read(scan, false)
        assertEquals(1, tail["ScannedCount"].intValue())
        assertEquals(0, tail["Count"].intValue())
        assertFalse(tail.has("LastEvaluatedKey"))
    }

    @Test
    fun `a row beyond the byte budget is returned on the next reverse page`() = fixture { db, table ->
        val rows = (0 until 4).map { row(it, 300 * 1024) }
        store(db, table, rows)
        val reads = Reads(db, Tables(db))
        val req = obj("TableName" to table.name, "KeyConditionExpression" to "pk = :p",
            "ExpressionAttributeValues" to obj(":p" to obj("S" to "partition")), "ScanIndexForward" to false)
        val first = reads.read(req, true)
        assertEquals(rows.reversed().take(3), first["Items"].map(::parseItem))
        req.set<com.fasterxml.jackson.databind.JsonNode>("ExclusiveStartKey", first["LastEvaluatedKey"])
        val last = reads.read(req, true)
        assertEquals(listOf(rows.first()), last["Items"].map(::parseItem))
        assertFalse(last.has("LastEvaluatedKey"))
    }

    @Test
    fun `table name cursors survive deletion and insertion`() = withStorageSession { _, db ->
        val prefix = "Pages-${UUID.randomUUID()}-"
        val names = listOf("a", "b", "c", "d", "e").map { prefix + it }
        fun create(name: String) = db.atomic {
            it.write(Storage.CATALOG + name, Records.encodeTable(Table(name, name, "pk", 0, null)))
        }
        try {
            listOf(names[0], names[2], names[4]).forEach(::create)
            val first = Tables(db).list(obj("Limit" to 2, "ExclusiveStartTableName" to prefix))
            assertEquals(listOf(names[0], names[2]), first["TableNames"].map { it.textValue() })
            assertEquals(names[2], first["LastEvaluatedTableName"].textValue())
            db.atomic { tx -> listOf(names[2], names[4]).forEach { tx.write(Storage.CATALOG + it, null) } }
            create(names[1])
            create(names[3])
            val next = Tables(db).list(obj("Limit" to 2, "ExclusiveStartTableName" to names[2]))
            assertEquals(listOf(names[3]), next["TableNames"].map { it.textValue() }.filter { it.startsWith(prefix) })
        } finally {
            db.atomic { tx -> names.forEach { tx.write(Storage.CATALOG + it, null) } }
        }
    }

    @Test
    fun `hash only query resumes without repeating its item`() = fixture(sort = false) { db, table ->
        val item = mapOf("pk" to Value.text("partition"))
        store(db, table, listOf(item))
        val reads = Reads(db, Tables(db))
        val query = obj("TableName" to table.name, "KeyConditionExpression" to "pk = :p", "Limit" to 1,
            "ExpressionAttributeValues" to obj(":p" to obj("S" to "partition")))
        val first = reads.read(query, true)
        assertEquals(listOf(item), first["Items"].map(::parseItem))
        query.set<com.fasterxml.jackson.databind.JsonNode>("ExclusiveStartKey", first["LastEvaluatedKey"])
        val last = reads.read(query, true)
        assertEquals(0, last["ScannedCount"].intValue())
        assertFalse(last.has("LastEvaluatedKey"))
    }

    private fun row(index: Int, size: Int): Item {
        val base = mapOf("pk" to Value.text("partition"), "sk" to Value.text(index.toString()),
            "payload" to Value.text(""))
        return base + ("payload" to Value.text("x".repeat(size - itemSize(base))))
    }

    private fun store(db: Storage, table: Table, rows: List<Item>) = db.atomic { tx ->
        rows.forEach { tx.write(table.address(it), Records.encodeItem(it)) }
    }

    private fun fixture(sort: Boolean = true, work: (Storage, Table) -> Unit) = withStorageSession { _, db ->
        val id = UUID.randomUUID().toString()
        val table = Table("Pages-$id", id, "pk", 0, null,
            sortKey = if (sort) "sk" else null, sortType = if (sort) "S" else null)
        db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
        try { work(db, table) } finally {
            db.atomic { tx ->
                (0 until 5).forEach { tx.write(table.address(row(it, 100)), null) }
                tx.write(Storage.CATALOG + table.name, null)
            }
        }
    }
}
