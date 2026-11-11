package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import tikronos.codec.parseItem
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.Value
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.baseItems
import tikronos.storage.entry
import tikronos.storage.key

@Tag("tikv")
class IndexProjectionIntegrationTest {
    @Test
    fun `first write compacts legacy entries and local base reads honor snapshots`() =
        withStorageSession { _, db ->
            val id = UUID.randomUUID().toString()
            val index = Index("byRank", "local", "pk", "S", "rank", "S", local = true,
                projection = "INCLUDE", attributes = listOf("label"))
            val table = Table("Projection-$id", id, "pk", 0, null, sortKey = "sk", sortType = "S",
                indexes = listOf(index), hashed = true)
            val original = mapOf("pk" to Value.text("p"), "sk" to Value.text("s"), "rank" to Value.text("a"),
                "label" to Value.text("one"), "payload" to Value.text("old"))
            val key = table.key(original, false)
            val address = table.address(key)
            val entry = requireNotNull(index.entry(table, original))
            val writer = ItemWriter(db)
            try {
                db.atomic { tx ->
                    tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                    tx.write(address, Records.encodeItem(original))
                    tx.write(entry, Records.encodeItem(original))
                }
                val before = db.snapshot()
                val updated = original - "payload" + ("other" to Value.text("new"))
                writer.mutate(table, key) { updated }
                assertEquals(original - "payload", Records.item(requireNotNull(db.read(db.snapshot(), entry))))
                assertEquals(original, before.baseItems(table, sequenceOf(key)).single())
                val scan = obj("TableName" to table.name, "IndexName" to index.name, "Select" to "ALL_ATTRIBUTES")
                assertEquals(updated, parseItem(Reads(db, Tables(db)).read(scan, false)["Items"].single()))

                val changed = updated + ("label" to Value.text("two"))
                writer.mutate(table, key) { changed }
                assertEquals(changed - "other", Records.item(requireNotNull(db.read(db.snapshot(), entry))))
                writer.mutate(table, key) { changed - "rank" }
                assertNull(db.read(db.snapshot(), entry))
            } finally {
                db.atomic { tx -> listOf(Storage.CATALOG + table.name, address, entry).forEach { tx.write(it, null) } }
            }
        }

    @Test
    fun `creating index materializes untouched projections and backfills existing valid keys`() =
        withStorageSession { _, db ->
            val id = UUID.randomUUID().toString()
            val index = Index("byGroup", "global", "grp", "S", projection = "KEYS_ONLY", status = "CREATING")
            val table = Table("IndexBuild-$id", id, "pk", 0, null, indexes = listOf(index))
            val first = mapOf("pk" to Value.text("one"), "grp" to Value.text("g"), "extra" to Value.text("old"))
            val second = first + ("pk" to Value.text("two"))
            val invalidKey = first + mapOf("pk" to Value.text("three"), "grp" to Value.number(1.toBigDecimal()))
            val rows = listOf(first, second, invalidKey)
            val entries = listOf(first, second).map { requireNotNull(index.entry(table, it)) }
            try {
                db.atomic { tx ->
                    tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                    rows.forEach { tx.write(table.address(it), Records.encodeItem(it)) }
                }
                ItemWriter(db).mutate(table, table.key(first, false)) { first + ("extra" to Value.text("new")) }
                assertEquals(first - "extra", Records.item(requireNotNull(db.read(db.snapshot(), entries.first()))))
                IndexBackfill(db).backfill(table, index)
                val scan = obj("TableName" to table.name, "IndexName" to index.name)
                assertEquals(setOf(first - "extra", second - "extra"),
                    Reads(db, Tables(db)).read(scan, false)["Items"].map(::parseItem).toSet())
                assertEquals(second - "extra", Records.item(requireNotNull(db.read(db.snapshot(), entries.last()))))
            } finally {
                db.atomic { tx -> (entries + rows.map(table::address) + (Storage.CATALOG + table.name))
                    .forEach { tx.write(it, null) } }
            }
        }

    @Test
    fun `reverse local query resumes at the byte limit across mixed index records`() = withStorageSession { _, db ->
        val id = UUID.randomUUID().toString()
        val index = Index("byRank", "rank", "pk", "S", "rank", "S", local = true, projection = "KEYS_ONLY")
        val table = Table("LocalPage-$id", id, "pk", 0, null, sortKey = "sk", sortType = "S",
            indexes = listOf(index), hashed = true)
        val rows = (0 until 14).map {
            val order = Value.text(it.toString().padStart(2, '0'))
            mapOf("pk" to Value.text("p"), "sk" to order, "rank" to order, "body" to Value.text("x".repeat(100 * 1024)))
        }
        val query = obj("TableName" to table.name, "IndexName" to index.name, "ScanIndexForward" to false,
            "Limit" to 11, "KeyConditionExpression" to "pk = :p", "ProjectionExpression" to "sk",
            "FilterExpression" to "attribute_exists(body)", "ExpressionAttributeValues" to obj(":p" to obj("S" to "p")))
        val reads = Reads(db, Tables(db))
        try {
            db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
            rows.forEachIndexed { i, row -> db.atomic {
                it.write(table.address(row), Records.encodeItem(row))
                val copy = if (i % 2 == 0) row else row - "body"
                it.write(requireNotNull(index.entry(table, row)), Records.encodeItem(copy))
            } }
            val first = reads.read(query, true)
            assertEquals(9, first.path("ScannedCount").intValue())
            assertEquals(rows[5] - "body", parseItem(first.obj("LastEvaluatedKey")))
            query.set<com.fasterxml.jackson.databind.JsonNode>("ExclusiveStartKey", first.obj("LastEvaluatedKey"))
            val last = reads.read(query, true)
            assertEquals(5, last.path("ScannedCount").intValue())
            assertNull(last["LastEvaluatedKey"])
            assertEquals(rows.asReversed().map { it.getValue("sk") },
                (first["Items"].toList() + last["Items"].toList()).map { parseItem(it).getValue("sk") })
        } finally {
            db.atomic { tx ->
                rows.forEach { row ->
                    tx.write(table.address(row), null)
                    tx.write(requireNotNull(index.entry(table, row)), null)
                }
                tx.write(Storage.CATALOG + table.name, null)
            }
        }
    }
}
