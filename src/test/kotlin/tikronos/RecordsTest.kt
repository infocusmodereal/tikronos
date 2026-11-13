package tikronos

import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import tikronos.codec.parseItem
import tikronos.model.MAX_DEPTH
import tikronos.model.MAX_ITEM_BYTES
import kotlin.test.Test
import kotlin.test.assertEquals
import tikronos.model.Table
import tikronos.model.Throughput
import tikronos.model.Value
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.byteOrder
import tikronos.storage.segmentEdge

class RecordsTest {
    @Test
    fun `typed records keep canonical wire bytes for all attribute types`() {
        val source = """{"m":{"M":{"l":{"L":[{"S":"x"},{"N":"1E+2"},{"B":"YQ"},{"BOOL":false},{"NULL":true}]}}},""" +
            """"ss":{"SS":["z","a"]},"ns":{"NS":["2.00","10"]},"bs":{"BS":["Yg","YQ"]}}"""
        val canonical = """{"m":{"M":{"l":{"L":[{"S":"x"},{"N":"100"},{"B":"YQ=="},""" +
            """{"BOOL":false},{"NULL":true}]}}},""" +
            """"ss":{"SS":["a","z"]},"ns":{"NS":["10","2"]},"bs":{"BS":["YQ==","Yg=="]}}"""
        val item = parseItem(json.readTree(source))
        for (version in listOf(2, 3)) {
            val bytes = byteArrayOf(version.toByte()) + canonical.encodeToByteArray()
            assertContentEquals(bytes, Records.encodeItem(item, projectedIndexes = version == 3))
            assertEquals(item, Records.item(bytes))
        }
        assertEquals(mapOf("pk" to Value.text("legacy")),
            Records.item(byteArrayOf(1) + """{"pk":"legacy"}""".encodeToByteArray()))
    }

    @Test
    fun `wire encoding round trips the largest supported depth and item size`() {
        val nested = (1..MAX_DEPTH).fold(Value.text("x") as Value) { value, _ -> Value.list(listOf(value)) }
        val largest = mapOf("x" to Value.text("v".repeat(MAX_ITEM_BYTES - 1)))
        for (item in listOf(mapOf("x" to nested), largest)) {
            validateItem(item)
            assertEquals(item, Records.item(Records.encodeItem(item)))
        }
        assertFailsWith<ApiError> { validateItem(mapOf("x" to Value.list(listOf(nested)))) }
        assertFailsWith<ApiError> { validateItem(largest + ("y" to Value.Null)) }
    }

    @Test
    fun `existing lock and table retain identity after upgrading the storage format`() {
        val old = checkNotNull(javaClass.getResource("/legacy-table.json")).readBytes()
        val provisioned =
            Table(
                "Locksfaa4b023",
                "82227b81-a1b1-4a08-9bca-896c334304aa",
                "id",
                1793923808L,
                Throughput(2, 3),
            )
        assertEquals(provisioned, Records.table(old))
        assertEquals(
            Storage.itemKey(provisioned.id, "state"),
            provisioned.address(mapOf("id" to Value.text("state"))),
        )
        val edges = (0..4).map { provisioned.segmentEdge(it, 4) }
        kotlin.test.assertTrue(edges.zipWithNext().all { (a, b) -> byteOrder.compare(a, b) < 0 })
        val table = Table("Locks", "123", "id", 1, null)
        val v1 =
            """{"name":"Locks","id":"123","partitionKey":"id","createdAt":1,"throughput":null}"""
        assertEquals(table, Records.table(byteArrayOf(1) + v1.encodeToByteArray()))
        assertEquals(table, Records.table(Records.encodeTable(table)))
        val item = mapOf("id" to Value.text("state"), "Info" to Value.text("old-owner"))
        assertEquals(
            item,
            Records.item("""{"id":{"S":"state"},"Info":{"S":"old-owner"}}""".encodeToByteArray()),
        )
        assertEquals(item, Records.item(Records.encodeItem(item)))
    }
}
