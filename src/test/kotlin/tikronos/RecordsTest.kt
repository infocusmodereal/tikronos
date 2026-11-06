package tikronos

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
