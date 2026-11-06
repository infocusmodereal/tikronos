package tikronos

import tikronos.model.Table
import tikronos.model.Throughput
import tikronos.storage.Records
import kotlin.test.Test
import kotlin.test.assertEquals

class RecordsTest {
    @Test
    fun `existing lock and table retain identity after upgrading the storage format`() {
        val old = checkNotNull(javaClass.getResource("/legacy-table.json")).readBytes()
        val provisioned = Table("Locksfaa4b023", "82227b81-a1b1-4a08-9bca-896c334304aa", "id",
            1793923808L, Throughput(2, 3))
        assertEquals(provisioned, Records.table(old))
        val table = Table("Locks", "123", "id", 1, null)
        val v1 = """{"name":"Locks","id":"123","partitionKey":"id","createdAt":1,"throughput":null}"""
        assertEquals(table, Records.table(byteArrayOf(1) + v1.encodeToByteArray()))
        assertEquals(table, Records.table(Records.encodeTable(table)))
        val item = mapOf("id" to "state", "Info" to "old-owner")
        assertEquals(item, Records.item("""{"id":{"S":"state"},"Info":{"S":"old-owner"}}""".encodeToByteArray()))
        assertEquals(item, Records.item(Records.encodeItem(item)))
    }
}
