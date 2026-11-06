package tikronos

import tikronos.model.Table
import tikronos.storage.Records
import kotlin.test.Test
import kotlin.test.assertEquals

class RecordsTest {
    @Test
    fun `existing lock and table retain identity after upgrading the storage format`() {
        val old = obj("TableName" to "Locks", "TableId" to "123", "CreationDateTime" to 1,
            "KeySchema" to listOf(obj("AttributeName" to "id", "KeyType" to "HASH")))
        val table = Table("Locks", "123", "id", 1, null)
        assertEquals(table, Records.table(json.writeValueAsBytes(old)))
        assertEquals(table, Records.table(Records.encodeTable(table)))
        val item = mapOf("id" to "state", "Info" to "old-owner")
        assertEquals(item, Records.item("""{"id":{"S":"state"},"Info":{"S":"old-owner"}}""".encodeToByteArray()))
        assertEquals(item, Records.item(Records.encodeItem(item)))
    }
}
