package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import tikronos.storage.CommitUnknown
import tikronos.storage.ContentionLimitExceeded
import tikronos.protocol.BatchWrite
import tikronos.model.Table
import tikronos.model.Value
import tikronos.model.WriteResult

class BatchRetryTest {
    @Test
    fun `partial batch retries only the rejected prewrite`() {
        fun delete(key: String) = obj("DeleteRequest" to obj("Key" to obj("pk" to obj("S" to key))))
        val table = Table("Locks", "id", "pk", 0, null)
        val writes = listOf("a", "b", "c").map {
            BatchWrite(table, it, delete(it), null, mapOf("pk" to Value.text(it)))
        }
        val committed = mutableListOf<String>()
        val result =
            executeBatch(writes, Capacity(obj())) {
                if (it.address == "b") throw ContentionLimitExceeded()
                committed += it.address
                WriteResult(table, null, null)
            }
        val retry = result["UnprocessedItems"]["Locks"].single()
        executeBatch(writes.filter { it.request == retry }, Capacity(obj())) {
            committed += it.address
            WriteResult(table, null, null)
        }
        assertEquals(listOf("a", "c", "b"), committed)
    }

    @Test
    fun `unknown commit stops the batch instead of returning replayable unprocessed items`() {
        val table = Table("Locks", "id", "pk", 0, null)
        val writes = listOf("a", "b").map { BatchWrite(table, it, obj(), null, emptyMap()) }
        var attempts = 0
        assertFailsWith<CommitUnknown> {
            executeBatch(writes, Capacity(obj())) {
                attempts++
                throw CommitUnknown(IllegalStateException("Lost response"))
            }
        }
        assertEquals(1, attempts)
    }

}
