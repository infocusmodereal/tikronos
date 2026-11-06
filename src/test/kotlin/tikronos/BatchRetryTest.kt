package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import tikronos.storage.ContentionLimitExceeded

class BatchRetryTest {
    @Test
    fun `partial batch retries only the rejected prewrite`() {
        fun delete(key: String) = obj("DeleteRequest" to obj("Key" to obj("pk" to obj("S" to key))))
        val writes = listOf("a", "b", "c").map { BatchWrite("Locks", it, delete(it), null, 0) }
        val committed = mutableListOf<String>()
        val result =
            executeBatch(writes, Capacity(obj())) {
                if (it.address == "b") throw ContentionLimitExceeded()
                committed += it.address
                null
            }
        val retry = result["UnprocessedItems"]["Locks"].single()
        executeBatch(writes.filter { it.request == retry }, Capacity(obj())) {
            committed += it.address
            null
        }
        assertEquals(listOf("a", "c", "b"), committed)
    }
}
