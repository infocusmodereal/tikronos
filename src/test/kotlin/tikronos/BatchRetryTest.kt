package tikronos

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Semaphore
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
        val writes = listOf("a", "b", "c").map { BatchWrite(table, it, obj(), null, emptyMap()) }
        val attempts = mutableListOf<String>()
        assertFailsWith<CommitUnknown> {
            executeBatch(writes, Capacity(obj())) {
                attempts += it.address
                if (it.address == "b") throw CommitUnknown(IllegalStateException("Lost response"))
                WriteResult(table, null, null)
            }
        }
        assertEquals(listOf("a", "b"), attempts)
    }

    @Test
    fun `concurrent unknown decision takes precedence over a client error and stops later writes`() {
        val table = Table("Locks", "id", "pk", 0, null)
        val writes = listOf("a", "b", "c").map { BatchWrite(table, it, obj(), null, emptyMap()) }
        val rendezvous = CyclicBarrier(2)
        val spare = Semaphore(1)
        val failure = assertFailsWith<CommitUnknown> {
            executeBatch(writes, Capacity(obj()), spare) { op ->
                check(op.address != "c") { "Started another operation after a fatal error" }
                rendezvous.await(5, TimeUnit.SECONDS)
                if (op.address == "a") invalid("Concurrent schema change")
                throw CommitUnknown(IllegalStateException("Lost reply"))
            }
        }
        assertEquals("Concurrent schema change", failure.suppressed.single().message)
        assertEquals(1, spare.availablePermits)
    }

    @Test
    fun `request cancellation drains current pair then stops before starting another item`() {
        val table = Table("Locks", "id", "pk", 0, null)
        val writes = listOf("a", "b", "c").map { BatchWrite(table, it, obj(), null, emptyMap()) }
        val spare = Semaphore(1)
        val finished = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
        val request = Job()
        assertFailsWith<CancellationException> {
            executeBatch(writes, Capacity(obj()), spare, request::ensureActive) { op ->
                if (op.address == "a") request.cancel()
                finished += op.address
                WriteResult(table, null, null)
            }
        }
        assertEquals(setOf("a", "b"), finished)
        assertEquals(1, spare.availablePermits)
    }

}
