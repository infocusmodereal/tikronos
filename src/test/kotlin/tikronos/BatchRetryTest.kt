package tikronos

import java.util.concurrent.CountDownLatch
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
            executeBatch(writes, Capacity(obj("ReturnConsumedCapacity" to "TOTAL")), Semaphore(1)) {
                if (it.address == "b") throw ContentionLimitExceeded()
                committed += it.address
                WriteResult(table, null, null)
            }
        assertEquals(2.0, result["ConsumedCapacity"].single()["CapacityUnits"].doubleValue())
        val retry = result["UnprocessedItems"]["Locks"].single()
        executeBatch(writes.filter { it.request == retry }, Capacity(obj())) {
            committed += it.address
            WriteResult(table, null, null)
        }
        assertEquals(listOf("a", "c", "b"), committed)
        assertFailsWith<ContentionLimitExceeded> {
            executeBatch(writes, Capacity(obj()), Semaphore(1)) { throw ContentionLimitExceeded() }
        }
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
        for (unknown in listOf("a", "b")) {
            val rendezvous = CyclicBarrier(2)
            val spare = Semaphore(1)
            val failure = assertFailsWith<CommitUnknown> {
                executeBatch(writes, Capacity(obj()), spare) { op ->
                    check(op.address != "c") { "Started another operation after a fatal error" }
                    rendezvous.await(5, TimeUnit.SECONDS)
                    if (op.address == unknown) throw CommitUnknown(IllegalStateException("Lost reply"))
                    invalid("Concurrent schema change")
                }
            }
            assertEquals("Concurrent schema change", failure.suppressed.single().message)
            assertEquals(1, spare.availablePermits)
        }
    }

    @Test
    fun `forced shutdown interrupts and drains a blocked helper without hiding its unknown outcome`() {
        val table = Table("Locks", "id", "pk", 0, null)
        val writes = listOf("a", "b", "c").map { BatchWrite(table, it, obj(), null, emptyMap()) }
        val entered = CountDownLatch(1)
        val spare = Semaphore(1)
        try {
            assertFailsWith<CommitUnknown> {
                executeBatch(writes, Capacity(obj()), spare) { op ->
                    if (op.address == "a") {
                        check(entered.await(5, TimeUnit.SECONDS))
                        Thread.currentThread().interrupt()
                    } else {
                        check(op.address == "b")
                        entered.countDown()
                        try {
                            check(CountDownLatch(1).await(5, TimeUnit.SECONDS)) { "Helper was not interrupted" }
                        } catch (stop: InterruptedException) {
                            throw CommitUnknown(stop)
                        }
                    }
                    WriteResult(table, null, null)
                }
            }
            assertEquals(true, Thread.currentThread().isInterrupted)
            assertEquals(1, spare.availablePermits)
        } finally {
            Thread.interrupted()
        }
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
