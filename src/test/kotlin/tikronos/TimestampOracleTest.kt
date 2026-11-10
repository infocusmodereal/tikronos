package tikronos

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.tikv.common.meta.TiTimestamp
import tikronos.storage.TimestampOracle

class TimestampOracleTest {
    @Test
    fun `concurrent allocations are unique and later callers never reuse an earlier allocation`() {
        val sequence = AtomicLong()
        val requests = AtomicLong()
        val oracle = TimestampOracle { count ->
            requests.incrementAndGet()
            val first = sequence.getAndAdd(count.toLong())
            List(count) { TiTimestamp(1, first + it) }
        }
        Executors.newFixedThreadPool(16).use { pool ->
            val ready = CountDownLatch(16)
            val start = CountDownLatch(1)
            val calls = List(16) {
                pool.submit<List<Long>> {
                    ready.countDown()
                    check(start.await(5, TimeUnit.SECONDS))
                    List(100) { oracle.next().version }
                }
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS))
            start.countDown()
            val versions = calls.flatMap { it.get(10, TimeUnit.SECONDS) }
            assertEquals(1600, versions.toSet().size)
            assertTrue(requests.get() < 1600)
            val before = requests.get()
            assertTrue(oracle.next().version > versions.max())
            assertEquals(before + 1, requests.get())
        }
    }

    @Test
    fun `a caller arriving during allocation waits for a fresh PD request`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val requests = AtomicLong()
        val oracle = TimestampOracle { count ->
            val physical = requests.incrementAndGet()
            if (physical == 1L) {
                entered.countDown()
                check(release.await(5, TimeUnit.SECONDS))
            }
            List(count) { TiTimestamp(physical, it.toLong()) }
        }
        Executors.newFixedThreadPool(2).use { pool ->
            val first = pool.submit<TiTimestamp> { oracle.next() }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val late = pool.submit<TiTimestamp> { oracle.next() }
            assertFailsWith<TimeoutException> { late.get(50, TimeUnit.MILLISECONDS) }
            release.countDown()
            assertEquals(1L, first.get(5, TimeUnit.SECONDS).physical)
            assertEquals(2L, late.get(5, TimeUnit.SECONDS).physical)
        }
    }

    @Test
    fun `interrupted queue head does not strand later callers`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val requests = AtomicLong()
        val oracle = TimestampOracle { count ->
            val physical = requests.incrementAndGet()
            if (physical == 1L) {
                entered.countDown()
                check(release.await(5, TimeUnit.SECONDS))
            }
            List(count) { TiTimestamp(physical, it.toLong()) }
        }
        val interrupted = CompletableFuture<Unit>()
        val head = Thread {
            try {
                assertFailsWith<InterruptedException> { oracle.next() }
                interrupted.complete(Unit)
            } catch (failure: Throwable) { interrupted.completeExceptionally(failure) }
        }
        Executors.newFixedThreadPool(2).use { pool ->
            val first = pool.submit<TiTimestamp> { oracle.next() }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            head.start()
            try {
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
                while (head.state != Thread.State.WAITING && System.nanoTime() < deadline) Thread.yield()
                assertEquals(Thread.State.WAITING, head.state)
                val later = pool.submit<TiTimestamp> { oracle.next() }
                assertFailsWith<TimeoutException> { later.get(50, TimeUnit.MILLISECONDS) }
                head.interrupt()
                interrupted.get(5, TimeUnit.SECONDS)
                release.countDown()
                assertEquals(1L, first.get(5, TimeUnit.SECONDS).physical)
                assertEquals(2L, later.get(5, TimeUnit.SECONDS).physical)
            } finally { head.interrupt(); release.countDown(); head.join(1000) }
        }
    }

    @Test
    fun `allocation failure releases the next allocator and allows recovery`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val oracle = TimestampOracle { count ->
            if (release.count > 0) {
                entered.countDown()
                check(release.await(5, TimeUnit.SECONDS))
                error("PD unavailable")
            }
            List(count) { TiTimestamp(2, it.toLong()) }
        }
        Executors.newFixedThreadPool(2).use { pool ->
            val failed = pool.submit { assertFailsWith<IllegalStateException> { oracle.next() } }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val waiting = pool.submit<TiTimestamp> { oracle.next() }
            release.countDown()
            failed.get(5, TimeUnit.SECONDS)
            assertEquals(2, waiting.get(5, TimeUnit.SECONDS).physical)
        }
    }
}
