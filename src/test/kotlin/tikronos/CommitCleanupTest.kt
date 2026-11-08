package tikronos

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertSame
import kotlin.test.assertTrue
import tikronos.storage.CommitCleanup

class CommitCleanupTest {
    @Test
    fun `saturation falls back inline and shutdown drains queued commits`() {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val queued = AtomicBoolean()
        val cleanup = CommitCleanup(workers = 1, capacity = 1)
        try {
            cleanup.execute { started.countDown(); release.await() }
            assertTrue(started.await(5, TimeUnit.SECONDS))
            cleanup.execute { queued.set(true) }
            val caller = Thread.currentThread()
            cleanup.execute { assertSame(caller, Thread.currentThread()) }
        } finally {
            release.countDown()
            cleanup.close()
        }
        assertTrue(queued.get())
        val afterClose = AtomicBoolean()
        cleanup.execute { afterClose.set(true) }
        assertTrue(afterClose.get())
    }
}
