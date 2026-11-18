package tikronos

import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore

class StoragePermitTest {
    @Test
    fun `finished worker releases its permit before HTTP resumes`() = runBlocking {
        val resumes = LinkedBlockingQueue<Runnable>()
        val http = object : CoroutineDispatcher() {
            override fun dispatch(context: CoroutineContext, block: Runnable) { resumes.add(block) }
        }
        val permits = Semaphore(1)
        val finish = CountDownLatch(1)
        val result = async(http, start = CoroutineStart.UNDISPATCHED) {
            withStoragePermit(permits, Dispatchers.IO, 1000, {}) {
                check(finish.await(5, TimeUnit.SECONDS))
                "done"
            }
        }
        finish.countDown()
        val resume = assertNotNull(resumes.poll(5, TimeUnit.SECONDS))
        try {
            assertEquals(1, permits.availablePermits)
        } finally {
            resume.run()
        }
        assertEquals("done", result.await())
    }
}
