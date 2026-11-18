package tikronos

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore

class StoragePermitTest {
    @Test
    fun `finished worker releases its permit before HTTP resumes`() = runBlocking {
        Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { http ->
            Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { storage ->
                val finish = CountDownLatch(1)
                val resumeHttp = CountDownLatch(1)
                val permits = Semaphore(1)
                val result = async(http, start = CoroutineStart.UNDISPATCHED) {
                    withStoragePermit(permits, storage, 1000, {}) {
                        check(finish.await(5, TimeUnit.SECONDS))
                        "done"
                    }
                }
                try {
                    http.executor.execute { resumeHttp.await(5, TimeUnit.SECONDS) }
                    finish.countDown()
                    // A task behind the storage call runs after its finally block.
                    val available = async(storage) { permits.availablePermits }
                    assertEquals(1, available.await())
                } finally {
                    finish.countDown()
                    resumeHttp.countDown()
                }
                assertEquals("done", result.await())
            }
        }
    }
}
