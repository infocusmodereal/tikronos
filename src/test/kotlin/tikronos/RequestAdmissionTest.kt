package tikronos

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking

class RequestAdmissionTest {
    @Test
    fun `a burst can use a released slot and a cancelled waiter frees queue space`() = runBlocking {
        val admission = RequestAdmission(1, 1, waitMs = 5000)
        assertTrue(admission.acquire())
        val cancelled = async(start = CoroutineStart.UNDISPATCHED) { admission.acquire() }
        assertFalse(admission.acquire())
        cancelled.cancelAndJoin()
        val next = async(start = CoroutineStart.UNDISPATCHED) { admission.acquire() }
        admission.release()
        assertTrue(next.await())
        admission.release()
    }

    @Test
    fun `a timed out waiter leaves the active slot with its owner`() = runBlocking {
        val admission = RequestAdmission(1, 1, waitMs = 250)
        assertTrue(admission.acquire())
        assertFalse(admission.acquire())
        val next = async(start = CoroutineStart.UNDISPATCHED) { admission.acquire() }
        admission.release()
        assertTrue(next.await())
        admission.release()
    }
}
