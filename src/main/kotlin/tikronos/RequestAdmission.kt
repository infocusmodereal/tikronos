package tikronos

import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull

/** Wait briefly for transient saturation before reading a body or claiming a storage worker. */
internal class RequestAdmission(maxRequests: Int, maxWaiters: Int, private val waitMs: Long = BURST_WAIT_MS) {
    private val slots = Semaphore(maxRequests)
    private val waiters = Semaphore(maxWaiters)

    suspend fun acquire(): Boolean {
        return slots.tryAcquire() || awaitSlot()
    }

    private suspend fun awaitSlot(): Boolean {
        if (!waiters.tryAcquire()) return false
        var acquired = false
        var admitted = false
        try {
            admitted = withTimeoutOrNull(waitMs) {
                slots.acquire()
                acquired = true
                true
            } == true
            return admitted
        } finally {
            if (acquired && !admitted) slots.release()
            waiters.release()
        }
    }

    fun release() = slots.release()
    suspend fun acquireForDrain() = slots.acquire()

    companion object { private const val BURST_WAIT_MS = 2L }
}
