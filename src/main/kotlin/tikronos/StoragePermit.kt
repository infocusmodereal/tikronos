package tikronos

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

internal suspend fun <T : Any> withStoragePermit(
    permits: Semaphore,
    dispatcher: CoroutineDispatcher,
    timeoutMs: Long,
    onAdmitted: () -> Unit,
    work: CoroutineScope.() -> T,
): T? {
    var held = permits.tryAcquire()
    try {
        if (!held && withTimeoutOrNull(timeoutMs) {
                permits.acquire()
                held = true
                true
            } != true) return null
        onAdmitted()
        return withContext(dispatcher) {
            try {
                work()
            } finally {
                // Release on the worker before the HTTP dispatcher resumes the request.
                held = false
                permits.release()
            }
        }
    } finally {
        // Timeout or cancellation after acquisition but before the worker starts.
        if (held) permits.release()
    }
}
