package tikronos

import java.util.concurrent.atomic.AtomicInteger

/** All-or-nothing accounting; a rejected growth cannot temporarily consume another request's allowance. */
internal class BodyBudget(private val capacity: Int) {
    private val used = AtomicInteger()

    fun reserve(bytes: Int) {
        while (true) {
            val before = used.get()
            if (bytes > capacity - before) {
                throw ApiError("ProvisionedThroughputExceededException", "Body budget is full")
            }
            if (used.compareAndSet(before, before + bytes)) return
        }
    }

    fun release(bytes: Int) { used.addAndGet(-bytes) }
}
