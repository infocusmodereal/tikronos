package tikronos

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

class BodyBudgetTest {
    @Test
    fun `concurrent growth admits one fitting request without partial reservations`() {
        val budget = BodyBudget(64)
        val workers = Executors.newFixedThreadPool(8)
        val barrier = CyclicBarrier(8)
        try {
            val attempts = (1..8).map {
                workers.submit<Boolean> {
                    barrier.await(5, TimeUnit.SECONDS)
                    try {
                        budget.reserve(40)
                        true
                    } catch (_: ApiError) {
                        false
                    }
                }
            }
            assertEquals(1, attempts.count { it.get(5, TimeUnit.SECONDS) })
            budget.reserve(24)
            budget.release(64)
            budget.reserve(64)
        } finally {
            workers.shutdownNow()
        }
    }
}
