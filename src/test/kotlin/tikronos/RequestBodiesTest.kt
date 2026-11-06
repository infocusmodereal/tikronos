package tikronos

import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.async
import kotlinx.coroutines.yield
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class RequestBodiesTest {
    @Test
    fun `real bytes exhaust budget until cancellation releases the request`() = runBlocking {
        val capacity = 2 * 8192
        val budget = BodyBudget(capacity)
        val bodies = RequestBodies(budget)
        val data = ByteArray(8192) { (it % 251).toByte() }
        val entered = CompletableDeferred<Unit>()
        val held = launch {
            bodies.consume(ByteReadChannel(data)) {
                assertContentEquals(data, it)
                entered.complete(Unit)
                awaitCancellation()
            }
        }
        try {
            withTimeout(5000) { entered.await() }
            val error = assertFailsWith<ApiError> { bodies.consume(ByteReadChannel(byteArrayOf(1))) {} }
            assertEquals("ProvisionedThroughputExceededException", error.code)
        } finally {
            held.cancelAndJoin()
        }
        // Reusing the entire budget proves neither the cancelled nor rejected request leaked bytes.
        bodies.consume(ByteReadChannel(data)) { assertContentEquals(data, it) }
        budget.reserve(capacity)
        budget.release(capacity)
    }

    @Test
    fun `idle timeout releases already buffered chunks`() = runBlocking {
        val capacity = 8192
        val budget = BodyBudget(capacity)
        val bodies = RequestBodies(budget)
        val input = ByteChannel(autoFlush = true)
        val writer = launch { input.writeFully(ByteArray(capacity)) }
        val reader = async {
            try {
                bodies.consume(input) {}
                null
            } catch (error: ApiError) {
                error
            }
        }
        try {
            withTimeout(2000) {
                while (true) {
                    // All coroutines share this runBlocking thread; release before yielding to the reader.
                    val charged = try {
                        budget.reserve(1)
                        budget.release(1)
                        false
                    } catch (_: ApiError) {
                        true
                    }
                    if (charged) break
                    yield()
                }
            }
            assertFalse(reader.isCompleted)
            assertEquals("RequestTimeout", withTimeout(5000) { reader.await() }?.code)
            writer.join()
            budget.reserve(capacity)
            budget.release(capacity)
        } finally {
            input.cancel(null)
            reader.cancelAndJoin()
            writer.cancelAndJoin()
        }
    }

    @Test
    fun `mid-read throttle and oversize release all preceding chunks`() = runBlocking {
        val cases = listOf(8192 to 16384, 64 * 1024 * 1024 to (Dynamo.MAX_BODY + 1))
        for ((capacity, length) in cases) {
            val budget = BodyBudget(capacity)
            val error = assertFailsWith<ApiError> {
                RequestBodies(budget).consume(ByteReadChannel(ByteArray(length))) {}
            }
            assertEquals(if (length > Dynamo.MAX_BODY) "RequestEntityTooLarge"
                else "ProvisionedThroughputExceededException", error.code)
            budget.reserve(capacity)
            budget.release(capacity)
        }
    }
}
