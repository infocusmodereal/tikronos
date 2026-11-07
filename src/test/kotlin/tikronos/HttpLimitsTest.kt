package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import tikronos.config.HttpLimits

class HttpLimitsTest {
    @Test
    fun `defaults use container CPU and preserve a bounded admission queue`() {
        assertEquals(HttpLimits(64, 128), HttpLimits.fromEnvironment(emptyMap(), 8))
        assertEquals(HttpLimits(128, 256), HttpLimits.fromEnvironment(emptyMap(), 256))
        assertEquals(HttpLimits(8, 16), HttpLimits.fromEnvironment(emptyMap(), 1))
        assertEquals(
            HttpLimits(12, 24),
            HttpLimits.fromEnvironment(mapOf("TIKRONOS_STORAGE_WORKERS" to "12"), 8),
        )
    }

    @Test
    fun `invalid limits fail before allocating workers`() {
        for (env in
            listOf(
                mapOf("TIKRONOS_STORAGE_WORKERS" to "0"),
                mapOf("TIKRONOS_STORAGE_WORKERS" to "257"),
                mapOf("TIKRONOS_MAX_REQUESTS" to "1"),
                mapOf("TIKRONOS_MAX_REQUESTS" to "513"),
                mapOf("TIKRONOS_STORAGE_WORKERS" to "many"),
            )) {
            assertFailsWith<IllegalArgumentException> { HttpLimits.fromEnvironment(env, 8) }
        }
    }
}
