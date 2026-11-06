package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import tikronos.model.Value

class TtlTest {
    @Test
    fun `malformed timestamps cannot make live data eligible for expiration`() {
        val now = 1_800_000_000L
        val invalid = listOf(Value.text("1"), Value.number("1.5".toBigDecimal()),
            Value.number("1e30".toBigDecimal()), Value.number(1.toBigDecimal()))
        for (value in invalid) assertNull(Ttl.expiration(mapOf("ttl" to value), "ttl", now))
        assertNull(Ttl.expiration(emptyMap(), "ttl", now))
        assertEquals(now - 1, Ttl.expiration(mapOf("ttl" to Value.number((now - 1).toBigDecimal())), "ttl", now))
    }
}
