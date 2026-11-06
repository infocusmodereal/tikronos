package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import tikronos.model.Value
import tikronos.storage.ordered

class OrderTest {
    @Test
    fun `numeric key order includes signed extremes and equivalent decimals share an address`() {
        val numbers =
            listOf(
                "-9.9e125",
                "-20",
                "-2.01",
                "-2",
                "-1e-130",
                "0",
                "1e-130",
                "1.01",
                "10",
                "9e125",
            )
        val keys = numbers.map { ordered(Value.number(it.toBigDecimal())) }
        assertEquals(keys.sorted(), keys)
        assertEquals(
            ordered(Value.number("1.00".toBigDecimal())),
            ordered(Value.number("1e0".toBigDecimal())),
        )
    }
}
