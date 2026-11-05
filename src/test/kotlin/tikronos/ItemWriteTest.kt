package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ItemWriteTest {
    @Test
    fun `retry observes the winner instead of overwriting its lock`() {
        val req = obj(
            "ConditionExpression" to "attribute_not_exists(#k)",
            "ExpressionAttributeNames" to obj("#k" to "id"),
        )
        val item = obj("id" to obj("S" to "state"))
        val change = ItemWrite(req, "id", item)
        assertEquals(item, change.apply(null))
        assertEquals("ConditionalCheckFailedException", assertFailsWith<ApiError> { change.apply(item) }.code)
    }

    @Test
    fun `unsupported condition cannot become an unconditional write`() {
        assertFailsWith<ApiError> { ItemWrite(obj("ConditionExpression" to "attribute_exists(id)"), "id", null) }
    }
}
