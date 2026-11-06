package tikronos

import tikronos.model.Condition
import tikronos.model.ConditionFailed
import tikronos.model.ItemWrite
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ItemWriteTest {
    @Test
    fun `conditional retry observes the existing owner`() {
        val item = mapOf("id" to "state")
        val change = ItemWrite(Condition.KEY_ABSENT, item)
        assertEquals(item, change.apply(null))
        assertFailsWith<ConditionFailed> { change.apply(item) }
    }
}
