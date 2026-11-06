package tikronos.model

internal enum class Condition { UNCONDITIONAL, KEY_ABSENT }

internal class ConditionFailed : RuntimeException()

internal data class ItemWrite(val condition: Condition, val replacement: Item?) {
    fun apply(current: Item?): Item? {
        if (condition == Condition.KEY_ABSENT && current != null) throw ConditionFailed()
        return replacement
    }
}
