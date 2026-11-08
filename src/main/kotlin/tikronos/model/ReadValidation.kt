package tikronos.model

internal data class ReadValidation(val guards: Set<String>, val commitCeiling: Long)
