package tikronos.model

internal data class LeasedValue(val bytes: ByteArray, val until: Long, val plan: TableWritePlan)
