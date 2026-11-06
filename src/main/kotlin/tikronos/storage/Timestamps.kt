package tikronos.storage

internal fun timestampMillis(version: Long): Long = version shr LOGICAL_BITS
internal fun timestampSeconds(version: Long): Long = timestampMillis(version) / MILLIS_PER_SECOND
internal fun timestampStartOfSecond(second: Long): Long = (second * MILLIS_PER_SECOND) shl LOGICAL_BITS
private const val LOGICAL_BITS = 18
private const val MILLIS_PER_SECOND = 1000L
