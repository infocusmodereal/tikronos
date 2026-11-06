package tikronos.model

internal data class StreamState(
    val arn: String,
    val label: String,
    val tableName: String,
    val tableId: String,
    val view: String,
    val createdAt: Long,
    val keys: List<Pair<String, String>>,
    val enabled: Boolean = true,
    val head: Long = 0,
    val trimmed: Long = 0,
    val closedAt: Long? = null,
)

internal data class StreamPosition(val arn: String, val after: Long, val expires: Long)
