package tikronos.model

internal data class Table(
    val name: String,
    val id: String,
    val partitionKey: String,
    val createdAt: Long,
    val throughput: Throughput?,
    val partitionType: String = "S",
    val sortKey: String? = null,
    val sortType: String? = null,
    val hashed: Boolean = false,
)

internal data class Throughput(val read: Long, val write: Long)
