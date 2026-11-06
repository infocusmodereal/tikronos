package tikronos.model

internal data class Table(
    val name: String,
    val id: String,
    val partitionKey: String,
    val createdAt: Long,
    val throughput: Throughput?,
)

internal data class Throughput(val read: Long, val write: Long)
internal typealias Item = Map<String, String>
