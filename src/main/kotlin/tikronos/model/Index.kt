package tikronos.model

/** Index identity is independent of its reusable display name. */
internal data class Index(
    val name: String,
    val id: String,
    val partitionKey: String,
    val partitionType: String,
    val sortKey: String? = null,
    val sortType: String? = null,
    val local: Boolean = false,
    val projection: String = "ALL",
    val attributes: List<String> = emptyList(),
    val throughput: Throughput? = null,
    val status: String = "ACTIVE",
    val cursor: String? = null,
)

internal const val MAX_GSI = 20
