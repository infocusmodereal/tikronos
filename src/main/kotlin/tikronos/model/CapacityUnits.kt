package tikronos.model

internal data class CapacityUnits(
    val table: Double,
    val local: Map<String, Double> = emptyMap(),
    val global: Map<String, Double> = emptyMap(),
) {
    val total: Double get() = table + local.values.sum() + global.values.sum()

    operator fun plus(other: CapacityUnits) = CapacityUnits(
        table + other.table, merge(local, other.local), merge(global, other.global))

    private fun merge(a: Map<String, Double>, b: Map<String, Double>): Map<String, Double> =
        (a.keys + b.keys).associateWith { a.getOrDefault(it, 0.0) + b.getOrDefault(it, 0.0) }
}
