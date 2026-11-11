package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import kotlin.math.max
import tikronos.model.CapacityUnits
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.itemSize
import tikronos.storage.existingEntry
import tikronos.storage.projected
import tikronos.storage.projectedTo

internal class Capacity(req: ObjectNode) {
    private val mode =
        if (req.has("ReturnConsumedCapacity")) req.string("ReturnConsumedCapacity") else "NONE"

    init {
        if (mode !in setOf("TOTAL", "INDEXES", "NONE")) invalid("Invalid ReturnConsumedCapacity")
    }

    val requested: Boolean get() = mode != "NONE"

    private fun entry(table: String, usage: CapacityUnits): ObjectNode =
        obj("TableName" to table, "CapacityUnits" to usage.total).apply {
            if (mode == "INDEXES") {
                set<ObjectNode>("Table", obj("CapacityUnits" to usage.table))
                if (usage.local.isNotEmpty()) set<ObjectNode>("LocalSecondaryIndexes", breakdown(usage.local))
                if (usage.global.isNotEmpty()) set<ObjectNode>("GlobalSecondaryIndexes", breakdown(usage.global))
            }
        }

    private fun breakdown(units: Map<String, Double>): ObjectNode =
        json.valueToTree(units.mapValues { obj("CapacityUnits" to it.value) })

    fun add(response: ObjectNode, table: String, units: Double) = add(response, table, CapacityUnits(units))

    fun add(response: ObjectNode, table: String, usage: CapacityUnits) {
        if (mode != "NONE") response.set<ObjectNode>("ConsumedCapacity", entry(table, usage))
    }

    fun batch(response: ObjectNode, units: Map<String, Double>) =
        batchUsage(response, units.mapValues { CapacityUnits(it.value) })

    fun batchUsage(response: ObjectNode, units: Map<String, CapacityUnits>) {
        if (mode != "NONE") response.set<ObjectNode>(
            "ConsumedCapacity", json.valueToTree(units.map { entry(it.key, it.value) }))
    }

}

internal fun writeCapacity(table: Table, old: Item?, new: Item?): CapacityUnits {
    val indexes = table.indexes.associateWith { index ->
        val before = old?.let { index.existingEntry(table, it) }
        val after = new?.let { index.existingEntry(table, it) }
        val projected = index.projected(table)
        val previous = old?.takeIf { before != null }?.projectedTo(projected)
        val next = new?.takeIf { after != null }?.projectedTo(projected)
        when {
            previous == next -> 0.0
            before != null && after != null && before != after ->
                writeUnits(itemSize(previous.orEmpty())) + writeUnits(itemSize(next.orEmpty()))
            else -> writeUnits(maxOf(itemSize(previous.orEmpty()), itemSize(next.orEmpty())))
        }
    }.filterValues { it > 0 }
    return CapacityUnits(
        writeUnits(maxOf(itemSize(old.orEmpty()), itemSize(new.orEmpty()))),
        indexes.filterKeys { it.local }.mapKeys { it.key.name },
        indexes.filterKeys { !it.local }.mapKeys { it.key.name },
    )
}

internal fun readUnits(bytes: Int, strong: Boolean) =
    max(1, (bytes + RCU_BYTES - 1) / RCU_BYTES) * if (strong) 1.0 else EVENTUAL_FACTOR

internal fun writeUnits(bytes: Int) = max(1, (bytes + WCU_BYTES - 1) / WCU_BYTES).toDouble()

internal fun ObjectNode.flag(name: String, default: Boolean = false): Boolean =
    get(name)?.let { if (it.isBoolean) it.booleanValue() else invalid("Expected boolean $name") }
        ?: default

internal const val PAGE_BYTES = 1024 * 1024

private const val RCU_BYTES = 4096
private const val WCU_BYTES = 1024
private const val EVENTUAL_FACTOR = 0.5
