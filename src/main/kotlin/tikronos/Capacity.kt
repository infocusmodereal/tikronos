package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import kotlin.math.max

internal class Capacity(req: ObjectNode) {
    private val mode =
        if (req.has("ReturnConsumedCapacity")) req.string("ReturnConsumedCapacity") else "NONE"

    init {
        if (mode !in setOf("TOTAL", "INDEXES", "NONE")) invalid("Invalid ReturnConsumedCapacity")
    }

    fun entry(table: String, units: Double): ObjectNode =
        obj("TableName" to table, "CapacityUnits" to units).apply {
            if (mode == "INDEXES") set<ObjectNode>("Table", obj("CapacityUnits" to units))
        }

    fun add(response: ObjectNode, table: String, units: Double) {
        if (mode != "NONE") response.set<ObjectNode>("ConsumedCapacity", entry(table, units))
    }

    fun batch(response: ObjectNode, units: Map<String, Double>) {
        if (mode != "NONE")
            response.set<ObjectNode>(
                "ConsumedCapacity",
                json.valueToTree(units.map { entry(it.key, it.value) }),
            )
    }
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
