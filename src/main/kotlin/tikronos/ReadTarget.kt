package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import org.tikv.shade.com.google.protobuf.ByteString
import tikronos.expression.Path
import tikronos.expression.Predicate
import tikronos.expression.paths
import tikronos.model.CapacityUnits
import tikronos.model.itemSize
import tikronos.model.Index
import tikronos.model.Item
import tikronos.model.Table
import tikronos.storage.entry
import tikronos.storage.schema
import tikronos.storage.projected
import tikronos.storage.projectedTo
import tikronos.storage.address
import tikronos.storage.bytes
import tikronos.storage.key

internal class ReadTarget(val table: Table, val index: Index?) {
    private val attributes = index?.projected(table)
    val schema = index?.schema(table) ?: table

    fun key(item: Item): Item = table.key(item, false) +
        (index?.let { schema.key(item, false) } ?: emptyMap())

    fun address(key: Item): ByteString {
        if (key(key).size != key.size) invalid("Unexpected ExclusiveStartKey attributes")
        return bytes(index?.entry(table, key) ?: table.address(key))
    }

    fun storedSize(item: Item): Int = itemSize(item.projectedTo(attributes))

    fun fetchesTable(req: ObjectNode, paths: List<Path>?, filter: Predicate?): Boolean {
        val projected = attributes ?: return false
        val unprojected = req.path("Select").asText() == "ALL_ATTRIBUTES" ||
            paths?.any { it.root !in projected } == true ||
            filter?.paths()?.any { it.root !in projected } == true
        return index?.local == true && unprojected
    }

    fun capacity(bytes: Int, fetchedBytes: Int, strong: Boolean): CapacityUnits {
        val units = readUnits(bytes, strong)
        val fetched = if (fetchedBytes == 0) 0.0 else readUnits(fetchedBytes, strong)
        return when {
            index == null -> CapacityUnits(units)
            index.local -> CapacityUnits(fetched, local = mapOf(index.name to units))
            else -> CapacityUnits(0.0, global = mapOf(index.name to units))
        }
    }

    fun validateFilter(filter: Predicate?) {
        val available = attributes
        if (index?.local == false && available != null &&
            filter?.paths()?.any { it.root !in available } == true)
            invalid("Filter references attributes not projected in the index")
    }

    fun projection(req: ObjectNode, paths: List<Path>?): Set<String>? {
        val available = attributes
        val select = req.path("Select").asText(if (index == null) "ALL_ATTRIBUTES" else "ALL_PROJECTED_ATTRIBUTES")
        if (select == "ALL_PROJECTED_ATTRIBUTES" && index == null) invalid("Select requires an index")
        if (index?.local == false && available != null) {
            if (select == "ALL_ATTRIBUTES" || paths?.any { it.root !in available } == true)
                invalid("Requested attributes are not projected in the index")
        }
        return if (paths != null || select == "ALL_ATTRIBUTES") null else available
    }
}

internal fun readTarget(req: ObjectNode, table: Table): ReadTarget {
    val index = if (req.has("IndexName")) {
        val name = req.string("IndexName")
        table.indexes.firstOrNull { it.name == name } ?: invalid("Unknown index $name")
    } else null
    if (index != null && index.status != "ACTIVE")
        invalid("Index is not ACTIVE")
    if (index?.local == false && req.flag("ConsistentRead"))
        invalid("Consistent reads are not supported on global indexes")
    return ReadTarget(table, index)
}
