package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.Condition
import tikronos.model.Item
import tikronos.model.ItemWrite
import tikronos.storage.Records
import tikronos.storage.Storage

internal class Items(private val db: Storage, private val tables: Tables) {
    fun get(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "Key", "ConsistentRead", "ProjectionExpression", "ExpressionAttributeNames")
        if (req.has("ConsistentRead") && !req["ConsistentRead"].isBoolean) invalid("Expected boolean ConsistentRead")
        val table = tables.find(Tables.tableName(req))
        val key = Storage.itemKey(table.id, partitionValue(keyAttributes(req), table.partitionKey))
        val names = AttributeNames(req)
        val fields = if (req.has("ProjectionExpression")) {
            req.string("ProjectionExpression").split(',').map { names.resolve(it.trim()) }
        } else null
        names.finish()
        if (fields != null && fields.distinct().size != fields.size) invalid("Overlapping projection paths")
        val item = db.read(db.snapshot(), key)?.let(Records::item) ?: return obj()
        return obj("Item" to wire(if (fields == null) item else item.filterKeys { it in fields }))
    }

    fun write(req: ObjectNode, put: Boolean): ObjectNode {
        val field = if (put) "Item" else "Key"
        req.fieldsOnly("TableName", field, "ConditionExpression", "ExpressionAttributeNames", "ReturnValues")
        val table = tables.find(Tables.tableName(req))
        val attributes = if (put) req.obj(field) else keyAttributes(req)
        val key = Storage.itemKey(table.id, partitionValue(attributes, table.partitionKey))
        val returns = if (req.has("ReturnValues")) req.string("ReturnValues") else "NONE"
        if (returns !in setOf("NONE", "ALL_OLD")) invalid("Invalid ReturnValues")
        val change = ItemWrite(condition(req, table.partitionKey), if (put) item(attributes) else null)
        val old = db.mutate(key) { bytes -> change.apply(bytes?.let(Records::item))?.let(Records::encodeItem) }
        return if (returns == "ALL_OLD" && old != null) obj("Attributes" to wire(Records.item(old))) else obj()
    }

    private fun condition(req: ObjectNode, hash: String): Condition {
        val names = AttributeNames(req)
        val condition = if (req.has("ConditionExpression")) {
            val match = absent.matchEntire(req.string("ConditionExpression").trim())
                ?: invalid("Expected attribute_not_exists(partitionKey)")
            if (names.resolve(match.groupValues[1]) != hash) invalid("Condition must reference the partition key")
            Condition.KEY_ABSENT
        } else Condition.UNCONDITIONAL
        names.finish()
        return condition
    }

    private fun wire(item: Item) = item.mapValues { (_, value) -> obj("S" to value) }

    private fun item(attributes: ObjectNode): Item {
        var size = 0
        return attributes.properties().associate { (name, attribute) ->
            if (name.isEmpty() || attribute.size() != 1) invalid("Expected a named string attribute")
            val value = attribute.string("S")
            size += utf8Size(name) + utf8Size(value)
            if (size > MAX_ITEM_BYTES) invalid("Item exceeds 400 KiB")
            name to value
        }
    }

    private fun keyAttributes(req: ObjectNode) = req.obj("Key").also {
        if (it.size() != 1) invalid("Expected one partition key")
    }

    private fun partitionValue(attributes: ObjectNode, hash: String): String {
        val attribute = attributes.obj(hash)
        val value = attribute.string("S")
        if (attribute.size() != 1 || utf8Size(value) !in 1..MAX_KEY_BYTES) invalid("Invalid partition key")
        return value
    }

    private fun utf8Size(value: String): Int {
        val bytes = value.encodeToByteArray()
        if (bytes.decodeToString() != value) invalid("Invalid Unicode string")
        return bytes.size
    }

    companion object {
        private const val MAX_ITEM_BYTES = 400 * 1024
        private const val MAX_KEY_BYTES = 2048
        private val absent = Regex("""attribute_not_exists\s*\(\s*(#?[a-zA-Z0-9_]+)\s*\)""")
    }
}
