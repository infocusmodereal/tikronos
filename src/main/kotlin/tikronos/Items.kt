package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode

internal class Items(private val db: Storage, private val tables: Tables) {
    fun get(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "Key", "ConsistentRead", "ProjectionExpression", "ExpressionAttributeNames")
        if (req.has("ConsistentRead") && !req["ConsistentRead"].isBoolean) invalid("Expected boolean ConsistentRead")
        val key = itemKey(req, "Key")
        val fields = projection(req)
        val item = db.read(db.snapshot(), key) ?: return obj()
        return obj("Item" to if (fields == null) item else item.retain(fields))
    }

    fun write(req: ObjectNode, put: Boolean): ObjectNode {
        val field = if (put) "Item" else "Key"
        req.fieldsOnly("TableName", field, "ConditionExpression", "ExpressionAttributeNames", "ReturnValues")
        val table = tables.describe(Tables.tableName(req))
        val hash = table["KeySchema"][0].string("AttributeName")
        val key = encodeKey(table, req.obj(field), hash, field)
        val returns = if (req.has("ReturnValues")) req.string("ReturnValues") else "NONE"
        if (returns !in setOf("NONE", "ALL_OLD")) invalid("Invalid ReturnValues")
        val replacement = if (put) req.obj(field) else null
        val change = ItemWrite(req, hash, replacement)
        val old = db.mutate(key, change::apply)
        return if (returns == "ALL_OLD" && old != null) obj("Attributes" to old) else obj()
    }

    private fun itemKey(req: ObjectNode, field: String): String {
        val table = tables.describe(Tables.tableName(req))
        return encodeKey(table, req.obj(field), table["KeySchema"][0].string("AttributeName"), field)
    }

    private fun encodeKey(table: ObjectNode, item: ObjectNode, hash: String, field: String): String {
        if (field == "Key" && item.size() != 1) invalid("Unexpected key fields")
        val attribute = item.obj(hash)
        val value = attribute.string("S")
        if (attribute.size() != 1 || value.encodeToByteArray().size !in 1..MAX_KEY_BYTES) invalid("Invalid key")
        return "tikronos/v1/item/${table.string("TableId")}/$value"
    }

    private fun projection(req: ObjectNode): List<String>? =
        if (req.has("ProjectionExpression")) req.string("ProjectionExpression").split(',')
            .map { ItemWrite.attributeName(it.trim(), req) }
        else null

    companion object {
        private const val MAX_KEY_BYTES = 2048
    }
}
