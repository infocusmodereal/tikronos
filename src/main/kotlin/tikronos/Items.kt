package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.expression.Action
import tikronos.expression.RequestExpressions
import tikronos.expression.patch
import tikronos.expression.paths
import tikronos.expression.project
import tikronos.model.ConditionFailed
import tikronos.model.Item
import tikronos.model.MAX_DEPTH
import tikronos.model.MAX_ITEM_BYTES
import tikronos.model.itemSize
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.key

internal class Items(private val db: Storage, private val tables: Tables) {
    private val writer = ItemWriter(db)

    fun get(req: ObjectNode): ObjectNode {
        req.fieldsOnly(
            "TableName",
            "Key",
            "ConsistentRead",
            "ProjectionExpression",
            "ExpressionAttributeNames",
            "AttributesToGet",
            "ReturnConsumedCapacity",
        )
        val snapshot = db.snapshot()
        val table = tables.find(Tables.tableName(req), snapshot)
        val key = table.key(parseItem(req.obj("Key")))
        val expressions = RequestExpressions(req)
        val paths = expressions.projection()
        expressions.finish()
        val strong = req.flag("ConsistentRead")
        val capacity = Capacity(req)
        val item = db.read(snapshot, table.address(key))?.let(Records::item)
        return obj().apply {
            if (item != null)
                set<ObjectNode>("Item", json.valueToTree(wireItem(project(item, paths))))
            capacity.add(this, table.name, readUnits(itemSize(item.orEmpty()), strong))
        }
    }

    fun write(req: ObjectNode, operation: String): ObjectNode {
        val put = operation == "PutItem"
        val update = operation == "UpdateItem"
        val field = if (put) "Item" else "Key"
        req.writeFields(field, update)
        val initial = db.transaction()
        val table = tables.find(Tables.tableName(req), initial)
        val input = parseItem(req.obj(field))
        val key = table.key(input, exact = !put)
        val expressions = RequestExpressions(req)
        val condition = expressions.predicate("ConditionExpression", "Expected")
        val actions = expressions.updates()
        if (actions.any { it.path.root in key }) invalid("Cannot update a key attribute")
        expressions.finish()
        val returns = req.returnMode(update)
        val onFailure = req.path("ReturnValuesOnConditionCheckFailure").asText("NONE")
        if (onFailure !in setOf("NONE", "ALL_OLD"))
            invalid("Invalid ReturnValuesOnConditionCheckFailure")
        val capacity = Capacity(req)
        val result =
            writer.mutate(table, key, initial) { old ->
                    if (condition?.matches(old.orEmpty()) == false) {
                        throw ConditionFailed(old.takeIf { onFailure == "ALL_OLD" })
                    }
                    when (operation) {
                            "PutItem" -> input
                            "UpdateItem" -> {
                                updated(old, key, actions)
                            }
                            "DeleteItem" -> null
                            else -> error("Unknown write")
                        }
                }
        return obj().apply {
            addReturned(returns, result.old, result.new, actions)
            capacity.add(
                this,
                table.name,
                writeCapacity(result.table, result.old, result.new),
            )
        }
    }

    private fun updated(old: Item?, key: Item, actions: List<Action>): Item? {
        val base = old ?: key
        val changes = actions.associate { it.path to it.evaluate(base) }
        if (old == null && actions.isNotEmpty() && changes.values.all { it == null }) return null
        return patch(base, changes)
    }

    private fun ObjectNode.writeFields(field: String, update: Boolean) {
        val allowed =
            listOf(
                "TableName",
                field,
                "ConditionExpression",
                "ExpressionAttributeNames",
                "Expected",
                "ExpressionAttributeValues",
                "ConditionalOperator",
                "ReturnValues",
                "ReturnConsumedCapacity",
                "ReturnValuesOnConditionCheckFailure",
            )
        fieldsOnly(
            if (update) allowed + listOf("AttributeUpdates", "UpdateExpression") else allowed
        )
    }

    private fun ObjectNode.addReturned(
        returns: String,
        previous: Item?,
        written: Item?,
        actions: List<Action>,
    ) {
        val state = if (returns.endsWith("OLD")) previous else written
        if (returns != "NONE" && state != null) {
            val result =
                if (returns.startsWith("UPDATED")) project(state, actions.map(Action::path))
                else state
            if (result.isNotEmpty())
                set<ObjectNode>("Attributes", json.valueToTree(wireItem(result)))
        }
    }

    private fun ObjectNode.returnMode(update: Boolean): String {
        val mode = if (has("ReturnValues")) string("ReturnValues") else "NONE"
        val allowed =
            if (update) setOf("NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW")
            else setOf("NONE", "ALL_OLD")
        return mode.takeIf { it in allowed } ?: invalid("Invalid ReturnValues")
    }
}

internal fun encodeChecked(item: Item): ByteArray {
    if (item.values.any { it.depth > MAX_DEPTH }) invalid("Document exceeds maximum nesting depth")
    if (itemSize(item) > MAX_ITEM_BYTES) invalid("Item exceeds 400 KiB")
    return Records.encodeItem(item)
}
