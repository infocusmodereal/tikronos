package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.expression.Path
import tikronos.expression.RequestExpressions
import tikronos.expression.project
import tikronos.model.Item
import tikronos.protocol.BatchWrite
import tikronos.model.WriteResult
import tikronos.model.CapacityUnits
import tikronos.model.Table
import tikronos.model.Value
import tikronos.model.itemSize
import tikronos.storage.ContentionLimitExceeded
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.key
import tikronos.storage.entry

internal class Batches(private val db: Storage, private val tables: Tables) {
    private val writer = ItemWriter(db)

    fun get(req: ObjectNode): ObjectNode {
        req.fieldsOnly("RequestItems", "ReturnConsumedCapacity")
        val capacity = Capacity(req)
        val groups =
            requests(req).map { (name, node) ->
                readGroup(tables.find(name), node as? ObjectNode ?: invalid("Expected key group"))
            }
        if (groups.sumOf { it.keys.size.toLong() } > MAX_BATCH_GET)
            invalid("At most $MAX_BATCH_GET keys per BatchGetItem")
        val responses = obj()
        val unprocessed = obj()
        val units = mutableMapOf<String, Double>()
        var remaining = RESPONSE_BUDGET
        val snapshot = db.snapshot()
        for (group in groups) {
            val partitions = mutableMapOf<Value, Int>()
            val result = json.createArrayNode()
            val pending = json.createArrayNode()
            fun readKey(key: Item) {
                val partition = key.getValue(group.table.partitionKey)
                val used = partitions.getOrDefault(partition, 0)
                val item = db.read(snapshot, group.table.address(key))?.let(Records::item)
                val bytes = itemSize(item.orEmpty())
                val projected = item?.let { wireItem(project(it, group.paths)) }
                val cost = projected?.let { json.writeValueAsBytes(it).size + 1 } ?: 0
                if (used + bytes > PAGE_BYTES || cost > remaining) {
                    pending.add(json.valueToTree<ObjectNode>(wireItem(key)))
                } else {
                    if (projected != null) result.add(json.valueToTree<ObjectNode>(projected))
                    remaining -= cost
                    partitions[partition] = used + bytes
                    units.merge(group.table.name, readUnits(bytes, group.strong), Double::plus)
                }
            }
            group.keys.forEach(::readKey)
            responses.set<ObjectNode>(group.table.name, result)
            if (!pending.isEmpty)
                unprocessed.set<ObjectNode>(
                    group.table.name,
                    group.request.deepCopy().apply { set<ObjectNode>("Keys", pending) },
                )
        }
        return obj("Responses" to responses, "UnprocessedKeys" to unprocessed).also {
            capacity.batch(it, units)
        }
    }

    private fun readGroup(table: Table, req: ObjectNode): ReadGroup {
        req.fieldsOnly(
            "Keys",
            "ConsistentRead",
            "ProjectionExpression",
            "ExpressionAttributeNames",
            "AttributesToGet",
        )
        val expr = RequestExpressions(req)
        val paths = expr.projection()
        expr.finish()
        val entries = req.path("Keys")
        if (!entries.isArray || entries.isEmpty || entries.size() > MAX_BATCH_GET)
            invalid("Invalid key count")
        val keys = entries.map { table.key(parseItem(it)) }
        if (keys.distinct().size != keys.size) invalid("BatchGetItem keys contain duplicates")
        return ReadGroup(table, req, keys, paths, req.flag("ConsistentRead"))
    }

    fun write(req: ObjectNode): ObjectNode {
        req.fieldsOnly("RequestItems", "ReturnConsumedCapacity", "ReturnItemCollectionMetrics")
        if (
            req.has("ReturnItemCollectionMetrics") &&
                req.string("ReturnItemCollectionMetrics") !in setOf("NONE", "SIZE")
        ) {
            invalid("Invalid ReturnItemCollectionMetrics")
        }
        val capacity = Capacity(req)
        val operations =
            requests(req).flatMap { (name, entries) ->
                val table = tables.find(name)
                if (!entries.isArray || entries.isEmpty || entries.size() > MAX_BATCH_WRITE)
                    invalid("Invalid write request list length")
                entries.map {
                    prepare(table, it as? ObjectNode ?: invalid("Expected write request"))
                }
            }
        if (
            operations.size > MAX_BATCH_WRITE ||
                operations.distinctBy { it.address }.size != operations.size
        ) {
            invalid(
                "BatchWriteItem keys contain duplicates or exceed the maximum list length of $MAX_BATCH_WRITE"
            )
        }
        return executeBatch(operations, capacity) { op ->
            writer.mutate(op.table, op.key) { op.item }
        }
    }

    private fun prepare(table: Table, request: ObjectNode): BatchWrite {
        request.fieldsOnly("PutRequest", "DeleteRequest")
        if (request.size() != 1) invalid("Expected exactly one write")
        val put = request.has("PutRequest")
        val body = request.obj(if (put) "PutRequest" else "DeleteRequest")
        val field = if (put) "Item" else "Key"
        body.fieldsOnly(field)
        val item = parseItem(body.obj(field))
        val key = table.key(item, exact = !put)
        if (put) table.indexes.forEach { it.entry(table, item) }
        return BatchWrite(
            table,
            table.address(key),
            request,
            if (put) item.also { encodeChecked(it) } else null,
            key,
        )
    }

    private fun requests(req: ObjectNode) =
        req.obj("RequestItems").also { if (it.isEmpty) invalid("Empty batch") }.properties()

    private data class ReadGroup(
        val table: Table,
        val request: ObjectNode,
        val keys: List<Item>,
        val paths: List<Path>?,
        val strong: Boolean,
    )

    companion object {
        private const val RESPONSE_BUDGET = 16 * PAGE_BYTES
    }
}

private const val MAX_BATCH_GET = 100
private const val MAX_BATCH_WRITE = 100

internal fun executeBatch(
    operations: List<BatchWrite>,
    capacity: Capacity,
    commit: (BatchWrite) -> WriteResult,
): ObjectNode {
    val unprocessed = obj()
    val units = mutableMapOf<String, CapacityUnits>()
    for (op in operations) {
        try {
            val result = commit(op)
            units.merge(op.table.name, writeCapacity(result.table, result.old, result.new), CapacityUnits::plus)
        } catch (_: ContentionLimitExceeded) {
            unprocessed.withArray(op.table.name).add(op.request)
        }
    }
    if (units.isEmpty()) throw ContentionLimitExceeded()
    return obj("UnprocessedItems" to unprocessed).also { capacity.batchUsage(it, units) }
}
