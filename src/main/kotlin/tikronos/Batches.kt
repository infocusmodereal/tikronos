package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import org.tikv.common.Snapshot
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.expression.Path
import tikronos.expression.RequestExpressions
import tikronos.expression.project
import tikronos.model.Item
import tikronos.protocol.BatchWrite
import kotlinx.coroutines.sync.Semaphore
import tikronos.model.WriteIntent
import tikronos.model.Table
import tikronos.model.Value
import tikronos.model.itemSize
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.key
import tikronos.storage.entry
import tikronos.storage.readBatch

internal class Batches(private val db: Storage, private val tables: Tables, private val executions: Semaphore? = null) {
    private val writer = ItemWriter(db)

    fun get(req: ObjectNode): ObjectNode {
        req.fieldsOnly("RequestItems", "ReturnConsumedCapacity")
        val capacity = Capacity(req)
        val snapshot = db.snapshot()
        val groups =
            requests(req).map { (name, node) ->
                readGroup(tables.find(name, snapshot), node as? ObjectNode ?: invalid("Expected key group"))
            }
        if (groups.sumOf { it.keys.size.toLong() } > MAX_BATCH_GET)
            invalid("At most $MAX_BATCH_GET keys per BatchGetItem")
        val responses = obj()
        val unprocessed = obj()
        val units = mutableMapOf<String, Double>()
        var remaining = RESPONSE_BUDGET
        for (group in groups) {
            val partitions = mutableMapOf<Value, Int>()
            val result = json.createArrayNode()
            val pending = json.createArrayNode()
            fun readKey(key: Item, stored: ByteArray?) {
                val partition = key.getValue(group.table.partitionKey)
                val used = partitions.getOrDefault(partition, 0)
                val item = stored?.let(Records::item)
                val bytes = itemSize(item.orEmpty())
                val projected = item?.let { wireItem(project(it, group.paths)) }
                val cost = projected?.let { json.writeValueAsBytes(it).size + 1 } ?: 0
                if (used + bytes > PAGE_BYTES || cost > remaining) {
                    pending.add(wireItem(key))
                } else {
                    if (projected != null) result.add(projected)
                    remaining -= cost
                    partitions[partition] = used + bytes
                    units.merge(group.table.name, readUnits(bytes, group.strong), Double::plus)
                }
            }
            group.forEachItem(snapshot, ::readKey)
            responses.set<ObjectNode>(group.table.name, result)
            if (!pending.isEmpty)
                unprocessed.set<ObjectNode>(
                    group.table.name,
                    group.request.deepCopy().apply { set<ObjectNode>("Keys", pending) },
                )
        }
        return obj().apply {
            set<ObjectNode>("Responses", responses)
            set<ObjectNode>("UnprocessedKeys", unprocessed)
            capacity.batch(this, units)
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

    private fun ReadGroup.forEachItem(snapshot: Snapshot, consume: (Item, ByteArray?) -> Unit) {
        keys.chunked(READ_CHUNK).forEach { chunk ->
            val addresses = chunk.map(table::address)
            val found = snapshot.readBatch(addresses)
            chunk.forEachIndexed { index, key -> consume(key, found[addresses[index]]) }
        }
    }

    fun write(req: ObjectNode, checkActive: () -> Unit = {}): ObjectNode {
        req.fieldsOnly("RequestItems", "ReturnConsumedCapacity", "ReturnItemCollectionMetrics")
        if (
            req.has("ReturnItemCollectionMetrics") &&
                req.string("ReturnItemCollectionMetrics") !in setOf("NONE", "SIZE")
        ) {
            invalid("Invalid ReturnItemCollectionMetrics")
        }
        val capacity = Capacity(req)
        val snapshot = db.snapshot()
        var spare = executions
        val operations =
            requests(req).flatMap { (name, entries) ->
                val table = tables.find(name, snapshot)
                // Scheduling hint only. Each commit validates the current table generation and write plan.
                val plan = db.catalog.cached(Storage.CATALOG + name, snapshot.version)?.plan
                if (plan == null || plan.streamArn != null || table.indexes.any { it.status != "ACTIVE" }) spare = null
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
        return executeBatch(operations, capacity, spare, checkActive) { op ->
            val intent = WriteIntent.select(true, capacity.requested, op.item) { op.item }
            writer.mutate(op.table, op.key, intent)
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
            if (put) item.also(::validateItem) else null,
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
        private const val READ_CHUNK = 16
    }
}

private const val MAX_BATCH_GET = 100
private const val MAX_BATCH_WRITE = 100
