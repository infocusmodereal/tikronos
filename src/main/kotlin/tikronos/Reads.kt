package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import org.tikv.shade.com.google.protobuf.ByteString
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.expression.Path
import tikronos.expression.RequestExpressions
import tikronos.expression.paths
import tikronos.expression.project
import tikronos.model.ScanOptions
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.itemSize
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.after
import tikronos.storage.baseItems
import tikronos.storage.projectedTo
import tikronos.storage.byteOrder
import tikronos.storage.bytes
import tikronos.storage.segmentEdge

internal class Reads(private val db: Storage, private val tables: Tables) {
    fun read(req: ObjectNode, query: Boolean): ObjectNode {
        val extra =
            if (query)
                listOf("KeyConditions", "KeyConditionExpression", "QueryFilter", "ScanIndexForward")
            else listOf("ScanFilter", "Segment", "TotalSegments")
        req.fieldsOnly(common + extra)
        val snapshot = db.snapshot()
        val table = tables.find(Tables.tableName(req), snapshot)
        val target = readTarget(req, table)
        val expressions = RequestExpressions(req)
        val range =
            if (query)
                target.schema.queryRange(
                    expressions.predicate("KeyConditionExpression", "KeyConditions"), target.index != null)
            else null
        val filter =
            expressions.predicate("FilterExpression", if (query) "QueryFilter" else "ScanFilter")
        if (query) validateQueryFilter(filter, target.schema)
        target.validateFilter(filter)
        val projection = expressions.projection()
        val countOnly = selectCount(req, projection)
        val available = target.projection(req, projection)
        val fetch = target.fetchesTable(req, projection, filter)
        expressions.finish()
        val forward = req.flag("ScanIndexForward", true)
        val limit = req.integer("Limit", 1..Int.MAX_VALUE) ?: Int.MAX_VALUE
        val strong = req.flag("ConsistentRead")
        val capacity = Capacity(req)
        val (first, end) = bounds(req, range, target.schema)
        val cursor = cursor(req, target, first, end)
        val startKey = if (forward && cursor != null) after(cursor) else first
        val endKey = if (!forward && cursor != null) cursor else end
        val rows = db.ranges.scan(startKey, endKey, forward, snapshot, ScanOptions(limit = limit))
            .map { Records.item(it.value.toByteArray()) }
        val page = collect(if (fetch) snapshot.baseItems(table, rows) else rows, limit, target, fetch)
        val included = page.items.filter { filter?.matches(it) != false }
        return obj("Count" to included.size, "ScannedCount" to page.items.size).apply {
            if (!countOnly) writeItems(included, available, projection)
            if (page.more)
                set<ObjectNode>(
                    "LastEvaluatedKey",
                    wireItem(target.key(page.items.last())),
                )
            capacity.add(this, table.name, target.capacity(page.bytes, page.fetchedBytes, strong))
        }
    }

    private fun ObjectNode.writeItems(items: List<Item>, available: Set<String>?, projection: List<Path>?) {
        val array = putArray("Items")
        items.forEach { item -> array.add(wireItem(project(item.projectedTo(available), projection))) }
    }

    private fun validateQueryFilter(filter: tikronos.expression.Predicate?, table: Table) {
        if (filter?.paths()?.any { it.root in setOf(table.partitionKey, table.sortKey) } == true) {
            invalid("QueryFilter cannot reference key attributes")
        }
    }

    private fun cursor(
        req: ObjectNode,
        target: ReadTarget,
        first: ByteString,
        end: ByteString,
    ): ByteString? {
        if (!req.has("ExclusiveStartKey")) return null
        val address = target.address(parseItem(req.obj("ExclusiveStartKey")))
        if (byteOrder.compare(address, first) < 0 || byteOrder.compare(address, end) >= 0) {
            invalid("ExclusiveStartKey outside query bounds or scan segment")
        }
        return address
    }

    private fun selectCount(req: ObjectNode, projection: List<Path>?): Boolean {
        if (!req.has("Select")) return false
        val select = req.string("Select")
        if (select !in setOf("COUNT", "ALL_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES"))
            invalid("Invalid Select")
        if ((projection != null) != (select == "SPECIFIC_ATTRIBUTES")) {
            invalid("Select=SPECIFIC_ATTRIBUTES requires AttributesToGet or ProjectionExpression")
        }
        return select == "COUNT"
    }

    private fun bounds(
        req: ObjectNode,
        range: KeyRange?,
        table: Table,
    ): Pair<ByteString, ByteString> {
        if (range != null) return bytes(range.start) to bytes(range.end)
        val total = req.integer("TotalSegments", 1..MAX_SEGMENTS)
        val index = req.integer("Segment", 0 until (total ?: 1))
        if ((total == null) != (index == null))
            invalid("Segment and TotalSegments must be supplied together")
        val count = total ?: 1
        val segment = index ?: 0
        return table.segmentEdge(segment, count) to table.segmentEdge(segment + 1, count)
    }

    companion object {
        private val common =
            listOf(
                "TableName",
                "IndexName",
                "Limit",
                "ExclusiveStartKey",
                "Select",
                "ConsistentRead",
                "ReturnConsumedCapacity",
                "ProjectionExpression",
                "FilterExpression",
                "ExpressionAttributeNames",
                "ExpressionAttributeValues",
                "AttributesToGet",
                "ConditionalOperator",
            )
    }
}

private data class Page(val items: List<Item>, val bytes: Int, val fetchedBytes: Int, val more: Boolean)

private fun collect(rows: Sequence<Item>, limit: Int, target: ReadTarget, fetch: Boolean): Page {
    val items = mutableListOf<Item>()
    var bytes = 0
    var fetchedBytes = 0
    for (item in rows) {
        val size = target.storedSize(item)
        val fetched = if (fetch) roundedReadBytes(itemSize(item)) else 0
        val pageSize = if (fetch) roundedReadBytes(bytes + size) + fetchedBytes + fetched else bytes + size
        if (pageSize <= PAGE_BYTES) {
            items += item
            bytes += size
            fetchedBytes += fetched
            if (items.size < limit && pageSize < PAGE_BYTES) continue
        }
        return Page(items, bytes, fetchedBytes, true)
    }
    return Page(items, bytes, fetchedBytes, false)
}

private fun roundedReadBytes(bytes: Int) = readUnits(bytes, true).toInt() * READ_BLOCK_BYTES
private const val READ_BLOCK_BYTES = 4096

internal fun ObjectNode.integer(name: String, range: IntRange): Int? =
    get(name)?.let {
        if (!it.isIntegralNumber || !it.canConvertToInt() || it.intValue() !in range)
            invalid("Invalid $name")
        it.intValue()
    }

private const val MAX_SEGMENTS = 1_000_000
