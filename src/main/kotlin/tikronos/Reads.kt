package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import org.tikv.shade.com.google.protobuf.ByteString
import tikronos.expression.Path
import tikronos.expression.RequestExpressions
import tikronos.expression.paths
import tikronos.expression.project
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.itemSize
import tikronos.model.parseItem
import tikronos.model.wireItem
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.after
import tikronos.storage.byteOrder
import tikronos.storage.bytes
import tikronos.storage.key
import tikronos.storage.segmentEdge

internal class Reads(private val db: Storage, private val tables: Tables) {
    fun read(req: ObjectNode, query: Boolean): ObjectNode {
        val extra =
            if (query)
                listOf("KeyConditions", "KeyConditionExpression", "QueryFilter", "ScanIndexForward")
            else listOf("ScanFilter", "Segment", "TotalSegments")
        req.fieldsOnly(common + extra)
        val table = tables.find(Tables.tableName(req))
        val expressions = RequestExpressions(req)
        val range =
            if (query)
                table.queryRange(expressions.predicate("KeyConditionExpression", "KeyConditions"))
            else null
        val filter =
            expressions.predicate("FilterExpression", if (query) "QueryFilter" else "ScanFilter")
        if (query) validateQueryFilter(filter, table)
        val projection = expressions.projection()
        val countOnly = selectCount(req, projection)
        expressions.finish()
        val forward = req.flag("ScanIndexForward", true)
        val limit = req.integer("Limit", 1..Int.MAX_VALUE) ?: Int.MAX_VALUE
        val strong = req.flag("ConsistentRead")
        val capacity = Capacity(req)
        val (first, end) = bounds(req, range, table)
        val cursor = cursor(req, table, first, end)
        val startKey = if (forward && cursor != null) after(cursor) else first
        val endKey = if (!forward && cursor != null) cursor else end
        val page =
            collect(
                db.ranges.scan(startKey, endKey, forward).map {
                    Records.item(it.value.toByteArray())
                },
                limit,
            )
        val included = page.items.filter { filter?.matches(it) != false }
        return obj("Count" to included.size, "ScannedCount" to page.items.size).apply {
            if (!countOnly)
                set<ObjectNode>(
                    "Items",
                    json.valueToTree(included.map { wireItem(project(it, projection)) }),
                )
            if (page.more)
                set<ObjectNode>(
                    "LastEvaluatedKey",
                    json.valueToTree(wireItem(table.key(page.items.last(), false))),
                )
            capacity.add(this, table.name, readUnits(page.bytes, strong))
        }
    }

    private fun validateQueryFilter(filter: tikronos.expression.Predicate?, table: Table) {
        if (filter?.paths()?.any { it.root in setOf(table.partitionKey, table.sortKey) } == true) {
            invalid("QueryFilter cannot reference key attributes")
        }
    }

    private fun cursor(
        req: ObjectNode,
        table: Table,
        first: ByteString,
        end: ByteString,
    ): ByteString? {
        if (!req.has("ExclusiveStartKey")) return null
        val address = bytes(table.address(table.key(parseItem(req.obj("ExclusiveStartKey")))))
        if (byteOrder.compare(address, first) < 0 || byteOrder.compare(address, end) >= 0) {
            invalid("ExclusiveStartKey outside query bounds or scan segment")
        }
        return address
    }

    private fun selectCount(req: ObjectNode, projection: List<Path>?): Boolean {
        if (!req.has("Select")) return false
        val select = req.string("Select")
        if (
            select !in setOf("COUNT", "ALL_ATTRIBUTES", "SPECIFIC_ATTRIBUTES") ||
                projection != null && select != "SPECIFIC_ATTRIBUTES"
        )
            invalid("Invalid Select")
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

private data class Page(val items: List<Item>, val bytes: Int, val more: Boolean)

private fun collect(rows: Sequence<Item>, limit: Int): Page {
    val items = mutableListOf<Item>()
    var bytes = 0
    for (item in rows) {
        val size = itemSize(item)
        if (bytes + size <= PAGE_BYTES) {
            items += item
            bytes += size
            if (items.size < limit && bytes < PAGE_BYTES) continue
        }
        return Page(items, bytes, true)
    }
    return Page(items, bytes, false)
}

internal fun ObjectNode.integer(name: String, range: IntRange): Int? =
    get(name)?.let {
        if (!it.isIntegralNumber || !it.canConvertToInt() || it.intValue() !in range)
            invalid("Invalid $name")
        it.intValue()
    }

private const val MAX_SEGMENTS = 1_000_000
