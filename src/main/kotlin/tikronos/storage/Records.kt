package tikronos.storage

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import tikronos.codec.parseItem
import tikronos.codec.wireItem
import tikronos.model.Index
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.Throughput
import tikronos.model.Value

/** A leading version byte distinguishes compact records from the original JSON documents. */
internal object Records {
    private val mapper = JsonMapper()
    private const val STRING_VERSION: Byte = 1
    private const val TYPED_VERSION: Byte = 2

    // On base rows, this version certifies that partial indexes were written with their projections.
    private const val PROJECTED_INDEX_VERSION: Byte = 3

    fun hasProjectedIndexes(bytes: ByteArray?): Boolean = bytes?.firstOrNull() == PROJECTED_INDEX_VERSION

    fun encodeItem(item: Item, projectedIndexes: Boolean = false): ByteArray =
        byteArrayOf(if (projectedIndexes) PROJECTED_INDEX_VERSION else TYPED_VERSION) +
            mapper.writeValueAsBytes(wireItem(item))

    fun item(bytes: ByteArray): Item {
        if (bytes.first() == TYPED_VERSION || hasProjectedIndexes(bytes))
            return parseItem(mapper.readTree(bytes.inputStream(1, bytes.size - 1)))
        val (legacy, record) = decode(bytes)
        return record.properties().associate { (name, value) ->
            name to Value.text(checkNotNull((if (legacy) value["S"] else value).textValue()))
        }
    }

    fun itemAt(table: Table, address: String, bytes: ByteArray): Item {
        val item = item(bytes)
        val key = table.key(item, exact = false)
        check(table.address(key) == address) { "Stored item primary key does not match its address" }
        return item
    }

    fun encodeTable(table: Table): ByteArray {
        val record =
            mapOf(
                "name" to table.name,
                "id" to table.id,
                "partitionKey" to table.partitionKey,
                "createdAt" to table.createdAt,
                "partitionType" to table.partitionType,
                "hashed" to table.hashed,
                "indexes" to table.indexes,
                "tags" to table.tags,
                "sortKey" to table.sortKey,
                "sortType" to table.sortType,
                "throughput" to
                    table.throughput?.let { mapOf("read" to it.read, "write" to it.write) },
            )
        return byteArrayOf(TYPED_VERSION) + mapper.writeValueAsBytes(record)
    }

    fun table(bytes: ByteArray): Table {
        val (legacy, node) = decode(bytes)
        return if (legacy) {
            Table(
                node["TableName"].textValue(),
                node["TableId"].textValue(),
                node["KeySchema"][0]["AttributeName"].textValue(),
                node["CreationDateTime"].longValue(),
                node["ProvisionedThroughput"]?.let {
                    Throughput(
                        it["ReadCapacityUnits"].longValue(),
                        it["WriteCapacityUnits"].longValue(),
                    )
                },
            )
        } else {
            Table(
                node["name"].textValue(),
                node["id"].textValue(),
                node["partitionKey"].textValue(),
                node["createdAt"].longValue(),
                node["throughput"].takeUnless(JsonNode::isNull)?.let {
                    Throughput(it["read"].longValue(), it["write"].longValue())
                },
                node.path("partitionType").asText("S"),
                node.path("sortKey").textValue(),
                node.path("sortType").textValue(),
                node.path("hashed").asBoolean(false),
                node.path("indexes").map(::index),
                node.path("tags").properties().associate { it.key to it.value.textValue() },
            )
        }
    }

    private fun index(node: JsonNode) = Index(
        node["name"].textValue(), node["id"].textValue(),
        node["partitionKey"].textValue(), node["partitionType"].textValue(),
        node.path("sortKey").textValue(), node.path("sortType").textValue(),
        node.path("local").asBoolean(), node.path("projection").asText("ALL"),
        node.path("attributes").map(JsonNode::textValue),
        node.path("throughput").takeUnless { it.isNull || it.isMissingNode }?.let {
            Throughput(it["read"].longValue(), it["write"].longValue())
        },
        node.path("status").asText("ACTIVE"), node.path("cursor").textValue(),
    )

    private fun decode(bytes: ByteArray): Pair<Boolean, JsonNode> =
        when (bytes.first()) {
            '{'.code.toByte() -> true to mapper.readTree(bytes)
            STRING_VERSION,
            TYPED_VERSION -> false to mapper.readTree(bytes.inputStream(1, bytes.size - 1))
            else -> error("Unknown record version")
        }
}
