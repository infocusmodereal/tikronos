package tikronos.storage

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.Throughput

/** A leading version byte distinguishes compact records from the original JSON documents. */
internal object Records {
    private val mapper = JsonMapper()
    private const val VERSION: Byte = 1

    fun encodeItem(item: Item): ByteArray = byteArrayOf(VERSION) + mapper.writeValueAsBytes(item)

    fun item(bytes: ByteArray): Item {
        val (legacy, record) = decode(bytes)
        return record.properties().associate { (name, value) ->
            name to checkNotNull((if (legacy) value["S"] else value).textValue())
        }
    }

    fun encodeTable(table: Table): ByteArray {
        val record = mapOf(
            "name" to table.name, "id" to table.id, "partitionKey" to table.partitionKey, "createdAt" to table.createdAt,
            "throughput" to table.throughput?.let { mapOf("read" to it.read, "write" to it.write) },
        )
        return byteArrayOf(VERSION) + mapper.writeValueAsBytes(record)
    }

    fun table(bytes: ByteArray): Table {
        val (legacy, node) = decode(bytes)
        return if (legacy) {
            Table(node["TableName"].textValue(), node["TableId"].textValue(),
                node["KeySchema"][0]["AttributeName"].textValue(),
                node["CreationDateTime"].longValue(), node["ProvisionedThroughput"]?.let {
                    Throughput(it["ReadCapacityUnits"].longValue(), it["WriteCapacityUnits"].longValue())
                })
        } else {
            Table(node["name"].textValue(), node["id"].textValue(), node["partitionKey"].textValue(),
                node["createdAt"].longValue(), node["throughput"].takeUnless(JsonNode::isNull)?.let {
                    Throughput(it["read"].longValue(), it["write"].longValue())
                })
        }
    }

    private fun decode(bytes: ByteArray): Pair<Boolean, JsonNode> = when (bytes.first()) {
        '{'.code.toByte() -> true to mapper.readTree(bytes)
        VERSION -> false to mapper.readTree(bytes.inputStream(1, bytes.size - 1))
        else -> error("Unknown record version")
    }
}
