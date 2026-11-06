package tikronos.storage

import com.fasterxml.jackson.databind.JsonNode
import tikronos.json
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.Throughput

/** A leading version byte distinguishes compact records from the original JSON documents. */
internal object Records {
    private const val VERSION: Byte = 1

    fun encodeItem(item: Item): ByteArray = byteArrayOf(VERSION) + json.writeValueAsBytes(item)

    fun item(bytes: ByteArray): Item {
        val (legacy, record) = decode(bytes)
        return record.properties().associate { (name, value) ->
            name to checkNotNull((if (legacy) value["S"] else value).textValue())
        }
    }

    fun encodeTable(table: Table): ByteArray = byteArrayOf(VERSION) + json.writeValueAsBytes(table)

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
        '{'.code.toByte() -> true to json.readTree(bytes)
        VERSION -> false to json.readTree(bytes.inputStream(1, bytes.size - 1))
        else -> error("Unknown record version")
    }
}
