package tikronos

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.StreamState
import tikronos.model.Table
import tikronos.model.Throughput
import tikronos.model.attributeName

internal fun keySchema(
    req: ObjectNode,
    definitions: JsonNode = req.path("AttributeDefinitions"),
): List<Pair<String, String>> {
    val keys = req.path("KeySchema")
    val defs = definitions
    if (!keys.isArray || keys.size() !in 1..2) invalid("Invalid key schema")
    if (!defs.isArray || defs.isEmpty) invalid("Invalid attribute definitions")
    val types =
        defs.associate {
            it.fieldsOnly("AttributeName", "AttributeType")
            val type = it.string("AttributeType")
            if (type !in setOf("S", "N", "B")) invalid("Invalid key type")
            it.string("AttributeName") to type
        }
    if (types.size != defs.size()) invalid("Duplicate attribute definition")
    val schema =
        keys.map {
            it.fieldsOnly("AttributeName", "KeyType")
            val role = it.string("KeyType")
            if (role !in setOf("HASH", "RANGE")) invalid("Invalid key role")
            role to it.string("AttributeName")
        }
    val sorted = schema.sortedBy { it.first }
    if (
        sorted.first().first != "HASH" ||
            sorted.map { it.first }.distinct().size != keys.size() ||
            sorted.map { it.second }.distinct().size != keys.size()
    )
        invalid("Duplicate or missing key")
    return sorted.map { (_, name) ->
        attributeName(name)
        if (name.encodeToByteArray().size > MAX_KEY_NAME_BYTES) invalid("Key name too long")
        name to (types[name] ?: invalid("Missing attribute definition"))
    }
}

internal fun throughput(req: ObjectNode): Throughput? {
    val mode = if (req.has("BillingMode")) req.string("BillingMode") else "PROVISIONED"
    if (mode == "PAY_PER_REQUEST") {
        if (req.has("ProvisionedThroughput")) invalid("Unexpected ProvisionedThroughput")
        return null
    }
    if (mode != "PROVISIONED") invalid("Invalid BillingMode")
    val throughput = req.obj("ProvisionedThroughput")
    throughput.fieldsOnly("ReadCapacityUnits", "WriteCapacityUnits")
    return Throughput(
        throughput.capacity("ReadCapacityUnits"),
        throughput.capacity("WriteCapacityUnits"),
    )
}

private fun JsonNode.capacity(field: String): Long =
    path(field)
        .takeIf { it.isIntegralNumber && it.canConvertToLong() && it.longValue() > 0 }
        ?.longValue() ?: invalid("Expected positive $field")

internal fun description(table: Table, stream: StreamState?) =
    obj(
            "TableName" to table.name,
            "TableArn" to tableArn(table),
            "TableStatus" to "ACTIVE",
            "TableId" to table.id,
            "CreationDateTime" to table.createdAt,
            "KeySchema" to
                listOfNotNull(
                    obj("AttributeName" to table.partitionKey, "KeyType" to "HASH"),
                    table.sortKey?.let { obj("AttributeName" to it, "KeyType" to "RANGE") },
                ),
            "AttributeDefinitions" to
                definitions(table).map { (name, type) ->
                    obj("AttributeName" to name, "AttributeType" to type)
                },
        )
        .apply {
            set<ObjectNode>("StreamSpecification", obj("StreamEnabled" to (stream?.enabled == true)).apply {
                if (stream != null) put("StreamViewType", stream.view)
            })
            if (stream != null) {
                put("LatestStreamArn", stream.arn)
                put("LatestStreamLabel", stream.label)
            }
            for (local in listOf(false, true)) {
                val indexes = table.indexes.filter { it.local == local }
                if (indexes.isNotEmpty()) set<ObjectNode>(
                    if (local) "LocalSecondaryIndexes" else "GlobalSecondaryIndexes",
                    json.valueToTree(indexes.map { indexDescription(table, it) }),
                )
            }
            table.throughput?.let {
                set<ObjectNode>(
                    "ProvisionedThroughput",
                    obj("ReadCapacityUnits" to it.read, "WriteCapacityUnits" to it.write),
                )
            }
        }

private const val MAX_KEY_NAME_BYTES = 255

internal fun tableArn(table: Table): String =
    "arn:aws:dynamodb:${System.getenv("TIKRONOS_REGION") ?: "us-east-1"}:000000000000:table/${table.name}"

internal fun definitions(table: Table): Map<String, String> = buildMap {
    put(table.partitionKey, table.partitionType)
    table.sortKey?.let { put(it, requireNotNull(table.sortType)) }
    table.indexes.forEach {
        put(it.partitionKey, it.partitionType)
        it.sortKey?.let { name -> put(name, requireNotNull(it.sortType)) }
    }
}
