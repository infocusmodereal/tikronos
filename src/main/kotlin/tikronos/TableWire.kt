package tikronos

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.Table
import tikronos.model.Throughput

internal fun keySchema(req: ObjectNode): List<Pair<String, String>> {
    val keys = req.path("KeySchema")
    val defs = req.path("AttributeDefinitions")
    if (!keys.isArray || keys.size() !in 1..2) invalid("Invalid key schema")
    if (!defs.isArray || defs.size() != keys.size()) invalid("Invalid attribute definitions")
    val types =
        defs.associate {
            it.fieldsOnly("AttributeName", "AttributeType")
            val type = it.string("AttributeType")
            if (type !in setOf("S", "N", "B")) invalid("Invalid key type")
            it.string("AttributeName") to type
        }
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
        if (name.isBlank()) invalid("Empty key name")
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

internal fun description(table: Table) =
    obj(
            "TableName" to table.name,
            "TableStatus" to "ACTIVE",
            "TableId" to table.id,
            "CreationDateTime" to table.createdAt,
            "KeySchema" to
                listOfNotNull(
                    obj("AttributeName" to table.partitionKey, "KeyType" to "HASH"),
                    table.sortKey?.let { obj("AttributeName" to it, "KeyType" to "RANGE") },
                ),
            "AttributeDefinitions" to
                listOfNotNull(
                    obj(
                        "AttributeName" to table.partitionKey,
                        "AttributeType" to table.partitionType,
                    ),
                    table.sortKey?.let {
                        obj(
                            "AttributeName" to it,
                            "AttributeType" to requireNotNull(table.sortType),
                        )
                    },
                ),
        )
        .apply {
            table.throughput?.let {
                set<ObjectNode>(
                    "ProvisionedThroughput",
                    obj("ReadCapacityUnits" to it.read, "WriteCapacityUnits" to it.write),
                )
            }
        }
