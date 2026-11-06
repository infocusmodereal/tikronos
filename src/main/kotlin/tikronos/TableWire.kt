package tikronos

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.Table
import tikronos.model.Throughput

internal fun partitionKey(req: ObjectNode): String {
    val key = req.singleEntry("KeySchema")
    val definition = req.singleEntry("AttributeDefinitions")
    key.fieldsOnly("AttributeName", "KeyType")
    definition.fieldsOnly("AttributeName", "AttributeType")
    val field = key.string("AttributeName")
    if (field.isBlank() || key.string("KeyType") != "HASH") invalid("Expected one HASH key")
    if (definition.string("AttributeName") != field || definition.string("AttributeType") != "S") {
        invalid("Expected one string HASH key")
    }
    return field
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
    return Throughput(throughput.capacity("ReadCapacityUnits"), throughput.capacity("WriteCapacityUnits"))
}

private fun JsonNode.capacity(field: String): Long =
    path(field).takeIf { it.isIntegralNumber && it.canConvertToLong() && it.longValue() > 0 }?.longValue()
        ?: invalid("Expected positive $field")

internal fun description(table: Table) = obj(
    "TableName" to table.name, "TableStatus" to "ACTIVE", "TableId" to table.id,
    "CreationDateTime" to table.createdAt,
    "KeySchema" to listOf(obj("AttributeName" to table.partitionKey, "KeyType" to "HASH")),
    "AttributeDefinitions" to listOf(obj("AttributeName" to table.partitionKey, "AttributeType" to "S")),
).apply {
    table.throughput?.let { set<ObjectNode>("ProvisionedThroughput", obj(
        "ReadCapacityUnits" to it.read, "WriteCapacityUnits" to it.write)) }
}

private fun JsonNode.singleEntry(field: String): JsonNode =
    path(field).takeIf { it.isArray && it.size() == 1 }?.first() ?: invalid("Expected one entry in $field")
