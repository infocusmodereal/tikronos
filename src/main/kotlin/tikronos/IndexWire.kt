package tikronos

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.UUID
import tikronos.model.MAX_GSI
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.attributeName

internal fun indexes(req: ObjectNode, table: Table): List<Index> {
    val indexes = listOf(false, true).flatMap { local ->
        val field = if (local) "LocalSecondaryIndexes" else "GlobalSecondaryIndexes"
        val nodes = req.get(field) ?: return@flatMap emptyList()
        if (!nodes.isArray || nodes.isEmpty || nodes.size() > if (local) MAX_LSI else MAX_GSI)
            invalid("Invalid $field")
        nodes.map { index(it, req.path("AttributeDefinitions"), table, local) }
    }
    if (indexes.distinctBy { it.name }.size != indexes.size) invalid("Duplicate index name")
    val defined = req.path("AttributeDefinitions").map { it.string("AttributeName") }.toSet()
    if (defined != definitions(table.copy(indexes = indexes)).keys)
        invalid("AttributeDefinitions must match table and index keys")
    return indexes
}

internal fun index(node: JsonNode, defs: JsonNode, table: Table, local: Boolean): Index {
    node.fieldsOnly("IndexName", "KeySchema", "Projection", "ProvisionedThroughput")
    val name = node.string("IndexName")
    if (!Regex("[a-zA-Z0-9_.-]{3,255}").matches(name)) invalid("Invalid index name")
    val schema = keySchema(node as? ObjectNode ?: invalid("Invalid index"), defs)
    if (local) validateLocal(table, schema)
    val (kind, names) = projection(node)
    val capacity = indexCapacity(node, table, local)
    return Index(name, UUID.randomUUID().toString(), schema[0].first, schema[0].second,
        schema.getOrNull(1)?.first, schema.getOrNull(1)?.second, local, kind, names, capacity)
}

private fun validateLocal(table: Table, schema: List<Pair<String, String>>) {
    if (table.sortKey == null || schema.size != 2) invalid("Local index requires a sort key")
    if (schema.first() != (table.partitionKey to table.partitionType))
        invalid("Local index must use the table partition key")
}

private fun projection(node: JsonNode): Pair<String, List<String>> {
    val projection = node.obj("Projection")
    projection.fieldsOnly("ProjectionType", "NonKeyAttributes")
    val kind = projection.string("ProjectionType")
    if (kind !in setOf("ALL", "KEYS_ONLY", "INCLUDE")) invalid("Invalid projection type")
    val attrs = projection.path("NonKeyAttributes")
    val names = if (attrs.isMissingNode) emptyList() else {
        if (!attrs.isArray || attrs.isEmpty || attrs.size() > MAX_PROJECTION_ATTRIBUTES)
            invalid("Invalid NonKeyAttributes")
        attrs.map {
            if (!it.isTextual) invalid("Expected attribute name")
            it.textValue().also(::attributeName)
        }
    }
    if ((kind == "INCLUDE") != names.isNotEmpty() || names.distinct().size != names.size)
        invalid("Invalid NonKeyAttributes")
    return kind to names
}

private fun indexCapacity(node: JsonNode, table: Table, local: Boolean) =
    if (!local && table.throughput != null) {
        throughput(obj("ProvisionedThroughput" to node.obj("ProvisionedThroughput")))
    } else {
        if (node.has("ProvisionedThroughput")) invalid("Unexpected index throughput")
        null
    }

internal fun indexDescription(table: Table, index: Index): ObjectNode = obj(
    "IndexName" to index.name,
    "IndexArn" to "${tableArn(table)}/index/${index.name}",
    "KeySchema" to listOfNotNull(
        obj("AttributeName" to index.partitionKey, "KeyType" to "HASH"),
        index.sortKey?.let { obj("AttributeName" to it, "KeyType" to "RANGE") },
    ),
    "Projection" to obj("ProjectionType" to index.projection).apply {
        if (index.attributes.isNotEmpty()) set<ObjectNode>(
            "NonKeyAttributes", json.valueToTree(index.attributes))
    },
).apply {
    if (!index.local) {
        put("IndexStatus", index.status)
        if (index.status == "CREATING") put("Backfilling", true)
        index.throughput?.let {
            set<ObjectNode>("ProvisionedThroughput", obj(
                "ReadCapacityUnits" to it.read, "WriteCapacityUnits" to it.write))
        }
    }
}

private const val MAX_LSI = 5
private const val MAX_PROJECTION_ATTRIBUTES = 20
