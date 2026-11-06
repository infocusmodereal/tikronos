package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.Table
import tikronos.model.MAX_GSI

internal fun updateCatalog(req: ObjectNode, table: Table): Table {
    val billing = req.has("BillingMode") || req.has("ProvisionedThroughput")
    val capacity = if (billing) throughput(req) else table.throughput
    var changed = table.copy(throughput = capacity, indexes = table.indexes.map {
        if (capacity == null) it.copy(throughput = null) else it
    })
    for (update in indexUpdates(req)) changed = applyIndexUpdate(update, req, changed)
    if (capacity != null && changed.indexes.any { !it.local && it.throughput == null })
        invalid("ProvisionedThroughput is required for every global index")
    return changed
}

private fun indexUpdates(req: ObjectNode): List<ObjectNode> {
    val updates = req.get("GlobalSecondaryIndexUpdates")
    if (updates == null) {
        if (req.has("AttributeDefinitions")) invalid("AttributeDefinitions requires index creation")
        return emptyList()
    }
    if (!updates.isArray || updates.isEmpty) invalid("Expected index updates")
    if (updates.size() > 1 && updates.any { !it.has("Update") })
        invalid("Only throughput updates may be combined")
    return updates.map { it as? ObjectNode ?: invalid("Expected index update") }
}

private fun applyIndexUpdate(update: ObjectNode, req: ObjectNode, table: Table): Table {
    update.fieldsOnly("Create", "Delete", "Update")
    if (update.size() != 1) invalid("Expected exactly one index operation")
    if (!update.has("Delete") && table.indexes.any { it.status != "ACTIVE" })
        throw ApiError("ResourceInUseException", "An index is still being created")
    return when {
        update.has("Create") -> addIndex(update.obj("Create"), req, table)
        update.has("Delete") -> deleteIndex(update.obj("Delete"), table)
        else -> resizeIndex(update.obj("Update"), table)
    }
}

private fun addIndex(body: ObjectNode, req: ObjectNode, table: Table): Table {
    val added = index(body, req.path("AttributeDefinitions"), table, false)
    if (table.indexes.any { it.name == added.name } || table.indexes.count { !it.local } >= MAX_GSI)
        invalid("Duplicate index or too many indexes")
    val defined = definitions(table)
    listOfNotNull(added.partitionKey to added.partitionType,
        added.sortKey?.let { it to requireNotNull(added.sortType) }).forEach { (name, type) ->
        if (defined[name]?.let { it != type } == true) invalid("Cannot change attribute type")
    }
    return table.copy(indexes = table.indexes + added.copy(status = "CREATING"))
}

private fun deleteIndex(body: ObjectNode, table: Table): Table {
    body.fieldsOnly("IndexName")
    val name = body.string("IndexName")
    val index = table.indexes.firstOrNull { it.name == name && !it.local } ?: invalid("Unknown global index")
    return table.copy(indexes = table.indexes - index)
}

private fun resizeIndex(body: ObjectNode, table: Table): Table {
    body.fieldsOnly("IndexName", "ProvisionedThroughput")
    val index = table.indexes.firstOrNull { it.name == body.string("IndexName") && !it.local }
    if (index == null || table.throughput == null) invalid("Unknown or on-demand index")
    return table.copy(indexes = table.indexes.map {
        if (it.id == index.id) it.copy(throughput = throughput(body)) else it
    })
}
