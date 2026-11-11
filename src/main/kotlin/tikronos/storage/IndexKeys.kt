package tikronos.storage

import tikronos.ApiError
import tikronos.model.Index
import tikronos.model.Item
import tikronos.model.Table

internal fun Index.schema(table: Table): Table = table.copy(
    id = "index/${table.id}/$id", partitionKey = partitionKey, partitionType = partitionType,
    sortKey = sortKey, sortType = sortType, hashed = true, indexes = emptyList(),
)

internal fun Index.entry(table: Table, item: Item): String? {
    listOfNotNull(partitionKey to partitionType, sortKey?.let { it to sortType }).forEach { (name, type) ->
        item[name]?.let { validateKeyValue(it, type, name == partitionKey) }
    }
    if (item[partitionKey] == null || sortKey?.let { item[it] == null } == true) return null
    val key = table.address(table.key(item, false))
    return schema(table).address(item) + INDEX_KEY_END + keyDigest(key)
}

internal fun Index.projected(table: Table): Set<String>? =
    if (projection == "ALL") null else
        (listOfNotNull(table.partitionKey, table.sortKey, partitionKey, sortKey) + attributes).toSet()

/** Project logical attributes even when reading an older, full-copy index entry. */
internal fun Item.projectedTo(attributes: Set<String>?): Item =
    if (attributes == null) this else filterKeys { it in attributes }

// Existing rows may predate an index definition and contain an invalid index key.
internal fun Index.existingEntry(table: Table, item: Item): String? = try {
    entry(table, item)
} catch (_: ApiError) {
    null
}

internal const val INDEX_KEY_END = "!"
internal const val INDEX_KEY_AFTER = "\""
