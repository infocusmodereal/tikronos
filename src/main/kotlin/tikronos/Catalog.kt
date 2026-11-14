package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.table

internal class Catalog(private val db: Storage) {
    private val tables = Tables(db)
    fun backups(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        val resource = req.string("TableName")
        val name = if (resource.startsWith("arn:")) nameFromArn(resource) else Tables.checkedName(resource)
        if (db.catalog.table(db.snapshot(), name) == null)
            throw ApiError("TableNotFoundException", "Table $name does not exist")
        return obj("ContinuousBackupsDescription" to obj(
            "ContinuousBackupsStatus" to "DISABLED",
            "PointInTimeRecoveryDescription" to obj("PointInTimeRecoveryStatus" to "DISABLED")))
    }

    fun tags(req: ObjectNode, operation: String): ObjectNode {
        req.fieldsOnly("ResourceArn", when (operation) {
            "TagResource" -> "Tags"
            "UntagResource" -> "TagKeys"
            else -> "NextToken"
        })
        val table = tables.find(nameFromArn(req.string("ResourceArn")))
        if (operation == "ListTagsOfResource") {
            if (req.has("NextToken")) invalid("Invalid NextToken")
            return obj("Tags" to table.tags.toSortedMap().map { obj("Key" to it.key, "Value" to it.value) })
        }
        if (operation == "TagResource" && !req.has("Tags")) invalid("Tags is required")
        val additions = if (operation == "TagResource") parseTags(req) else emptyMap()
        val removals = if (operation == "UntagResource") tagKeys(req) else emptySet()
        changeTags(table, additions, removals)
        return obj()
    }

    private fun changeTags(table: Table, additions: Map<String, String>, removals: Set<String>) {
        val name = table.name
        db.atomic { tx ->
            val current = tx.read(Storage.CATALOG + name)?.let(Records::table)
            if (current?.id != table.id) throw ApiError("ResourceNotFoundException", "Table no longer exists")
            val changed = (current.tags + additions) - removals
            if (changed.size > MAX_TAGS) invalid("Too many tags")
            if (changed != current.tags)
                tx.write(Storage.CATALOG + name, Records.encodeTable(current.copy(tags = changed)))
        }
    }
}

private fun nameFromArn(arn: String): String {
    val name = Tables.checkedName(arn.substringAfter(":table/", ""))
    if (arn != tableArn(name)) invalid("Invalid table ARN")
    return name
}

private fun tagKey(key: String): String {
    if (key.codePointCount(0, key.length) !in 1..MAX_TAG_KEY || key.startsWith("aws:")) invalid("Invalid tag key")
    return key
}

private fun tagKeys(req: ObjectNode): Set<String> {
    val keys = req.path("TagKeys")
    if (!keys.isArray || keys.isEmpty || keys.size() > MAX_TAGS) invalid("Invalid TagKeys")
    return keys.map { tagKey(it.textValue() ?: invalid("Invalid tag key")) }.toSet()
}

internal fun parseTags(req: ObjectNode): Map<String, String> {
    val nodes = req.get("Tags") ?: return emptyMap()
    if (!nodes.isArray || nodes.size() > MAX_TAGS) invalid("Invalid Tags")
    val result = nodes.associate {
        it.fieldsOnly("Key", "Value")
        val key = tagKey(it.string("Key"))
        val value = it.string("Value")
        if (value.codePointCount(0, value.length) > MAX_TAG_VALUE)
            invalid("Invalid tag")
        key to value
    }
    if (result.size != nodes.size()) invalid("Duplicate tag key")
    return result
}

private const val MAX_TAGS = 50
private const val MAX_TAG_KEY = 128
private const val MAX_TAG_VALUE = 256
