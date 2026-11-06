package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.Storage

internal class Catalog(private val db: Storage) {
    private val tables = Tables(db)
    fun backups(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        tables.find(Tables.tableName(req))
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
        val arn = req.string("ResourceArn")
        val name = arn.substringAfter(":table/", "")
        val table = tables.find(name)
        if (arn != tableArn(table)) invalid("Invalid ResourceArn")
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
            tx.write(Storage.CATALOG + name, Records.encodeTable(current.copy(tags = changed)))
        }
    }
}

private fun tagKeys(req: ObjectNode): Set<String> {
    val keys = req.path("TagKeys")
    if (!keys.isArray || keys.isEmpty || keys.size() > MAX_TAGS) invalid("Invalid TagKeys")
    return keys.map { if (!it.isTextual) invalid("Invalid tag key") else it.textValue() }.toSet()
}

internal fun parseTags(req: ObjectNode): Map<String, String> {
    val nodes = req.get("Tags") ?: return emptyMap()
    if (!nodes.isArray || nodes.size() > MAX_TAGS) invalid("Invalid Tags")
    val result = nodes.associate {
        it.fieldsOnly("Key", "Value")
        val key = it.string("Key")
        val value = it.string("Value")
        if (key.length !in 1..MAX_TAG_KEY || value.length > MAX_TAG_VALUE || key.startsWith("aws:"))
            invalid("Invalid tag")
        key to value
    }
    if (result.size != nodes.size()) invalid("Duplicate tag key")
    return result
}

private const val MAX_TAGS = 50
private const val MAX_TAG_KEY = 128
private const val MAX_TAG_VALUE = 256
