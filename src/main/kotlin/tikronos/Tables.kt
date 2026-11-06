package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.util.UUID
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.Storage

internal class Tables(private val db: Storage) {
    private val streams = Streams(db)
    fun find(name: String): Table =
        db.read(db.snapshot(), Storage.CATALOG + name)?.let(Records::table) ?: missing(name)

    fun describe(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        return obj("Table" to describe(find(tableName(req))))
    }

    fun create(req: ObjectNode): ObjectNode {
        req.fieldsOnly(
            "TableName",
            "AttributeDefinitions",
            "KeySchema",
            "BillingMode",
            "ProvisionedThroughput",
            "GlobalSecondaryIndexes",
            "LocalSecondaryIndexes",
            "StreamSpecification",
            "Tags",
        )
        val name = tableName(req)
        val schema = keySchema(req)
        val base =
            Table(
                name,
                UUID.randomUUID().toString(),
                schema.first().first,
                Instant.now().epochSecond,
                throughput(req),
                schema.first().second,
                schema.getOrNull(1)?.first,
                schema.getOrNull(1)?.second,
                hashed = true,
            )
        val table = base.copy(indexes = indexes(req, base), tags = parseTags(req))
        db.atomic { tx ->
            if (tx.read(Storage.CATALOG + name) != null)
                throw ApiError("ResourceInUseException", "Table $name already exists")
            tx.write(Storage.CATALOG + name, Records.encodeTable(table))
            if (req.has("StreamSpecification")) streams.configure(tx, table, req.obj("StreamSpecification"))
        }
        return obj("TableDescription" to describe(table))
    }

    fun delete(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        val name = tableName(req)
        val table = db.atomic { tx ->
            val table = tx.read(Storage.CATALOG + name)?.let(Records::table) ?: missing(name)
            streams.disable(tx, table)
            tx.write(Storage.CATALOG + name, null)
            table
        }
        return obj("TableDescription" to describe(table))
    }

    fun update(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "AttributeDefinitions", "GlobalSecondaryIndexUpdates",
            "BillingMode", "ProvisionedThroughput", "StreamSpecification")
        val name = tableName(req)
        if (req.size() == 1) invalid("UpdateTable requires an update")
        val table = db.atomic { tx ->
            val old = tx.read(Storage.CATALOG + name)?.let(Records::table) ?: missing(name)
            val changed = updateCatalog(req, old)
            if (req.has("StreamSpecification"))
                streams.configure(tx, changed, req.obj("StreamSpecification"))
            tx.write(Storage.CATALOG + name, Records.encodeTable(changed))
            changed
        }
        return obj("TableDescription" to describe(table))
    }

    private fun describe(table: Table) = streams.decorate(table, description(table))

    fun list(req: ObjectNode): ObjectNode {
        req.fieldsOnly("Limit", "ExclusiveStartTableName")
        val limit =
            req.get("Limit")?.let {
                if (
                    !it.isIntegralNumber || !it.canConvertToInt() || it.intValue() !in 1..MAX_TABLES
                ) {
                    invalid("Invalid Limit")
                }
                it.intValue()
            } ?: MAX_TABLES
        val after =
            if (req.has("ExclusiveStartTableName"))
                checkedName(req.string("ExclusiveStartTableName"))
            else ""
        val names = db.tables(after, limit + 1)
        return obj("TableNames" to names.take(limit)).apply {
            if (names.size > limit) put("LastEvaluatedTableName", names[limit - 1])
        }
    }

    private fun missing(name: String): Nothing =
        throw ApiError("ResourceNotFoundException", "Table $name does not exist")

    companion object {
        private const val MAX_TABLES = 100
        private val validName = Regex("[a-zA-Z0-9_.-]{3,255}")

        private fun checkedName(name: String) =
            name.takeIf(validName::matches) ?: invalid("Invalid table name")

        fun tableName(req: ObjectNode) = checkedName(req.string("TableName"))
    }
}
