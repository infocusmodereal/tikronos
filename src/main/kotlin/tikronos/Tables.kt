package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.util.UUID
import tikronos.model.Table
import tikronos.storage.Records
import tikronos.storage.Storage

internal class Tables(private val db: Storage) {
    fun find(name: String): Table =
        db.read(db.snapshot(), Storage.CATALOG + name)?.let(Records::table) ?: missing(name)

    fun describe(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        return obj("Table" to description(find(tableName(req))))
    }

    fun create(req: ObjectNode): ObjectNode {
        req.fieldsOnly(
            "TableName",
            "AttributeDefinitions",
            "KeySchema",
            "BillingMode",
            "ProvisionedThroughput",
        )
        val name = tableName(req)
        val schema = keySchema(req)
        val table =
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
        db.mutate(Storage.CATALOG + name) { old ->
            if (old != null) throw ApiError("ResourceInUseException", "Table $name already exists")
            Records.encodeTable(table)
        }
        return obj("TableDescription" to description(table))
    }

    fun delete(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        val name = tableName(req)
        val old =
            db.mutate(Storage.CATALOG + name) { old ->
                if (old == null) missing(name)
                null
            }
        return obj("TableDescription" to description(Records.table(checkNotNull(old))))
    }

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
