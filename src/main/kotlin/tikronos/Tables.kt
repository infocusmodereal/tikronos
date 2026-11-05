package tikronos

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.util.UUID

internal class Tables(private val db: Storage) {
    fun describe(name: String): ObjectNode = db.read(db.snapshot(), Storage.CATALOG + name) ?: missing(name)

    fun create(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "AttributeDefinitions", "KeySchema", "BillingMode", "ProvisionedThroughput")
        val name = tableName(req)
        val key = req.singleEntry("KeySchema")
        val definition = req.singleEntry("AttributeDefinitions")
        val field = key.string("AttributeName")
        if (field.isBlank() || key.string("KeyType") != "HASH") invalid("Expected one HASH key")
        if (definition.string("AttributeName") != field ||
            definition.string("AttributeType") != "S") invalid("Expected one string HASH key")
        val table = obj("TableName" to name, "TableStatus" to "ACTIVE", "TableId" to UUID.randomUUID().toString(),
            "KeySchema" to req["KeySchema"], "AttributeDefinitions" to req["AttributeDefinitions"],
            "CreationDateTime" to Instant.now().epochSecond)
        when (req.path("BillingMode").asText("PROVISIONED")) {
            "PAY_PER_REQUEST" -> if (req.has("ProvisionedThroughput")) invalid("Unexpected ProvisionedThroughput")
            "PROVISIONED" -> {
                val throughput = req.obj("ProvisionedThroughput")
                for (capacity in listOf("ReadCapacityUnits", "WriteCapacityUnits")) {
                    val units = throughput.path(capacity)
                    if (!units.isIntegralNumber || !units.canConvertToLong() || units.longValue() < 1) {
                        invalid("Expected positive $capacity")
                    }
                }
                table.set<ObjectNode>("ProvisionedThroughput", throughput)
            }
            else -> invalid("Invalid BillingMode")
        }
        db.mutate(Storage.CATALOG + name) { old ->
            if (old != null) throw ApiError("ResourceInUseException", "Table $name already exists")
            table
        }
        return obj("TableDescription" to table)
    }

    fun delete(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        val name = tableName(req)
        val old = db.mutate(Storage.CATALOG + name) { old ->
            if (old == null) missing(name)
            null
        }
        return obj("TableDescription" to checkNotNull(old))
    }

    fun list(req: ObjectNode): ObjectNode {
        req.fieldsOnly("Limit", "ExclusiveStartTableName")
        val limit = req.get("Limit")?.let {
            if (!it.isIntegralNumber || !it.canConvertToInt() || it.intValue() !in 1..MAX_TABLES) {
                invalid("Invalid Limit")
            }
            it.intValue()
        } ?: MAX_TABLES
        val after = if (req.has("ExclusiveStartTableName")) checkedName(req.string("ExclusiveStartTableName")) else ""
        val names = db.tables(after, limit + 1)
        return obj("TableNames" to names.take(limit)).apply {
            if (names.size > limit) put("LastEvaluatedTableName", names[limit - 1])
        }
    }

    private fun JsonNode.singleEntry(field: String): JsonNode =
        path(field).takeIf { it.isArray && it.size() == 1 }?.first() ?: invalid("Expected one entry in $field")

    private fun missing(name: String): Nothing =
        throw ApiError("ResourceNotFoundException", "Table $name does not exist")

    companion object {
        private const val MAX_TABLES = 100
        private val validName = Regex("[a-zA-Z0-9_.-]{3,255}")
        private fun checkedName(name: String) = name.takeIf(validName::matches) ?: invalid("Invalid table name")
        fun tableName(req: ObjectNode) = checkedName(req.string("TableName"))
    }
}
