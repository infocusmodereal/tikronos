package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.util.UUID
import org.tikv.common.Snapshot
import tikronos.model.Table
import tikronos.storage.BackfillRecords
import tikronos.storage.StreamRecords
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.table
import tikronos.storage.TtlRecords
import tikronos.storage.Transaction

internal class Tables(private val db: Storage) {
    private val streams = Streams(db)
    fun find(name: String, snapshot: Snapshot = db.snapshot()): Table =
        db.catalog.table(snapshot, name) ?: missing(name)

    fun find(name: String, tx: Transaction): Table =
        tx.table(name) ?: missing(name)

    fun describe(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        return obj("Table" to describe(tableName(req), db.snapshot()))
    }

    fun describe(name: String, snapshot: Snapshot): ObjectNode {
        val table = find(name, snapshot)
        return description(table, streams.current(table, snapshot))
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
        val stream = db.atomic { tx ->
            if (tx.read(Storage.CATALOG + name) != null)
                throw ApiError("ResourceInUseException", "Table $name already exists")
            tx.write(Storage.CATALOG + name, Records.encodeTable(table))
            if (req.has("StreamSpecification")) streams.configure(tx, table, req.obj("StreamSpecification")) else null
        }
        return obj("TableDescription" to description(table, stream))
    }

    fun delete(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        val name = tableName(req)
        val (table, stream) = db.atomic { tx ->
            val current = tx.table(name) ?: missing(name)
            val closed = streams.disable(tx, current)
            RetiredRows.enqueue(tx, current)
            current.indexes.forEach { RetiredRows.retireIndex(tx, current, it) }
            if (closed != null) RetiredRows.retireConfig(tx, StreamRecords.config(current.id))
            tx.read(TtlRecords.config(current.id))?.let {
                tx.write(BackfillRecords.ttl(TtlRecords.state(it)), null)
                RetiredRows.retireConfig(tx, TtlRecords.config(current.id))
            }
            tx.write(Storage.CATALOG + name, null)
            current to closed
        }
        return obj("TableDescription" to description(table, stream))
    }

    fun update(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "AttributeDefinitions", "GlobalSecondaryIndexUpdates",
            "BillingMode", "ProvisionedThroughput", "StreamSpecification")
        val name = tableName(req)
        if (req.size() == 1) invalid("UpdateTable requires an update")
        val (table, stream) = db.atomic { tx ->
            val old = tx.table(name) ?: missing(name)
            val changed = updateCatalog(req, old)
            old.indexes.filter { previous -> changed.indexes.none { it.id == previous.id } }
                .forEach { RetiredRows.retireIndex(tx, old, it) }
            val state = if (req.has("StreamSpecification"))
                streams.configure(tx, changed, req.obj("StreamSpecification")) else streams.current(tx, changed)
            tx.write(Storage.CATALOG + name, Records.encodeTable(changed))
            changed to state
        }
        return obj("TableDescription" to description(table, stream))
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
