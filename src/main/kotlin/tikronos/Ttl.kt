package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.util.UUID
import tikronos.model.ExpiryCandidate
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.TtlState
import tikronos.model.attributeName
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.TtlRecords
import tikronos.storage.address
import tikronos.storage.key

internal class Ttl(private val db: Storage) {
    fun update(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName", "TimeToLiveSpecification")
        val name = Tables.tableName(req)
        val spec = req.obj("TimeToLiveSpecification")
        spec.fieldsOnly("AttributeName", "Enabled")
        val attribute = spec.string("AttributeName")
        attributeName(attribute)
        if (attribute.encodeToByteArray().size > MAX_ATTRIBUTE_BYTES) invalid("TTL attribute name too long")
        if (!spec.path("Enabled").isBoolean) invalid("Expected boolean Enabled")
        val enabled = spec.flag("Enabled")
        db.atomic { tx ->
            val table = tx.read(Storage.CATALOG + name)?.let(Records::table)
                ?: throw ApiError("ResourceNotFoundException", "Table does not exist")
            val old = tx.read(TtlRecords.config(table.id))?.let(TtlRecords::state)
            if (old?.enabled == true && enabled) invalid("TTL is already enabled")
            if (!enabled && (old?.enabled != true || old.attribute != attribute))
                invalid("TTL is not enabled for this attribute")
            val state = TtlState(name, table.id, attribute, enabled, UUID.randomUUID().toString())
            // Semantic TTL changes drain item writers' catalog leases; cursor progress does not.
            tx.write(Storage.CATALOG + name, Records.encodeTable(table))
            tx.write(TtlRecords.config(table.id), TtlRecords.encode(state))
        }
        return obj("TimeToLiveSpecification" to spec)
    }

    fun describe(req: ObjectNode): ObjectNode {
        req.fieldsOnly("TableName")
        val snap = db.snapshot()
        val table = db.read(snap, Storage.CATALOG + Tables.tableName(req))?.let(Records::table)
            ?: throw ApiError("ResourceNotFoundException", "Table does not exist")
        val state = db.read(snap, TtlRecords.config(table.id))?.let(TtlRecords::state)
        return obj("TimeToLiveDescription" to obj("TimeToLiveStatus" to
            if (state?.enabled == true) "ENABLED" else "DISABLED").apply {
                if (state != null) put("AttributeName", state.attribute)
            })
    }

    fun track(tx: Transaction, table: Table, old: Item?, new: Item?) {
        val state = tx.read(TtlRecords.config(table.id))?.let(TtlRecords::state)
            ?.takeIf { it.enabled } ?: return
        old?.let { candidate(state, table, it) }?.let { candidate ->
            tx.write(TtlRecords.address(candidate, table.address(candidate.key)), null)
        }
        new?.let { candidate(state, table, it) }?.let { candidate ->
            tx.write(TtlRecords.address(candidate, table.address(candidate.key)), TtlRecords.encode(candidate))
        }
    }


    internal fun candidate(state: TtlState, table: Table, item: Item): ExpiryCandidate? {
        val expiry = expiration(item, state.attribute, Instant.now().epochSecond) ?: return null
        return ExpiryCandidate(table.name, table.id, state.generation, expiry, table.key(item, false))
    }

    companion object {
        private const val MAX_ATTRIBUTE_BYTES = 255
        private const val FIVE_YEARS = 5 * 365 * 24 * 60 * 60L

        internal fun expiration(item: Item, attribute: String, now: Long): Long? {
            val value = item[attribute]?.takeIf { it.type == "N" } ?: return null
            val number = value.decimal()
            val seconds = try {
                number.longValueExact()
            } catch (_: ArithmeticException) {
                null
            }
            return seconds?.takeIf { it >= maxOf(0, now - FIVE_YEARS) }
        }
    }
}
