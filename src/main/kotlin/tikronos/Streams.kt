package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.util.UUID
import tikronos.codec.wireItem
import tikronos.model.Item
import tikronos.model.TableWritePlan
import tikronos.model.StreamState
import tikronos.model.Table
import tikronos.model.itemSize
import tikronos.storage.timestampSeconds
import tikronos.storage.Storage
import tikronos.storage.StreamRecords
import tikronos.storage.Transaction
import tikronos.storage.key

internal class Streams(private val db: Storage) {
    fun configure(tx: Transaction, table: Table, spec: ObjectNode) {
        spec.fieldsOnly("StreamEnabled", "StreamViewType")
        if (!spec.path("StreamEnabled").isBoolean) invalid("Expected boolean StreamEnabled")
        val enabled = spec.flag("StreamEnabled")
        val current = current(tx, table)
        val view = if (spec.has("StreamViewType")) spec.string("StreamViewType") else null
        if (view != null && view !in VIEWS) invalid("Invalid StreamViewType")
        if (enabled && view == null) invalid("StreamViewType required")
        if (enabled && current?.enabled == true) invalid("Stream is already enabled")
        if (!enabled) {
            disable(tx, table)
            return
        }
        val label = Instant.now().toString() + "-" + UUID.randomUUID()
        val arn = tableArn(table) + "/stream/$label"
        val state = StreamState(arn, label, table.name, table.id, requireNotNull(view),
            Instant.now().epochSecond,
            listOfNotNull(table.partitionKey to "HASH", table.sortKey?.let { it to "RANGE" }))
        touchCatalog(tx, table)
        tx.write(StreamRecords.descriptor(arn), StreamRecords.encode(state))
        tx.write(StreamRecords.config(table.id), arn.encodeToByteArray())
    }

    fun disable(tx: Transaction, table: Table) {
        val state = current(tx, table) ?: return
        if (state.enabled) {
            touchCatalog(tx, table)
            tx.write(StreamRecords.descriptor(state.arn),
                StreamRecords.encode(state.copy(enabled = false, closedAt = Instant.now().epochSecond)))
        }
    }

    private fun touchCatalog(tx: Transaction, table: Table) {
        val key = Storage.CATALOG + table.name
        tx.write(key, checkNotNull(tx.read(key)))
    }

    fun decorate(table: Table, description: ObjectNode): ObjectNode {
        val snap = db.snapshot()
        val arn = db.read(snap, StreamRecords.config(table.id))?.decodeToString()
        val state = arn?.let { db.read(snap, StreamRecords.descriptor(it))?.let(StreamRecords::decode) }
        description.set<ObjectNode>("StreamSpecification", obj("StreamEnabled" to (state?.enabled == true)).apply {
            if (state != null) put("StreamViewType", state.view)
        })
        if (state != null) {
            description.put("LatestStreamArn", state.arn)
            description.put("LatestStreamLabel", state.label)
        }
        return description
    }

    fun record(tx: Transaction, plan: TableWritePlan, old: Item?, new: Item?, ttl: Boolean = false) {
        if (old == new) return
        val table = plan.table
        val state = plan.streamArn?.let { current(tx, table, it) }?.takeIf { it.enabled } ?: return
        val seq = tx.version
        check(seq > state.head) { "Stream sequence must follow its committed predecessor" }
        val event = table.event(state, old, new, seq, ttl)
        tx.write(StreamRecords.events(state) + StreamRecords.sequence(seq), json.writeValueAsBytes(event))
        tx.write(StreamRecords.descriptor(state.arn), StreamRecords.encode(state.copy(head = seq)))
    }

    private fun current(tx: Transaction, table: Table): StreamState? {
        val arn = tx.read(StreamRecords.config(table.id))?.decodeToString() ?: return null
        return current(tx, table, arn)
    }

    private fun current(tx: Transaction, table: Table, arn: String): StreamState? {
        // Enable/disable also writes the catalog. Active head updates remain conflict-checked writes.
        tx.coveredBy(Storage.CATALOG + table.name, listOf(StreamRecords.descriptor(arn)))
        return tx.read(StreamRecords.descriptor(arn))?.let(StreamRecords::decode)
    }

    private fun Table.event(state: StreamState, old: Item?, new: Item?, sequence: Long, ttl: Boolean): ObjectNode {
        val keys = key(requireNotNull(new ?: old), false)
        val includeOld = state.view in setOf("OLD_IMAGE", "NEW_AND_OLD_IMAGES")
        val includeNew = state.view in setOf("NEW_IMAGE", "NEW_AND_OLD_IMAGES")
        val size = itemSize(keys) + (if (includeOld) itemSize(old.orEmpty()) else 0) +
            (if (includeNew) itemSize(new.orEmpty()) else 0)
        val data = obj("ApproximateCreationDateTime" to timestampSeconds(sequence),
            "Keys" to wireItem(keys), "SequenceNumber" to StreamRecords.sequence(sequence),
            "StreamViewType" to state.view, "SizeBytes" to size)
        if (old != null && includeOld)
            data.set<ObjectNode>("OldImage", json.valueToTree(wireItem(old)))
        if (new != null && includeNew)
            data.set<ObjectNode>("NewImage", json.valueToTree(wireItem(new)))
        return obj("eventID" to "${state.label}:$sequence", "eventName" to when {
            old == null -> "INSERT"
            new == null -> "REMOVE"
            else -> "MODIFY"
        }, "eventVersion" to "1.1", "eventSource" to "aws:dynamodb",
            "awsRegion" to state.arn.split(':')[REGION_PART], "dynamodb" to data).apply {
                if (ttl) set<ObjectNode>("userIdentity",
                    obj("Type" to "Service", "PrincipalId" to "dynamodb.amazonaws.com"))
            }
    }

    companion object {
        private const val REGION_PART = 3
        private val VIEWS = setOf("KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES")
    }
}
