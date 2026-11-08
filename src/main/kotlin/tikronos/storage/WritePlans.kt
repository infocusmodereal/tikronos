package tikronos.storage

import tikronos.model.Table
import tikronos.model.TableWritePlan
import tikronos.model.policy

/** Load all semantic dependencies from this transaction's snapshot, including their absence. */
internal fun Transaction.loadWritePlan(table: Table): TableWritePlan {
    val ttlKey = TtlRecords.config(table.id)
    val streamKey = StreamRecords.config(table.id)
    prefetch(listOf(ttlKey, streamKey))
    coveredBy(Storage.CATALOG + table.name, listOf(ttlKey, streamKey))
    val ttl = read(ttlKey)?.let(TtlRecords::state)?.policy
    val arn = read(streamKey)?.decodeToString()
    val stream = arn?.let {
        val descriptor = StreamRecords.descriptor(it)
        coveredBy(Storage.CATALOG + table.name, listOf(descriptor))
        read(descriptor)?.let(StreamRecords::decode)
    }
    return TableWritePlan(table, ttl, stream?.takeIf { it.enabled }?.arn)
}
