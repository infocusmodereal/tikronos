package tikronos.storage

import tikronos.model.Item
import tikronos.model.Table
import org.tikv.common.Snapshot
import org.tikv.common.util.BackOffer

/** SDK region batching on this snapshot; failures propagate rather than becoming absent keys. */
internal fun Snapshot.readBatch(
    keys: List<String>, backoffMs: Int = BackOffer.GET_MAX_BACKOFF,
): Map<String, ByteArray> {
    if (keys.isEmpty()) return emptyMap()
    // API reads retain the point-Get budget so abandoned locks can expire and be resolved.
    return batchGet(backoffMs, keys.map(String::encodeToByteArray))
        .filter { it.value.isNotEmpty() }
        .associate { it.key.decodeToString() to it.value }
}

/** Batch unprojected LSI reads on the index scan's snapshot and restore scan order. */
internal fun Snapshot.baseItems(table: Table, entries: Sequence<Item>): Sequence<Item> =
    entries.map(table::address).chunked(BASE_READ_BATCH).flatMap { addresses ->
        val rows = readBatch(addresses)
        addresses.map { address -> Records.itemAt(table, address, checkNotNull(rows[address]) {
            "Local index entry has no base item at the scan snapshot"
        }) }
    }

private const val BASE_READ_BATCH = 8
