package tikronos.storage

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
