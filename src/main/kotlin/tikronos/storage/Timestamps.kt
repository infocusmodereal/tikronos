package tikronos.storage

import org.tikv.common.meta.TiTimestamp
import org.tikv.kvproto.Pdpb

internal fun timestampMillis(version: Long): Long = version shr LOGICAL_BITS
internal fun timestampSeconds(version: Long): Long = timestampMillis(version) / MILLIS_PER_SECOND
internal fun timestampStartOfSecond(second: Long): Long = (second * MILLIS_PER_SECOND) shl LOGICAL_BITS
/** Decode PD's upper timestamp, respecting logical suffix bits and rejecting reused allocations. */
internal fun timestampBatch(
    response: Pdpb.TsoResponse, count: Int, clusterId: Long, previous: Long,
): List<TiTimestamp> {
    check(response.header.clusterId == clusterId) { "PD timestamp cluster mismatch" }
    check(response.count == count) { "PD returned an unexpected timestamp count" }
    val timestamp = response.timestamp
    check(timestamp.suffixBits in 0 until LOGICAL_BITS)
    check(timestamp.physical in 1..(Long.MAX_VALUE ushr LOGICAL_BITS))
    val step = 1L shl timestamp.suffixBits
    val first = timestamp.logical - (count - 1) * step
    check(first >= 0 && timestamp.logical < (1L shl LOGICAL_BITS))
    return List(count) { TiTimestamp(timestamp.physical, first + it * step) }.also {
        check(it.first().version > previous) { "PD timestamps did not advance" }
    }
}


private const val LOGICAL_BITS = 18
private const val MILLIS_PER_SECOND = 1000L
