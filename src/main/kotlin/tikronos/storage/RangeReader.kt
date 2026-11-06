package tikronos.storage

import java.util.concurrent.TimeUnit
import org.tikv.common.Snapshot
import org.tikv.common.TiSession
import org.tikv.common.region.TiRegion
import org.tikv.common.util.BackOffFunction.BackOffFuncType
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.shade.io.grpc.StatusRuntimeException
import org.tikv.txn.AbstractLockResolverClient

internal val byteOrder = ByteString.unsignedLexicographicalComparator()

internal fun bytes(value: String): ByteString = ByteString.copyFromUtf8(value)

internal fun after(key: ByteString): ByteString = key.concat(ZERO)

private val ZERO = ByteString.copyFrom(byteArrayOf(0))

/**
 * The client iterator lacks reverse scans and sends unbounded end keys. Use the same MVCC timestamp
 * and SDK region/lock resolution, but give every KvScan a direction, upper bound and small limit.
 */
internal class RangeReader(private val session: TiSession) {
    private val regions
        get() = session.regionManager

    private val codec
        get() = session.pdClient.codec

    fun scan(
        first: ByteString, end: ByteString, forward: Boolean,
        snapshot: Snapshot = session.createSnapshot(),
    ): Sequence<Kvrpcpb.KvPair> =
        sequence {
            val snap = snapshot
            var low = first
            var high = end
            while (byteOrder.compare(low, high) < 0) {
                val page = page(snap, low, high, forward)
                for (pair in page.rows) if (!pair.value.isEmpty) yield(pair)
                if (forward) low = page.next else high = page.next
            }
        }

    private data class Page(val rows: List<Kvrpcpb.KvPair>, val next: ByteString)

    // Region churn, transport retries and MVCC lock resolution each restart the same cursor.
    @Suppress("LoopWithTooManyJumpStatements")
    private fun page(snap: Snapshot, low: ByteString, high: ByteString, forward: Boolean): Page {
        val retry = ConcreteBackOffer.newScannerNextMaxBackOff()
        val resolved = mutableSetOf<Long>()
        while (true) {
            val region = if (forward) regions.getRegionByKey(low) else lastRegion(low, high)
            val from = maxOf(low, region.startKey, byteOrder)
            val to = if (region.endKey.isEmpty) high else minOf(high, region.endKey, byteOrder)
            val response =
                try {
                    fetch(region, snap.version, from to to, forward, resolved)
                } catch (e: StatusRuntimeException) {
                    regions.onRequestFail(region)
                    retry.doBackOff(BackOffFuncType.BoTiKVRPC, e)
                    continue
                }
            if (response.hasRegionError()) {
                regions.invalidateRegion(region)
                retry.doBackOff(
                    BackOffFuncType.BoRegionMiss,
                    IllegalStateException(response.regionError.toString()),
                )
                continue
            }
            if (resolveLocks(response, region, snap.version, retry, resolved)) continue
            val rows = codec.decodeKvPairs(response.pairsList)
            val next =
                if (rows.size < RPC_ROWS) {
                    if (forward) to else from
                } else if (forward) after(rows.last().key) else rows.last().key
            return Page(rows, next)
        }
    }

    private fun resolveLocks(
        response: Kvrpcpb.ScanResponse,
        region: TiRegion,
        version: Long,
        retry: ConcreteBackOffer,
        resolved: MutableSet<Long>,
    ): Boolean {
        val errors =
            response.pairsList.filter { it.hasError() }.map { it.error } +
                listOfNotNull(response.error.takeIf { response.hasError() })
        if (errors.isNotEmpty()) {
            val locks = errors.map { AbstractLockResolverClient.extractLockFromKeyErr(it, codec) }
            val resolution =
                session.regionStoreClientBuilder.build(region).use { client ->
                    client.lockResolverClient.resolveLocks(retry, version, locks, false)
                }
            resolved += resolution.resolvedLocks
            if (resolution.msBeforeTxnExpired > 0) {
                retry.doBackOffWithMaxSleep(
                    BackOffFuncType.BoTxnLockFast,
                    resolution.msBeforeTxnExpired,
                    IllegalStateException("Waiting for a scan lock"),
                )
            }
            return true
        }
        return false
    }

    /** Only consult region metadata when the exclusive upper bound is exactly a split point. */
    private fun lastRegion(low: ByteString, high: ByteString): TiRegion {
        val atEnd = regions.getRegionByKey(high)
        if (byteOrder.compare(atEnd.startKey, high) < 0) return atEnd
        var region = regions.getRegionByKey(low)
        while (!region.endKey.isEmpty && byteOrder.compare(region.endKey, high) < 0) {
            region = regions.getRegionByKey(region.endKey)
        }
        return region
    }

    private fun fetch(
        region: TiRegion,
        version: Long,
        bounds: Pair<ByteString, ByteString>,
        forward: Boolean,
        resolved: Set<Long>,
    ): Kvrpcpb.ScanResponse {
        val store = regions.getStoreById(region.leader.storeId)
        val channel = session.channelFactory.getChannel(store.address, session.pdClient.hostMapping)
        val range = codec.encodeRange(bounds.first, bounds.second)
        val request =
            Kvrpcpb.ScanRequest.newBuilder()
                .setContext(region.getReplicaContext(region.leader, resolved))
                .setVersion(version)
                .setStartKey(if (forward) range.first else range.second)
                .setEndKey(if (forward) range.second else range.first)
                .setReverse(!forward)
                .setLimit(RPC_ROWS)
                .build()
        return TikvGrpc.newBlockingStub(channel)
            .withDeadlineAfter(session.conf.scanTimeout, TimeUnit.MILLISECONDS)
            .kvScan(request)
    }

    companion object {
        private const val RPC_ROWS = 8
    }
}
