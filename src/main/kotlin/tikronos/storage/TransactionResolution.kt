package tikronos.storage

import java.util.concurrent.TimeUnit
import org.tikv.common.TiSession
import org.tikv.common.exception.RegionException
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import org.tikv.shade.com.google.protobuf.ByteString

/** Explicit keys keep cleanup scoped when a transaction spans TiKV regions. */
internal class TransactionResolution(private val session: TiSession) {
    fun resolve(key: ByteString, start: Long, committed: Long) {
        resolve(listOf(key), start, committed)
    }

    fun resolve(keys: List<ByteString>, start: Long, committed: Long) {
        RegionBatches(session).execute(keys) { region, batch, _ ->
            val store = session.regionManager.getStoreById(region.leader.storeId)
            val channel = session.channelFactory.getChannel(store.address, session.pdClient.hostMapping)
            val request = Kvrpcpb.ResolveLockRequest.newBuilder()
                .setContext(region.getReplicaContext(region.leader, emptySet()))
                .setStartVersion(start)
                .setCommitVersion(committed)
                .addAllKeys(batch.map(session.pdClient.codec::encodeKey))
                .build()
            val response = TikvGrpc.newBlockingStub(channel)
                .withDeadlineAfter(BUDGET_MS.toLong(), TimeUnit.MILLISECONDS).kvResolveLock(request)
            if (response.hasRegionError()) {
                throw RegionException(response.regionError)
            } else {
                check(!response.hasError()) { response.error.toString() }
            }
        }
    }

    companion object { private const val BUDGET_MS = 3000 }
}
