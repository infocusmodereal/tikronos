package tikronos.storage

import java.util.concurrent.TimeUnit
import org.tikv.common.TiSession
import org.tikv.common.util.BackOffFunction.BackOffFuncType
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import org.tikv.shade.com.google.protobuf.ByteString

/** Explicit keys keep cleanup scoped when a transaction spans TiKV regions. */
internal class TransactionResolution(private val session: TiSession) {
    fun resolve(key: ByteString, start: Long, committed: Long) {
        val budget = ConcreteBackOffer.newCustomBackOff(BUDGET_MS)
        while (true) {
            val region = session.regionManager.getRegionByKey(key, budget)
            val store = session.regionManager.getStoreById(region.leader.storeId)
            val channel = session.channelFactory.getChannel(store.address, session.pdClient.hostMapping)
            val request = Kvrpcpb.ResolveLockRequest.newBuilder()
                .setContext(region.getReplicaContext(region.leader, emptySet()))
                .setStartVersion(start)
                .setCommitVersion(committed)
                .addKeys(session.pdClient.codec.encodeKey(key))
                .build()
            val response = TikvGrpc.newBlockingStub(channel)
                .withDeadlineAfter(BUDGET_MS.toLong(), TimeUnit.MILLISECONDS).kvResolveLock(request)
            if (response.hasRegionError()) {
                session.regionManager.invalidateRegion(region)
                budget.doBackOff(BackOffFuncType.BoRegionMiss, IllegalStateException(response.regionError.toString()))
            } else {
                check(!response.hasError()) { response.error.toString() }
                return
            }
        }
    }

    companion object { private const val BUDGET_MS = 3000 }
}
