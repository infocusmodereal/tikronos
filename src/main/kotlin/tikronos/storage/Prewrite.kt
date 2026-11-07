package tikronos.storage

import java.util.concurrent.TimeUnit
import org.tikv.common.TiSession
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.RegionException
import org.tikv.common.region.TiRegion
import org.tikv.common.util.BackOffFunction.BackOffFuncType
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.txn.Lock
import org.tikv.shade.io.grpc.Status
import org.tikv.shade.io.grpc.StatusRuntimeException

/** TiKV 8.x prewrite, without the SDK's per-client server-version probe or fixed transaction size. */
internal class Prewrite(private val session: TiSession) {
    @Suppress("LongParameterList", "ThrowsCount") // Protocol fields and distinct region, key and lock failures.
    fun send(
        region: TiRegion,
        budget: ConcreteBackOffer,
        primary: ByteString,
        mutations: List<Kvrpcpb.Mutation>,
        start: Long,
        ttl: Long,
        blind: Boolean,
        transactionSize: Int,
    ) {
        val codec = session.pdClient.codec
        val store = session.regionManager.getStoreById(region.leader.storeId)
        val channel = session.channelFactory.getChannel(store.address, session.pdClient.hostMapping)
        val request = Kvrpcpb.PrewriteRequest.newBuilder()
            .setContext(region.getReplicaContext(region.leader, emptySet()))
            .setStartVersion(start)
            .setPrimaryLock(codec.encodeKey(primary))
            .addAllMutations(codec.encodeMutations(mutations))
            .setLockTtl(ttl)
            .setSkipConstraintCheck(blind)
            .setMinCommitTs(start)
            .setTxnSize(transactionSize.toLong())
            .build()
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RPC_MS)
        var locked: Kvrpcpb.KeyError? = null
        while (true) {
            val remaining = deadline - System.nanoTime()
            if (remaining <= 0) {
                locked?.let { throw KeyException(it) }
                error("Prewrite deadline exceeded")
            }
            // A transport failure is deliberately propagated: AtomicCommit aborts before retrying.
            val response = try {
                TikvGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS).kvPrewrite(request)
            } catch (error: StatusRuntimeException) {
                if (locked == null || error.status.code != Status.Code.DEADLINE_EXCEEDED) throw error
                // AtomicCommit aborts all prepared keys before retrying this known lock wait.
                throw KeyException(locked).apply { addSuppressed(error) }
            }
            if (response.hasRegionError()) throw RegionException(response.regionError)
            if (response.errorsCount == 0) return
            val locks = response.errorsList.map {
                if (!it.hasLocked()) throw KeyException(it)
                Lock(it.locked, codec)
            }
            locked = response.errorsList.first()
            val resolution = session.regionStoreClientBuilder.build(region).use { client ->
                client.lockResolverClient.resolveLocks(budget, start, locks, true)
            }
            if (resolution.msBeforeTxnExpired > 0) {
                budget.doBackOffWithMaxSleep(
                    BackOffFuncType.BoTxnLock, resolution.msBeforeTxnExpired,
                    KeyException(response.errorsList.first()),
                )
            }
        }
    }

    companion object { private const val RPC_MS = 3000L }
}
