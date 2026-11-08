package tikronos.storage

import tikronos.diagnostics.RequestEvents
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
import tikronos.model.OnePhaseBounds

/** TiKV 8.x prewrite, without the SDK's per-client server-version probe or fixed transaction size. */
internal class Prewrite(
    private val session: TiSession,
    private val exchange: (TikvGrpc.TikvBlockingStub, Kvrpcpb.PrewriteRequest) -> Kvrpcpb.PrewriteResponse =
        { stub, request -> stub.kvPrewrite(request) },
) {
    @Suppress("LongParameterList", "ThrowsCount") // Native protocol fields and distinct region/key/lock errors.
    fun send(
        region: TiRegion,
        budget: ConcreteBackOffer,
        primary: ByteString,
        mutations: List<Kvrpcpb.Mutation>,
        start: Long,
        ttl: Long,
        blind: Boolean,
        transactionSize: Int,
        onePhase: OnePhaseBounds? = null,
    ): Long {
        if (onePhase != null) validateOnePhase(primary, mutations, blind, transactionSize)
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
            .setMinCommitTs(onePhase?.minimum ?: start)
            .setTryOnePc(onePhase != null)
            .setMaxCommitTs(onePhase?.maximum ?: 0)
            .setTxnSize(transactionSize.toLong())
            .build()
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RPC_MS)
        var locked: Kvrpcpb.KeyError? = null
        while (true) {
            val remaining = deadline - System.nanoTime()
            val minimum = if (onePhase != null && locked != null)
                TimeUnit.MILLISECONDS.toNanos(MIN_RETRY_MS) else 0
            if (remaining <= minimum) {
                locked?.let { throw KeyException(it) }
                error("Prewrite deadline exceeded")
            }
            val response = sendRequest(TikvGrpc.newBlockingStub(channel)
                .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS), request, locked)
            if (response.hasRegionError()) throw RegionException(response.regionError)
            if (response.errorsCount == 0) return committed(response.onePcCommitTs, onePhase)
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

    private fun validateOnePhase(
        primary: ByteString, mutations: List<Kvrpcpb.Mutation>, blind: Boolean, size: Int,
    ) {
        require(!blind && size == 1 && mutations.size == 1)
        require(mutations.single().key == primary)
        val operation = mutations.single().op
        require(operation == Kvrpcpb.Op.Put || operation == Kvrpcpb.Op.Del)
    }

    @Suppress("TooGenericExceptionCaught", "ThrowsCount") // Any failed 1PC exchange may have committed.
    private fun sendRequest(
        stub: TikvGrpc.TikvBlockingStub, request: Kvrpcpb.PrewriteRequest, locked: Kvrpcpb.KeyError?,
    ): Kvrpcpb.PrewriteResponse = try {
        if (request.tryOnePc) RequestEvents.onePhaseAttempt()
        val started = RequestEvents.start()
        try { exchange(stub, request) } finally { RequestEvents.prewrite(started) }
    } catch (error: StatusRuntimeException) {
        // A preceding lock error says nothing about the outcome of this send.
        if (request.tryOnePc) throw CommitUnknown(error)
        if (locked == null || error.status.code != Status.Code.DEADLINE_EXCEEDED) throw error
        throw KeyException(locked).apply { addSuppressed(error) }
    } catch (error: Exception) {
        if (request.tryOnePc) throw CommitUnknown(error)
        throw error
    }

    private fun committed(timestamp: Long, bounds: OnePhaseBounds?): Long {
        if (bounds != null) RequestEvents.onePhaseResult(timestamp != 0L)
        if (timestamp == 0L) return 0
        if (bounds == null || timestamp !in bounds.minimum..bounds.maximum) {
            throw CommitUnknown(IllegalStateException("Unexpected 1PC commit timestamp"))
        }
        return timestamp
    }

    companion object {
        private const val RPC_MS = 3000L
        private const val MIN_RETRY_MS = 1000L
    }
}
