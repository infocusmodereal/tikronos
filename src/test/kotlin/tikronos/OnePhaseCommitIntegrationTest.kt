package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import org.tikv.kvproto.Errorpb
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import org.tikv.shade.io.grpc.Status
import tikronos.storage.AtomicCommit
import tikronos.storage.CommitUnknown
import tikronos.storage.Prewrite
import tikronos.storage.Storage
import tikronos.storage.TransactionResolution
import tikronos.storage.bytes

@Tag("tikv")
class OnePhaseCommitIntegrationTest {
    @Test
    fun `single item put and delete commit natively and retain conflict checks`() = fixture { session, key ->
        var sends = 0
        var timestamps = 0
        val prewrite = Prewrite(session) { stub, request ->
            assertTrue(request.tryOnePc)
            assertFalse(request.skipConstraintCheck)
            assertTrue(request.minCommitTs > request.startVersion)
            sends++
            stub.kvPrewrite(request).also {
                if (it.errorsCount == 0) assertTrue(it.onePcCommitTs >= request.minCommitTs)
            }
        }
        val commit = AtomicCommit(session, timestamp = { timestamps++; session.timestamp }, prewriter = prewrite)
        val stale = session.timestamp.version
        assertTrue(commit.commit(session.timestamp.version, mapOf(key to byteArrayOf(1))))
        assertEquals(1, timestamps) // No second timestamp or explicit commit after successful 1PC.
        assertFalse(commit.commit(stale, mapOf(key to byteArrayOf(2))))
        assertEquals(listOf<Byte>(1), session.createSnapshot().get(bytes(key)).toByteArray().toList())
        assertTrue(commit.commit(session.timestamp.version, mapOf(key to null)))
        assertTrue(session.createSnapshot().get(bytes(key)).isEmpty)
        assertEquals(3, sends)
    }

    @Test
    fun `explicit zero result falls back to two phase commit`() = fixture { session, key ->
        var timestamps = 0
        val prewrite = Prewrite(session) { stub, request ->
            assertTrue(request.tryOnePc)
            // Exercise a real prepared lock and the driver's explicit fallback decision.
            stub.kvPrewrite(request.toBuilder().setTryOnePc(false).build()).also {
                assertEquals(0, it.errorsCount)
                assertEquals(0L, it.onePcCommitTs)
            }
        }
        assertTrue(AtomicCommit(session, timestamp = { timestamps++; session.timestamp }, prewriter = prewrite)
            .commit(session.timestamp.version, mapOf(key to byteArrayOf(1))))
        assertEquals(2, timestamps)
        assertEquals(listOf<Byte>(1), session.createSnapshot().get(bytes(key)).toByteArray().toList())
    }

    @Test
    fun `reader pushing native minimum past lease forces fallback and refuses commit`() = fixture { session, key ->
        val start = session.timestamp.version
        val minimum = session.timestamp
        val ceiling = minimum.version + 1
        session.createSnapshot().get(bytes(key)) // Advance the store's max read timestamp past the ceiling.
        var timestamps = 0
        var fallback = false
        val prewrite = Prewrite(session) { stub, request ->
            assertEquals(ceiling, request.maxCommitTs)
            stub.kvPrewrite(request).also { fallback = it.errorsCount == 0 && it.onePcCommitTs == 0L }
        }
        assertFalse(AtomicCommit(session, timestamp = {
            if (timestamps++ == 0) minimum else session.timestamp
        }, prewriter = prewrite).commit(start, mapOf(key to byteArrayOf(1)), ceiling = ceiling))
        assertTrue(fallback)
        assertTrue(session.createSnapshot().get(bytes(key)).isEmpty)
        // An already-expired lease must not even send prewrite.
        assertFalse(AtomicCommit(session, prewriter = Prewrite(session) { _, _ -> error("Unexpected send") })
            .commit(session.timestamp.version, mapOf(key to byteArrayOf(1)), ceiling = ceiling))
    }

    @Test
    fun `lost successful response after a lock error stays unknown instead of retryable`() = fixture { session, key ->
        val owner = lockedOwner(session, key)
        val start = session.timestamp.version
        var sends = 0
        val prewrite = Prewrite(session) { stub, request ->
            val response = stub.kvPrewrite(request)
            if (sends++ == 0) {
                assertTrue(response.errorsList.single().hasLocked())
                TransactionResolution(session).resolve(bytes(key), owner, 0)
                response
            } else {
                assertTrue(response.onePcCommitTs > 0)
                throw Status.DEADLINE_EXCEEDED.asRuntimeException()
            }
        }
        assertFailsWith<CommitUnknown> {
            AtomicCommit(session, prewriter = prewrite).commit(start, mapOf(key to byteArrayOf(1)))
        }
        assertEquals(2, sends)
        assertEquals(listOf<Byte>(1), session.createSnapshot().get(bytes(key)).toByteArray().toList())
    }

    @Test
    fun `lost fallback response leaves the unresolved lock for native recovery`() = fixture { session, key ->
        val start = session.timestamp.version
        val prewrite = Prewrite(session) { stub, request ->
            val response = stub.kvPrewrite(request.toBuilder().setTryOnePc(false).setLockTtl(1000).build())
            assertEquals(0, response.errorsCount)
            throw Status.UNAVAILABLE.asRuntimeException()
        }
        assertFailsWith<CommitUnknown> {
            AtomicCommit(session, prewriter = prewrite).commit(start, mapOf(key to byteArrayOf(1)))
        }
        val region = session.regionManager.getRegionByKey(bytes(key))
        val store = session.regionManager.getStoreById(region.leader.storeId)
        val channel = session.channelFactory.getChannel(store.address, session.pdClient.hostMapping)
        val state = TikvGrpc.newBlockingStub(channel).kvCheckTxnStatus(Kvrpcpb.CheckTxnStatusRequest.newBuilder()
            .setContext(region.getReplicaContext(region.leader, emptySet()))
            .setPrimaryKey(session.pdClient.codec.encodeKey(bytes(key)))
            .setLockTs(start).setCallerStartTs(start).setCurrentTs(start).build())
        assertTrue(state.lockTtl > 0, "Unknown 1PC must not roll back a possible fallback lock")
        Thread.sleep(state.lockTtl + 200)
        val expired = TikvGrpc.newBlockingStub(channel).kvCheckTxnStatus(Kvrpcpb.CheckTxnStatusRequest.newBuilder()
            .setContext(region.getReplicaContext(region.leader, emptySet()))
            .setPrimaryKey(session.pdClient.codec.encodeKey(bytes(key)))
            .setLockTs(start).setCallerStartTs(session.timestamp.version)
            .setCurrentTs(session.timestamp.version).build())
        assertEquals(0L, expired.lockTtl)
        assertEquals(Kvrpcpb.Action.TTLExpireRollback, expired.action)
        assertTrue(session.createSnapshot().get(bytes(key)).isEmpty)
    }

    @Test
    fun `region errors retry the same transaction and ineligible writes stay two phase`() = fixture { session, key ->
        val starts = mutableListOf<Long>()
        val prewrite = Prewrite(session) { stub, request ->
            starts += request.startVersion
            if (starts.size == 1) return@Prewrite Kvrpcpb.PrewriteResponse.newBuilder()
                .setRegionError(Errorpb.Error.newBuilder()
                    .setMaxTimestampNotSynced(Errorpb.MaxTimestampNotSynced.getDefaultInstance())).build()
            val response = stub.kvPrewrite(request)
            assertTrue(response.onePcCommitTs > 0)
            if (starts.size == 2) Kvrpcpb.PrewriteResponse.newBuilder()
                .setRegionError(Errorpb.Error.newBuilder().setMessage("Injected region change after apply"))
                .build() else response
        }
        assertTrue(AtomicCommit(session, prewriter = prewrite)
            .commit(session.timestamp.version, mapOf(key to byteArrayOf(1))))
        assertEquals(3, starts.size)
        assertEquals(1, starts.distinct().size)
        val ordinary = Prewrite(session) { stub, request ->
            assertFalse(request.tryOnePc)
            stub.kvPrewrite(request)
        }
        val commit = AtomicCommit(session, prewriter = ordinary)
        assertTrue(commit.commit(session.timestamp.version, mapOf(key to byteArrayOf(2)), guards = setOf("$key/guard")))
        assertTrue(commit.commit(session.timestamp.version,
            mapOf(key to byteArrayOf(3), "$key/second" to byteArrayOf(1))))
        assertTrue(commit.commit(session.timestamp.version,
            mapOf("tikronos-test/" + UUID.randomUUID() to byteArrayOf(1))))
        assertTrue(commit.commit(session.timestamp.version,
            mapOf(Storage.itemKey("index/" + UUID.randomUUID(), "row") to byteArrayOf(1))))
        assertTrue(AtomicCommit(session, prewriter = ordinary, onePhase = false)
            .commit(session.timestamp.version, mapOf(key to byteArrayOf(4))))
    }

    @Test
    fun `short budget after a lock wait aborts before resending`() = fixture { session, key ->
        val owner = lockedOwner(session, key)
        var sends = 0
        val prewrite = Prewrite(session) { stub, request ->
            sends++
            stub.kvPrewrite(request).also { response ->
                assertTrue(response.errorsList.single().hasLocked())
                TransactionResolution(session).resolve(bytes(key), owner, 0)
                Thread.sleep(2100)
            }
        }
        assertFalse(AtomicCommit(session, prewriter = prewrite)
            .commit(session.timestamp.version, mapOf(key to byteArrayOf(1))))
        assertEquals(1, sends)
        assertTrue(session.createSnapshot().get(bytes(key)).isEmpty)
    }

    private fun lockedOwner(session: TiSession, key: String): Long {
        val owner = session.timestamp.version
        val region = session.regionManager.getRegionByKey(bytes(key))
        session.regionStoreClientBuilder.build(region).use { client ->
            client.prewrite(org.tikv.common.util.ConcreteBackOffer.newCustomBackOff(3000), bytes(key),
                listOf(Kvrpcpb.Mutation.newBuilder().setKey(bytes(key)).setOp(Kvrpcpb.Op.Put)
                    .setValue(bytes("owner")).build()), owner, 10000)
        }
        return owner
    }

    private fun fixture(test: (TiSession, String) -> Unit) {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
            test(session, Storage.itemKey("one-phase-" + UUID.randomUUID(), "item"))
        }
    }
}
