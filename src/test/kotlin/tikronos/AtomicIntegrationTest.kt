package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executor
import org.tikv.kvproto.TikvGrpc
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import org.tikv.common.TiSession
import tikronos.storage.AtomicCommit
import tikronos.storage.Storage
import tikronos.storage.SingleKeyCommit
import tikronos.storage.bytes
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb

@Tag("tikv")
class AtomicIntegrationTest {
    @Test
    fun `deferred completion preserves visibility and native guard recovery`() = withStorage { db, session ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        val config = prefix + "config"
        val item = prefix + "item"
        val event = prefix + "event"
        db.mutate(config) { byteArrayOf(1) }
        val before = db.snapshot()
        val tasks = mutableListOf<Runnable>()
        val tx = db.transaction()
        tx.read(config)
        tx.write(item, byteArrayOf(2))
        tx.write(event, byteArrayOf(2))
        assertTrue(AtomicCommit(session, Executor { tasks.add(it) })
            .commit(tx.version, tx.writes, tx.guards()))
        assertTrue(tasks.size == 1)
        // No queued cleanup has run: readers and writers must recover using the primary decision.
        db.mutate(config) { byteArrayOf(3) }
        for (key in listOf(item, event)) {
            assertNull(db.read(before, key))
            assertContentEquals(byteArrayOf(2), db.read(db.snapshot(), key))
        }
        tasks.single().run()
        db.atomic { current -> listOf(config, item, event).forEach { current.write(it, null) } }
    }

    @Test
    fun `secondary conflict rolls back the primary without exposing a partial mutation`() = withStorage { db, session ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        val start = session.timestamp.version
        db.mutate(prefix + "b") { byteArrayOf(2) }
        assertFalse(AtomicCommit(session).commit(start,
            mapOf(prefix + "a" to byteArrayOf(1), prefix + "b" to byteArrayOf(1))))
        assertNull(db.read(db.snapshot(), prefix + "a"))
        assertContentEquals(byteArrayOf(2), db.read(db.snapshot(), prefix + "b"))
        db.mutate(prefix + "b") { null }
    }

    @Test
    fun `read dependency forces retry and all records share one visibility boundary`() = withStorage { db, _ ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        db.mutate(prefix + "config") { byteArrayOf(1) }
        val before = db.snapshot()
        var first = true
        val initial = db.transaction()
        initial.read(prefix + "config")
        db.atomic(initial) { tx ->
            val value = tx.read(prefix + "config")
            if (first) {
                first = false
                db.mutate(prefix + "config") { byteArrayOf(2) }
            }
            tx.write(prefix + "item", value)
            tx.write(prefix + "event", value)
        }
        val after = db.snapshot()
        for (name in listOf("item", "event")) {
            assertNull(db.read(before, prefix + name))
            assertContentEquals(byteArrayOf(2), db.read(after, prefix + name))
        }
        db.atomic { tx -> listOf("config", "item", "event").forEach { tx.write(prefix + it, null) } }
    }

    @Test
    fun `failed commit timestamp allocation rolls back every prepared key immediately`() = withStorage { _, session ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        val keys = listOf(prefix + "a", prefix + "b")
        val start = session.timestamp.version
        assertFailsWith<IllegalStateException> {
            AtomicCommit(session, timestamp = { error("PD unavailable") })
                .commit(start, keys.associateWith { byteArrayOf(1) })
        }
        val readVersion = session.timestamp.version
        for (key in keys) {
            val region = session.regionManager.getRegionByKey(bytes(key))
            val store = session.regionManager.getStoreById(region.leader.storeId)
            val channel = session.channelFactory.getChannel(store.address, session.pdClient.hostMapping)
            val request = Kvrpcpb.GetRequest.newBuilder()
                .setContext(region.getReplicaContext(region.leader, emptySet()))
                .setKey(session.pdClient.codec.encodeKey(bytes(key))).setVersion(readVersion).build()
            // Bypass SDK lock resolution: an abandoned lock must fail, not wait until its TTL expires.
            val response = TikvGrpc.newBlockingStub(channel)
                .withDeadlineAfter(3, TimeUnit.SECONDS).kvGet(request)
            assertFalse(response.hasRegionError(), response.regionError.toString())
            assertFalse(response.hasError(), response.error.toString())
            assertTrue(response.value.isEmpty)
        }
    }

    @Test
    fun `reader completes secondary after writer stops at committed primary`() = withStorage { db, session ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        val primary = bytes(prefix + "item")
        val secondary = bytes(prefix + "event")
        val before = db.snapshot()
        val start = session.timestamp.version
        for (key in listOf(primary, secondary)) {
            val budget = ConcreteBackOffer.newCustomBackOff(3000)
            session.regionStoreClientBuilder.build(key, budget).use { client ->
                val mutation = Kvrpcpb.Mutation.newBuilder().setKey(key).setOp(Kvrpcpb.Op.Put)
                    .setValue(bytes("committed")).build()
                client.prewrite(budget, primary, listOf(mutation), start, 60_000)
            }
        }
        SingleKeyCommit(session).commit(primary, start, session.timestamp.version)
        // Deliberately omit secondary commit, as after a writer process disappears.
        val recovered = db.snapshot()
        for (key in listOf(primary, secondary)) {
            assertNull(db.read(before, key.toStringUtf8()))
            assertContentEquals("committed".encodeToByteArray(), db.read(recovered, key.toStringUtf8()))
        }
        db.atomic { tx -> listOf(primary, secondary).forEach { tx.write(it.toStringUtf8(), null) } }
    }

    private fun withStorage(test: (Storage, TiSession) -> Unit) {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        withStorageSession(endpoint) { session, db -> test(db, session) }
    }
}
