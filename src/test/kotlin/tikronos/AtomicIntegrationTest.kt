package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
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
        TiSession.create(TiConfiguration.createDefault(endpoint)).use { test(Storage(it), it) }
    }
}
