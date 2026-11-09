package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiSession
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb
import tikronos.storage.AtomicCommit
import tikronos.storage.MetadataFences
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.TransactionResolution
import tikronos.storage.TtlRecords
import tikronos.storage.bytes

@Tag("tikv")
class MetadataFencesIntegrationTest {
    @Test
    fun `an in-flight item does not lock every writer using the same metadata`() =
        withFixture { db, session, config, prefix ->
            val first = Transaction(db.snapshot(), db)
            first.write(prefix + "a", first.read(config))
            val keys = (first.writes.keys + first.guards()).sorted()
            val primary = bytes(keys.first())
            try {
                for (key in keys) {
                    val mutation =
                        Kvrpcpb.Mutation.newBuilder()
                            .setKey(bytes(key))
                            .setOp(if (key in first.guards()) Kvrpcpb.Op.Lock else Kvrpcpb.Op.Put)
                            .setValue(
                                org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
                                    first.writes[key] ?: byteArrayOf()
                                )
                            )
                            .build()
                    val budget = ConcreteBackOffer.newCustomBackOff(3000)
                    session.regionStoreClientBuilder.build(mutation.key, budget).use {
                        it.prewrite(budget, primary, listOf(mutation), first.version, 60_000)
                    }
                }
                val second = Transaction(db.snapshot(), db)
                second.write(prefix + "b", second.read(config))
                assertTrue(first.guards().intersect(second.guards()).isEmpty())
                assertTrue(
                    AtomicCommit(session).commit(second.version, second.writes, second.guards())
                )
                assertContentEquals(byteArrayOf(1), db.read(db.snapshot(), prefix + "b"))
            } finally {
                TransactionResolution(session).resolve(keys.map(::bytes), first.version, 0)
            }
        }

    @Test
    fun `metadata updates tolerate completed readers but still reject stale metadata writes`() =
        withFixture { db, session, config, prefix ->
            val metadata = Transaction(db.snapshot(), db)
            metadata.read(config)
            metadata.write(config, byteArrayOf(2))
            val stale = Transaction(db.snapshot(), db)
            stale.read(config)
            stale.write(config, byteArrayOf(3))
            db.atomic { tx -> tx.write(prefix + "a", tx.read(config)) }
            assertTrue(
                AtomicCommit(session).commit(metadata.version, metadata.writes, metadata.guards())
            )
            assertFalse(AtomicCommit(session).commit(stale.version, stale.writes, stale.guards()))
            assertContentEquals(byteArrayOf(2), db.read(db.snapshot(), config))
        }

    @Test
    fun `metadata changed after a snapshot invalidates its deferred validation lock`() =
        withFixture { db, session, config, prefix ->
            val old = Transaction(db.snapshot(), db)
            old.prefetch(listOf(config, prefix + "a"))
            old.write(prefix + "a", old.read(config))
            db.mutate(config) { byteArrayOf(2) }
            assertFalse(AtomicCommit(session).commit(old.version, old.writes, old.guards()))
            db.atomic { tx -> tx.write(prefix + "a", tx.read(config)) }
            assertContentEquals(byteArrayOf(2), db.read(db.snapshot(), prefix + "a"))
        }

    private fun withFixture(test: (Storage, TiSession, String, String) -> Unit) {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        withStorageSession(endpoint) { session, db ->
            val id = UUID.randomUUID().toString()
            val config = TtlRecords.CONFIGS + id
            val prefix = "tikronos-test/$id/"
            try {
                db.mutate(config) { byteArrayOf(1) }
                test(db, session, config, prefix)
            } finally {
                db.atomic { tx ->
                    listOf(config, prefix + "a", prefix + "b").forEach { tx.write(it, null) }
                    MetadataFences.keys(config).forEach { tx.write(it, null) }
                }
            }
        }
    }
}
