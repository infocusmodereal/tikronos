package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiSession
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.RegionException
import org.tikv.kvproto.Errorpb
import org.tikv.kvproto.Kvrpcpb
import org.tikv.shade.io.grpc.Status
import tikronos.storage.AtomicCommit
import tikronos.storage.CommitNotApplied
import tikronos.storage.CommitUnknown
import tikronos.storage.SingleKeyCommit
import tikronos.storage.Storage
import tikronos.storage.TransactionResolution
import tikronos.storage.bytes

@Tag("tikv")
class CommitOutcomesIntegrationTest {
    @Test
    fun `lost primary request preserves prepared locks without rollback`() = fixture { session, keys ->
        val start = session.timestamp.version
        var sends = 0
        val primary = SingleKeyCommit(session, exchange = { _, _, _, _, _ ->
            sends++
            throw Status.DEADLINE_EXCEEDED.asRuntimeException()
        })
        try {
            assertFailsWith<CommitUnknown> {
                AtomicCommit(session, primaryCommit = primary).commit(start, keys.associateWith { byteArrayOf(1) })
            }
            assertEquals(1, sends)
            val version = session.timestamp.version
            keys.forEach { assertTrue(session.readWithoutResolvingLocks(it, version).error.hasLocked()) }
        } finally {
            TransactionResolution(session).resolve(keys.map(::bytes), start, 0)
        }
    }

    @Test
    fun `lost successful primary response does not roll back or replay the transaction`() = fixture { session, keys ->
        var sends = 0
        val primary = SingleKeyCommit(session, exchange = { client, budget, key, start, commit ->
            sends++
            client.commit(budget, listOf(key), start, commit)
            throw Status.UNAVAILABLE.withDescription("Lost committed response").asRuntimeException()
        })
        assertFailsWith<CommitUnknown> {
            AtomicCommit(session, primaryCommit = primary)
                .commit(session.timestamp.version, keys.associateWith { byteArrayOf(1) })
        }
        assertEquals(1, sends)
        val version = session.timestamp.version
        assertFalse(session.readWithoutResolvingLocks(keys.first(), version).hasError())
        assertTrue(session.readWithoutResolvingLocks(keys.last(), version).error.hasLocked())
        val after = session.createSnapshot()
        keys.forEach {
            assertContentEquals(byteArrayOf(1), after.get(it.encodeToByteArray()))
        }
    }

    @Test
    fun `key errors after an SDK call cannot authorize rollback of a durable primary`() = fixture { session, keys ->
        val primary = SingleKeyCommit(session, exchange = { client, budget, key, start, commit ->
            client.commit(budget, listOf(key), start, commit)
            // Model a later error returned after the SDK hid a successful attempt's response.
            throw KeyException(Kvrpcpb.KeyError.newBuilder().setRetryable("unrecognized native error").build())
        })
        val failure = assertFailsWith<CommitUnknown> {
            AtomicCommit(session, primaryCommit = primary)
                .commit(session.timestamp.version, keys.associateWith { byteArrayOf(1) })
        }
        assertTrue(failure.cause is KeyException)
        keys.forEach { assertContentEquals(byteArrayOf(1), session.createSnapshot().get(it.encodeToByteArray())) }
    }

    @Test
    fun `region retry exhaustion cannot erase an earlier uncertain primary attempt`() = fixture { session, keys ->
        var sends = 0
        val primary = SingleKeyCommit(session, exchange = { client, budget, key, start, commit ->
            if (sends++ == 0) client.commit(budget, listOf(key), start, commit)
            throw RegionException(Errorpb.Error.newBuilder().setMessage("Region changed after lost response").build())
        })
        assertFailsWith<CommitUnknown> {
            AtomicCommit(session, primaryCommit = primary)
                .commit(session.timestamp.version, keys.associateWith { byteArrayOf(1) })
        }
        assertTrue(sends > 1)
        keys.forEach { assertContentEquals(byteArrayOf(1), session.createSnapshot().get(it.encodeToByteArray())) }
    }

    @Test
    fun `failed timestamp refresh after explicit rejection rolls back all prepared keys`() = fixture { session, keys ->
        var sends = 0
        val primary = SingleKeyCommit(session, timestamp = { error("PD unavailable after rejection") },
            exchange = { client, budget, key, start, commit ->
                if (sends++ == 0) {
                    throw RegionException(Errorpb.Error.newBuilder().setMessage("Before primary send").build())
                }
                // A fresh reader pushes native minCommitTs beyond the proposed timestamp.
                assertTrue(session.createSnapshot().get(key).isEmpty)
                client.commit(budget, listOf(key), start, commit)
            })
        val failure = assertFailsWith<CommitNotApplied> {
            AtomicCommit(session, primaryCommit = primary)
                .commit(session.timestamp.version, keys.associateWith { byteArrayOf(1) })
        }
        assertEquals("PD unavailable after rejection", failure.cause?.message)
        assertEquals(2, sends)
        val version = session.timestamp.version
        keys.forEach {
            val response = session.readWithoutResolvingLocks(it, version)
            assertFalse(response.hasError(), response.error.toString())
            assertTrue(response.value.isEmpty)
        }
    }

    private fun fixture(action: (TiSession, List<String>) -> Unit) {
        withStorageSession { session, db ->
            val id = UUID.randomUUID().toString()
            val keys = listOf("a", "b").map { Storage.itemKey(id, it) }
            try { action(session, keys) } finally {
                db.atomic { tx -> keys.forEach { tx.write(it, null) } }
            }
        }
    }
}
