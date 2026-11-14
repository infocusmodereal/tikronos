package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.kvproto.Errorpb
import org.tikv.kvproto.Kvrpcpb
import tikronos.storage.AtomicCommit
import tikronos.storage.CommitUnknown
import tikronos.storage.Prewrite
import tikronos.storage.Storage
import tikronos.storage.bytes
import tikronos.storage.readBatch

@Tag("tikv")
class IndexedCommitIntegrationTest {
    @Test
    fun `one region decides all rows at one timestamp even when response is lost`() = withStorageSession { session, _ ->
        val id = UUID.randomUUID().toString()
        val base = Storage.itemKey("index.-$id", "base")
        val entry = Storage.itemKey("index/000-$id", "entry")
        val values = mapOf(base to byteArrayOf(1), entry to byteArrayOf(2))
        val before = session.createSnapshot()
        var sends = 0
        val prewrite = Prewrite(session) { stub, request ->
            assertTrue(request.tryOnePc)
            assertFalse(request.useAsyncCommit)
            assertEquals(2, request.mutationsCount)
            val response = stub.kvPrewrite(request)
            assertTrue(response.onePcCommitTs >= request.minCommitTs)
            sends++
            if (sends == 2) Kvrpcpb.PrewriteResponse.newBuilder().setRegionError(
                Errorpb.Error.newBuilder().setMessage("Lost decision after application")).build()
            else response
        }
        val commit = AtomicCommit(session, prewriter = prewrite)
        assertTrue(commit.commit(session.timestamp.version, values))
        val written = session.createSnapshot()
        assertFailsWith<CommitUnknown> {
            commit.commit(session.timestamp.version, values.mapValues { null })
        }
        assertEquals(2, sends)
        assertTrue(before.readBatch(values.keys.toList()).isEmpty())
        assertEquals(values.mapValues { it.value.toList() },
            written.readBatch(values.keys.toList()).mapValues { it.value.toList() })
        assertTrue(session.createSnapshot().readBatch(values.keys.toList()).isEmpty())
    }

    @Test
    fun `index conflict and lease fallback cannot publish a partial item`() = withStorageSession { session, _ ->
        val id = UUID.randomUUID().toString()
        val base = Storage.itemKey("index.-$id", "base")
        val entry = Storage.itemKey("index/000-$id", "entry")
        val start = session.timestamp.version
        val values = mapOf(base to byteArrayOf(1), entry to byteArrayOf(2))
        val ordinary = AtomicCommit(session, onePhase = false)
        assertTrue(ordinary.commit(session.timestamp.version, mapOf(entry to byteArrayOf(9))))
        assertFalse(AtomicCommit(session).commit(start, values))
        val minimum = session.timestamp
        session.createSnapshot().get(bytes(entry))
        var timestamps = 0
        var fallback = false
        val prewrite = Prewrite(session) { stub, request ->
            assertTrue(request.tryOnePc)
            stub.kvPrewrite(request).also { fallback = it.errorsCount == 0 && it.onePcCommitTs == 0L }
        }
        val commit = AtomicCommit(session, timestamp = { if (timestamps++ == 0) minimum else session.timestamp },
            prewriter = prewrite)
        assertFalse(commit.commit(minimum.version, values, ceiling = minimum.version + 1))
        assertTrue(fallback)
        assertEquals(mapOf(entry to listOf<Byte>(9)),
            session.createSnapshot().readBatch(values.keys.toList()).mapValues { it.value.toList() })
    }

    @Test
    fun `split region and byte cap never propose a partial decision`() = withStorageSession { session, _ ->
        val id = UUID.randomUUID().toString()
        val base = Storage.itemKey("index.-$id", "base")
        val entry = Storage.itemKey("index/zz-$id", "entry")
        val small = mapOf(base to byteArrayOf(1), entry to byteArrayOf(2))
        val requests = mutableListOf<Int>()
        val commit = AtomicCommit(session, prewriter = Prewrite(session) { stub, request ->
            assertFalse(request.tryOnePc)
            requests += request.mutationsCount
            stub.kvPrewrite(request)
        })
        assertTrue(commit.commit(session.timestamp.version, small.mapValues { ByteArray(600 * 1024) }))
        assertEquals(listOf(1, 1), requests)
        requests.clear()
        session.splitRegionAndScatter(listOf(entry.encodeToByteArray()), 5000, 1000, 0)
        val baseRegion = session.regionManager.getRegionByKey(bytes(base)).id
        check(baseRegion != session.regionManager.getRegionByKey(bytes(entry)).id)
        assertTrue(commit.commit(session.timestamp.version, small))
        assertEquals(listOf(1, 1), requests)
    }
}
