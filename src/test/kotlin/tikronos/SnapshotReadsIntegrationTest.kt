package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import org.tikv.common.Snapshot
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.bytes
import tikronos.storage.readBatch

@Tag("tikv")
class SnapshotReadsIntegrationTest {
    @Test
    fun `batch reads and transaction prefetch retain one snapshot across regions`() {
        withStorageSession { session, db ->
            val id = UUID.randomUUID().toString()
            val keys = listOf("a", "m", "z").map { Storage.itemKey(id, it) }
            val missing = Storage.itemKey(id, "missing")
            try {
                db.atomic { tx -> keys.forEach { tx.write(it, byteArrayOf(1)) } }
                val snapshot = db.snapshot()
                session.splitRegionAndScatter(listOf(keys[1].encodeToByteArray()), 5000, 1000, 0)
                assertNotEquals(session.regionManager.getRegionByKey(bytes(keys.first())).id,
                    session.regionManager.getRegionByKey(bytes(keys.last())).id)
                db.atomic { tx ->
                    tx.write(keys[0], byteArrayOf(2))
                    tx.write(keys[1], null)
                    tx.write(missing, byteArrayOf(3))
                }
                assertEquals(keys.associateWith { listOf<Byte>(1) },
                    snapshot.readBatch(keys.reversed() + missing).mapValues { it.value.toList() })
                val prefetched = object : Snapshot(snapshot.timestamp, session) {
                    override fun get(key: ByteArray): ByteArray = error("Unexpected point read after prefetch")
                }
                val tx = Transaction(prefetched, db, cached = false)
                tx.prefetch(keys + missing)
                keys.forEach { assertEquals(listOf<Byte>(1), tx.read(it)?.toList()) }
                assertNull(tx.read(missing))
                assertEquals(mapOf(keys[0] to listOf<Byte>(2), keys[2] to listOf<Byte>(1),
                    missing to listOf<Byte>(3)),
                    db.snapshot().readBatch(keys + missing).mapValues { it.value.toList() })
            } finally {
                db.atomic { tx -> (keys + missing).forEach { tx.write(it, null) } }
            }
        }
    }
}
