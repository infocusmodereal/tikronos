package tikronos

import java.util.UUID
import java.util.concurrent.Executor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import tikronos.model.ScanOptions
import tikronos.storage.AtomicCommit
import tikronos.storage.bytes
import tikronos.storage.prefixEnd

@Tag("tikv")
class RangeReaderIntegrationTest {
    @Test
    fun `bounded scans retain snapshot and direction through a region split`() = withStorageSession { session, db ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        val keys = (0 until 20).map { prefix + it.toString().padStart(2, '0') }
        val inserted = prefix + "new"
        try {
            db.atomic { tx -> keys.forEach { tx.write(it, byteArrayOf(1)) } }
            val snapshot = db.snapshot()
            val rows = db.ranges.scan(bytes(prefix), prefixEnd(prefix), true, snapshot).iterator()
            assertEquals(keys.first(), rows.next().key.toStringUtf8())
            session.splitRegionAndScatter(listOf(keys[6], keys[12]).map { it.encodeToByteArray() }, 5000, 1000, 0)
            db.atomic { tx ->
                tx.write(keys[2], byteArrayOf(2))
                tx.write(keys[3], null)
                tx.write(inserted, byteArrayOf(3))
            }
            val rest = rows.asSequence().toList()
            assertEquals(keys.drop(1), rest.map { it.key.toStringUtf8() })
            assertTrue(rest.all { it.value.toByteArray().contentEquals(byteArrayOf(1)) })
            for (forward in listOf(true, false)) {
                val result = db.ranges.scan(bytes(keys[6]), bytes(keys[12]), forward, snapshot,
                    ScanOptions(limit = 3, keyOnly = true)).toList()
                val expected = keys.subList(6, 12).let { if (forward) it else it.reversed() }.take(3)
                assertEquals(expected, result.map { it.key.toStringUtf8() })
                assertTrue(result.all { it.value.isEmpty })
            }
            val current = db.ranges.scan(bytes(prefix), prefixEnd(prefix), true,
                options = ScanOptions(keyOnly = true)).toList()
            assertEquals(keys.filter { it != keys[3] } + inserted, current.map { it.key.toStringUtf8() })
            assertTrue(db.ranges.scan(bytes(prefix), prefixEnd(prefix), true,
                options = ScanOptions(limit = 0)).none())
        } finally {
            db.atomic { tx -> (keys + inserted).forEach { tx.write(it, null) } }
        }
    }

    @Test
    fun `key only scans resolve committed secondary locks`() = withStorageSession { session, db ->
        val prefix = "tikronos-test/${UUID.randomUUID()}/"
        val keys = listOf("a-primary", "b-row", "c-row").map { prefix + it }
        val cleanup = mutableListOf<Runnable>()
        try {
            val before = db.snapshot()
            val start = db.snapshot().version
            assertTrue(AtomicCommit(session, Executor { cleanup += it })
                .commit(start, keys.associateWith { byteArrayOf(1) }))
            val snapshot = db.snapshot()
            assertTrue(session.readWithoutResolvingLocks(keys[1], snapshot.version).hasError())
            val rows = db.ranges.scan(bytes(keys[1]), prefixEnd(prefix), true, snapshot,
                ScanOptions(limit = 1, keyOnly = true)).toList()
            assertEquals(listOf(keys[1]), rows.map { it.key.toStringUtf8() })
            assertTrue(rows.single().value.isEmpty)
            assertTrue(db.ranges.scan(bytes(prefix), prefixEnd(prefix), true, before,
                ScanOptions(keyOnly = true)).none())
        } finally {
            cleanup.forEach { it.run() }
            db.atomic { tx -> keys.forEach { tx.write(it, null) } }
        }
    }
}
