package tikronos

import java.nio.file.Files
import java.util.UUID
import jdk.jfr.Recording
import jdk.jfr.consumer.RecordingFile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import tikronos.diagnostics.CatalogLeaseEvent
import tikronos.diagnostics.RequestEvent
import tikronos.diagnostics.RequestEvents
import tikronos.model.CatalogLease
import tikronos.model.Index
import tikronos.model.Table
import tikronos.storage.CatalogLeases
import tikronos.storage.Records
import tikronos.storage.Storage

@Tag("tikv")
class CatalogLeaseEventsIntegrationTest {
    @Test
    fun `profiling distinguishes renewal reuse and drain without changing snapshot coverage`() {
        withStorageSession { session, db -> Storage(session).use { other ->
            val id = UUID.randomUUID().toString()
            val table = Table("lease-$id", id, "pk", 0, null)
            val key = Storage.CATALOG + table.name
            val path = Files.createTempFile("catalog-leases-", ".jfr")
            try {
                db.atomic { it.write(key, Records.encodeTable(table)) }
                Recording().use { recording ->
                    recording.enable(CatalogLeaseEvent::class.java)
                    recording.start()
                    db.read(db.snapshot(), key)
                    val old = db.snapshot()
                    val initial = assertNotNull(db.catalog.cached(key, old.version))
                    while (db.snapshot().version < initial.until - CatalogLeases.DURATION / 2) Thread.sleep(10)
                    val renewal = RequestEvent()
                    RequestEvents.within(renewal) { assertNotNull(db.read(db.snapshot(), key)) }
                    assertEquals(1, renewal.catalogAcquisitions)
                    assertTrue(renewal.catalogAcquireNanos > 0)
                    assertTrue(assertNotNull(db.catalog.cached(key, old.version)).until > initial.until)
                    val drain = RequestEvent()
                    RequestEvents.within(drain) { other.atomic { it.write(key, Records.encodeTable(table)) } }
                    assertEquals(1, drain.catalogDrains)
                    assertTrue(drain.catalogDrainNanos > 0)
                    // Seed a shared durable lease with headroom; this instance has no local entry.
                    other.uncachedAtomic { it.write(CatalogLeases.key(key),
                        CatalogLeases.encode(CatalogLease(it.version + 5 * CatalogLeases.DURATION))) }
                    val cold = RequestEvent()
                    RequestEvents.within(cold) { assertEquals(table, Tables(other).find(table.name)) }
                    assertEquals(1, cold.catalogFallbackGets)
                    recording.stop()
                    recording.dump(path)
                }
                val events = RecordingFile.readAllEvents(path).filter { it.eventType.name == "tikronos.CatalogLease" }
                assertEquals(listOf("extended", "extended", "drained", "reused"),
                    events.map { it.getString("outcome") })
                assertEquals(listOf("cold", "renewal", "", "cold"), events.map { it.getString("trigger") })
                assertEquals(listOf(1, 1, 1, 0), events.map { it.getInt("leaseWriteAttempts") })
                assertTrue(events[2].getInt("polls") > 0)
                assertTrue(events[2].getDuration("waitNanos").toNanos() > 0)
            } finally {
                other.uncachedAtomic { it.write(CatalogLeases.key(key), null) }
                other.atomic { it.write(key, null) }
                Files.deleteIfExists(path)
            }
        } }
    }

    @Test
    fun `ineligible catalog loads are not reported as lease extensions`() {
        withStorageSession { _, db ->
            val id = UUID.randomUUID().toString()
            val table = Table("creating-$id", id, "pk", 0, null,
                indexes = listOf(Index("byGroup", id, "group", "S", status = "CREATING")))
            val key = Storage.CATALOG + table.name
            val absent = Storage.CATALOG + "absent-$id"
            val path = Files.createTempFile("catalog-ineligible-", ".jfr")
            try {
                db.atomic { it.write(key, Records.encodeTable(table)) }
                Recording().use { recording ->
                    recording.enable(CatalogLeaseEvent::class.java)
                    recording.start()
                    val event = RequestEvent()
                    RequestEvents.within(event) {
                        assertNull(db.read(db.snapshot(), absent))
                        assertNotNull(db.read(db.snapshot(), key))
                        db.uncachedAtomic { it.write(CatalogLeases.key(key), CatalogLeases.encode(
                            CatalogLease(blockedUntil = it.version + 5 * CatalogLeases.DURATION))) }
                        assertNotNull(db.read(db.snapshot(), key))
                    }
                    assertEquals(3, event.catalogFallbackGets)
                    recording.stop()
                    recording.dump(path)
                }
                val events = RecordingFile.readAllEvents(path).filter { it.eventType.name == "tikronos.CatalogLease" }
                assertEquals(listOf("missing", "creating", "blocked"), events.map { it.getString("outcome") })
                assertTrue(events.all { it.getInt("attempts") == 1 && it.getInt("leaseWriteAttempts") == 0 })
            } finally {
                db.atomic { it.write(key, null) }
                Files.deleteIfExists(path)
            }
        }
    }
}
