package tikronos

import java.nio.file.Files
import jdk.jfr.Recording
import jdk.jfr.consumer.RecordingFile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.tikv.common.meta.TiTimestamp
import tikronos.diagnostics.RequestEvent
import tikronos.diagnostics.RequestEvents
import tikronos.diagnostics.CatalogLeaseEvent
import tikronos.storage.TimestampOracle

class RequestEventsTest {
    @Test
    fun `flight recording retains phase attribution without leaking context into later requests`() {
        val path = Files.createTempFile("tikronos-request-", ".jfr")
        try {
            Recording().use { recording ->
                recording.enable(RequestEvent::class.java)
                recording.enable(CatalogLeaseEvent::class.java)
                recording.start()
                val event = assertNotNull(RequestEvents.begin("DynamoDB_20120810.PutItem"))
                RequestEvents.within(event) {
                    CatalogLeaseEvent.measure("acquire", "renewal") { lease ->
                        assertNotNull(lease).apply { attempts = 2; leaseWriteAttempts = 2; outcome = "extended" }
                        TimestampOracle {
                            Thread.sleep(1) // Make the measured allocation exceed timer resolution.
                            List(it) { TiTimestamp(1, 1) }
                        }.next()
                    }
                    RequestEvents.catalogFallback()
                    RequestEvents.catalogLoaderBusy()
                    RequestEvents.onePhaseAttempt()
                    RequestEvents.onePhaseResult(true)
                    RequestEvents.status(200)
                }
                assertFailsWith<IllegalStateException> {
                    RequestEvents.within(event) {
                        CatalogLeaseEvent.measure("drain") { lease ->
                            lease?.outcome = "drained"
                            error("Injected action failure")
                        }
                    }
                }
                event.commit()
                assertEquals(0, RequestEvents.start())
                val rejected = assertNotNull(RequestEvents.begin("untrusted.operation-name"))
                rejected.rejection = "admission"
                rejected.status = 400
                rejected.commit()
                recording.stop()
                recording.dump(path)
            }
            assertRecording(path)
        } finally { Files.deleteIfExists(path) }
        assertNull(CatalogLeaseEvent.measure("acquire") { it })
    }

    private fun assertRecording(path: java.nio.file.Path) {
        val events = RecordingFile.readAllEvents(path).filter { it.eventType.name == "tikronos.Request" }
        assertEquals(listOf("PutItem", "Unknown"), events.map { it.getString("operation") })
        assertEquals(1, events[0].getInt("timestamps"))
        assertTrue(events[0].getDuration("timestampNanos").toNanos() > 0)
        val allocation = events[0].getDuration("timestampAllocationNanos").toNanos()
        assertTrue(allocation > 0)
        assertTrue(allocation <= events[0].getDuration("timestampNanos").toNanos())
        assertEquals(1, events[0].getInt("onePhaseCommits"))
        assertEquals(1, events[0].getInt("catalogAcquisitions"))
        assertEquals(1, events[0].getInt("catalogDrains"))
        assertEquals(1, events[0].getInt("catalogFallbackGets"))
        assertEquals(1, events[0].getInt("catalogLoaderBusy"))
        assertTrue(events[0].getDuration("catalogAcquireNanos").toNanos() > 0)
        assertTrue(events[0].getDuration("catalogDrainNanos").toNanos() > 0)
        assertEquals(0, events[1].getInt("timestamps"))
        assertEquals(0, events[1].getInt("catalogAcquisitions"))
        assertEquals("admission", events[1].getString("rejection"))
        val leases = RecordingFile.readAllEvents(path).filter { it.eventType.name == "tikronos.CatalogLease" }
        assertEquals(listOf("extended", "error"), leases.map { it.getString("outcome") })
        assertEquals(2, leases.first().getInt("leaseWriteAttempts"))
    }
}
