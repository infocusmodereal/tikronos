package tikronos

import java.nio.file.Files
import jdk.jfr.Recording
import jdk.jfr.consumer.RecordingFile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.tikv.common.meta.TiTimestamp
import tikronos.diagnostics.RequestEvent
import tikronos.diagnostics.RequestEvents
import tikronos.storage.TimestampOracle

class RequestEventsTest {
    @Test
    fun `flight recording retains phase attribution without leaking context into later requests`() {
        val path = Files.createTempFile("tikronos-request-", ".jfr")
        try {
            Recording().use { recording ->
                recording.enable(RequestEvent::class.java)
                recording.start()
                val event = assertNotNull(RequestEvents.begin("DynamoDB_20120810.PutItem"))
                RequestEvents.within(event) {
                    TimestampOracle { List(it) { TiTimestamp(1, 1) } }.next()
                    RequestEvents.onePhaseAttempt()
                    RequestEvents.onePhaseResult(true)
                    RequestEvents.status(200)
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
            val events = RecordingFile.readAllEvents(path).filter { it.eventType.name == "tikronos.Request" }
            assertEquals(listOf("PutItem", "Unknown"), events.map { it.getString("operation") })
            assertEquals(1, events[0].getInt("timestamps"))
            assertTrue(events[0].getDuration("timestampNanos").toNanos() > 0)
            assertEquals(1, events[0].getInt("onePhaseCommits"))
            assertEquals(0, events[1].getInt("timestamps"))
            assertEquals("admission", events[1].getString("rejection"))
        } finally { Files.deleteIfExists(path) }
    }
}
