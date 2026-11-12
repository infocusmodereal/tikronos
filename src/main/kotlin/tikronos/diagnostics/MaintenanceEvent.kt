package tikronos.diagnostics

import jdk.jfr.Category
import jdk.jfr.Event
import jdk.jfr.EventType
import jdk.jfr.Label
import jdk.jfr.Name
import jdk.jfr.StackTrace

@Name("tikronos.Maintenance")
@Label("TiKronos maintenance visit")
@Category("TiKronos")
@StackTrace(false)
internal class MaintenanceEvent : Event() {
    @JvmField var family = ""

    companion object {
        private val type = EventType.getEventType(MaintenanceEvent::class.java)

        fun measure(family: String, work: () -> Unit) {
            if (!type.isEnabled) return work()
            val event = MaintenanceEvent().also { it.family = family; it.begin() }
            try { work() } finally { event.commit() }
        }
    }
}
