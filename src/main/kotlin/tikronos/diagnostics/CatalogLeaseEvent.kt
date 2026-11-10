package tikronos.diagnostics

import jdk.jfr.Category
import jdk.jfr.Event
import jdk.jfr.EventType
import jdk.jfr.Label
import jdk.jfr.Name
import jdk.jfr.StackTrace
import jdk.jfr.Timespan

/** One complete acquisition or drain, including transaction retries; never records catalog identities. */
@Name("tikronos.CatalogLease")
@Label("TiKronos catalog lease")
@Category("TiKronos")
@StackTrace(false)
internal class CatalogLeaseEvent : Event() {
    @JvmField var action = ""
    @JvmField var trigger = ""
    @JvmField var outcome = "error"
    @JvmField var attempts = 0
    @JvmField var leaseWriteAttempts = 0
    @JvmField var polls = 0
    @JvmField @Timespan(Timespan.NANOSECONDS) var waitNanos = 0L

    companion object {
        private val type = EventType.getEventType(CatalogLeaseEvent::class.java)

        fun <T> measure(action: String, trigger: String = "", work: (CatalogLeaseEvent?) -> T): T {
            val started = RequestEvents.start()
            val event = if (!type.isEnabled) null else CatalogLeaseEvent().apply {
                this.action = action
                this.trigger = trigger
                begin()
            }
            var successful = false
            return try {
                work(event).also { successful = true }
            } finally {
                RequestEvents.catalog(action, started)
                event?.apply {
                    // A failed commit must not be counted as a durable lease extension.
                    if (!successful) outcome = "error"
                    commit()
                }
            }
        }
    }
}
