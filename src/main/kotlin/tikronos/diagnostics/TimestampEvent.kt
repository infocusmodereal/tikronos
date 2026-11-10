package tikronos.diagnostics

import jdk.jfr.Category
import jdk.jfr.Event
import jdk.jfr.EventType
import jdk.jfr.Label
import jdk.jfr.Name
import jdk.jfr.StackTrace
import jdk.jfr.Timespan

/** One allocation batch, including retries; timings contain no endpoint or request data. */
@Name("tikronos.Timestamp")
@Label("TiKronos timestamp allocation")
@Category("TiKronos")
@StackTrace(false)
internal class TimestampEvent : Event() {
    @JvmField var count = 0
    @JvmField var attempts = 0
    @JvmField var successful = false
    @JvmField @Timespan(Timespan.NANOSECONDS) var rpcNanos = 0L

    companion object {
        private val type = EventType.getEventType(TimestampEvent::class.java)

        fun begin(count: Int): TimestampEvent? = if (!type.isEnabled) null else
            TimestampEvent().apply { this.count = count; begin() }
    }
}
