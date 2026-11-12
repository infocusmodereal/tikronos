package tikronos.diagnostics

import jdk.jfr.Category
import jdk.jfr.Event
import jdk.jfr.Label
import jdk.jfr.Name
import jdk.jfr.StackTrace
import jdk.jfr.Timespan

/** Bounded operation names and timings only; no keys, values, headers or credentials. */
@Name("tikronos.Request")
@Label("TiKronos request")
@Category("TiKronos")
@StackTrace(false)
internal class RequestEvent : Event() {
    @JvmField var operation = "Unknown"
    @JvmField var status = 0
    @JvmField var rejection = ""
    @JvmField @Timespan(Timespan.NANOSECONDS) var queueNanos = 0L
    @JvmField @Timespan(Timespan.NANOSECONDS) var timestampNanos = 0L
    @JvmField @Timespan(Timespan.NANOSECONDS) var timestampAllocationNanos = 0L
    @JvmField @Timespan(Timespan.NANOSECONDS) var prewriteNanos = 0L
    @JvmField @Timespan(Timespan.NANOSECONDS) var commitNanos = 0L
    @JvmField @Timespan(Timespan.NANOSECONDS) var catalogAcquireNanos = 0L
    @JvmField @Timespan(Timespan.NANOSECONDS) var catalogDrainNanos = 0L
    @JvmField var catalogAcquisitions = 0
    @JvmField var catalogDrains = 0
    @JvmField var catalogFallbackGets = 0
    @JvmField var catalogLoaderBusy = 0
    @JvmField var batchPairs = 0
    @JvmField var timestamps = 0
    @JvmField var prewrites = 0
    @JvmField var commits = 0
    @JvmField var onePhaseAttempts = 0
    @JvmField var onePhaseCommits = 0
    @JvmField var onePhaseFallbacks = 0
}
