package tikronos.diagnostics

import jdk.jfr.EventType

/** The context is installed only inside the blocking dispatcher, never across a coroutine suspension. */
@Suppress("TooManyFunctions") // Keep all request phase accounting behind the same thread-local lifecycle.
internal object RequestEvents {
    private val type = EventType.getEventType(RequestEvent::class.java)
    private val current = ThreadLocal<RequestEvent>()
    private val operations = setOf(
        "GetItem", "PutItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem", "Query", "Scan",
        "CreateTable", "DescribeTable", "DeleteTable", "ListTables", "UpdateTable", "DescribeEndpoints",
        "UpdateTimeToLive", "DescribeTimeToLive", "TagResource", "UntagResource", "ListTagsOfResource",
        "DescribeContinuousBackups", "ListStreams", "DescribeStream", "GetShardIterator", "GetRecords",
    )

    fun begin(target: String?): RequestEvent? {
        if (!type.isEnabled) return null
        return RequestEvent().apply {
            operation = target?.substringAfterLast('.')?.takeIf { it in operations } ?: "Unknown"
            begin()
        }
    }

    inline fun <T> within(event: RequestEvent?, action: () -> T): T {
        if (event == null) return action()
        val previous = current.get()
        current.set(event)
        return try { action() } finally {
            if (previous == null) current.remove() else current.set(previous)
        }
    }

    fun child(): RequestEvent? = current.get()?.let { RequestEvent() }

    fun include(child: RequestEvent?) {
        if (child == null) return
        val parent = checkNotNull(current.get())
        parent.batchPairs++
        parent.timestampNanos += child.timestampNanos
        parent.timestampAllocationNanos += child.timestampAllocationNanos
        parent.prewriteNanos += child.prewriteNanos
        parent.commitNanos += child.commitNanos
        parent.catalogAcquireNanos += child.catalogAcquireNanos
        parent.catalogDrainNanos += child.catalogDrainNanos
        parent.catalogAcquisitions += child.catalogAcquisitions
        parent.catalogDrains += child.catalogDrains
        parent.catalogFallbackGets += child.catalogFallbackGets
        parent.catalogLoaderBusy += child.catalogLoaderBusy
        parent.timestamps += child.timestamps
        parent.prewrites += child.prewrites
        parent.commits += child.commits
        parent.onePhaseAttempts += child.onePhaseAttempts
        parent.onePhaseCommits += child.onePhaseCommits
        parent.onePhaseFallbacks += child.onePhaseFallbacks
    }

    fun start(): Long = if (current.get() == null) 0 else System.nanoTime()

    fun timestamp(start: Long, allocationNanos: Long) {
        if (start != 0L) current.get()?.let {
            it.timestampNanos += System.nanoTime() - start
            it.timestampAllocationNanos += allocationNanos
            it.timestamps++
        }
    }

    fun prewrite(start: Long) {
        if (start != 0L) current.get()?.let { it.prewriteNanos += System.nanoTime() - start; it.prewrites++ }
    }

    fun commit(start: Long) {
        if (start != 0L) current.get()?.let { it.commitNanos += System.nanoTime() - start; it.commits++ }
    }

    fun catalog(action: String, start: Long = 0) {
        current.get()?.let {
            when (action) {
                "acquire" -> if (start != 0L) {
                    it.catalogAcquireNanos += System.nanoTime() - start
                    it.catalogAcquisitions++
                }
                "drain" -> if (start != 0L) {
                    it.catalogDrainNanos += System.nanoTime() - start
                    it.catalogDrains++
                }
            }
        }
    }

    fun catalogFallback() { current.get()?.let { it.catalogFallbackGets++ } }
    fun catalogLoaderBusy() { current.get()?.let { it.catalogLoaderBusy++ } }

    fun onePhaseAttempt() { current.get()?.let { it.onePhaseAttempts++ } }
    fun onePhaseResult(committed: Boolean) {
        current.get()?.let { if (committed) it.onePhaseCommits++ else it.onePhaseFallbacks++ }
    }
    fun rejection(reason: String) { current.get()?.rejection = reason }
    fun status(status: Int) { current.get()?.status = status }
}
