package tikronos.diagnostics

import jdk.jfr.EventType

/** The context is installed only inside the blocking dispatcher, never across a coroutine suspension. */
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

    fun onePhaseAttempt() { current.get()?.let { it.onePhaseAttempts++ } }
    fun onePhaseResult(committed: Boolean) {
        current.get()?.let { if (committed) it.onePhaseCommits++ else it.onePhaseFallbacks++ }
    }
    fun rejection(reason: String) { current.get()?.rejection = reason }
    fun status(status: Int) { current.get()?.status = status }
}
