package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import kotlinx.coroutines.sync.Semaphore
import tikronos.diagnostics.RequestEvents
import tikronos.model.CapacityUnits
import tikronos.model.WriteResult
import tikronos.protocol.BatchWrite
import tikronos.storage.CommitUnknown
import tikronos.storage.ContentionLimitExceeded

internal fun executeBatch(
    operations: List<BatchWrite>,
    capacity: Capacity,
    spare: Semaphore? = null,
    checkActive: () -> Unit = {},
    commit: (BatchWrite) -> WriteResult,
): ObjectNode {
    val unprocessed = obj()
    val units = mutableMapOf<String, CapacityUnits>()
    var completed = 0
    fun attempt(op: BatchWrite): WriteResult? = try {
        commit(op)
    } catch (_: ContentionLimitExceeded) {
        null
    }
    fun account(op: BatchWrite, result: WriteResult?) {
        if (result == null) {
            unprocessed.withArray(op.table.name).add(op.request)
        } else {
            completed++
            if (capacity.requested)
                units.merge(op.table.name, writeCapacity(result.table, result.old, result.new), CapacityUnits::plus)
        }
    }
    val rows = operations.iterator()
    while (rows.hasNext()) {
        checkActive()
        if (Thread.currentThread().isInterrupted) throw InterruptedException("Batch write interrupted")
        val first = rows.next()
        if (rows.hasNext() && spare != null && spare.tryAcquire()) {
            val second = rows.next()
            val pair = try {
                runPair({ attempt(first) }, { attempt(second) })
            } finally {
                spare.release()
            }
            account(first, pair.first)
            account(second, pair.second)
        } else {
            account(first, attempt(first))
        }
    }
    if (completed == 0) throw ContentionLimitExceeded()
    return obj("UnprocessedItems" to unprocessed).also { capacity.batchUsage(it, units) }
}

/** Join both durable decisions, even on interruption or failure. Only the caller builds the response. */
private fun <T> runPair(first: () -> T, second: () -> T): Pair<T, T> {
    val phases = RequestEvents.child()
    val future = CompletableFuture.supplyAsync({ runCatching { RequestEvents.within(phases, second) } }, virtual)
    val caller = runCatching(first)
    val helper = future.join()
    RequestEvents.include(phases)
    // An unknown outcome takes precedence over a concurrent, otherwise reportable client error.
    val errors = listOfNotNull(caller.exceptionOrNull(), helper.exceptionOrNull())
    if (errors.isNotEmpty()) {
        val primary = errors.firstOrNull { it is CommitUnknown } ?: errors.first()
        errors.filter { it !== primary }.forEach(primary::addSuppressed)
        throw primary
    }
    return caller.getOrThrow() to helper.getOrThrow()
}

// Helpers never queue behind a parent occupying the storage dispatcher.
private val virtual = Executor { task -> Thread.startVirtualThread(task) }
