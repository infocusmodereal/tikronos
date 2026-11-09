package tikronos.storage

import tikronos.diagnostics.RequestEvents
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.tikv.common.meta.TiTimestamp

/** Batch only callers already waiting before PD allocation; never cache future timestamps. */
internal class TimestampOracle(private val allocate: (Int) -> List<TiTimestamp>) {
    private class Ticket(var result: Result<TiTimestamp>? = null, var allocationNanos: Long = 0)
    private val lock = ReentrantLock()
    private val changed = lock.newCondition()
    private val pending = ArrayDeque<Ticket>()
    private var allocating = false

    fun next(): TiTimestamp {
        val started = RequestEvents.start()
        var allocationNanos = 0L
        return try {
            val ticket = allocateNext()
            allocationNanos = ticket.allocationNanos
            checkNotNull(ticket.result).getOrThrow()
        } finally { RequestEvents.timestamp(started, allocationNanos) }
    }

    private fun allocateNext(): Ticket {
        val ticket = Ticket()
        lock.withLock { pending.addLast(ticket) }
        while (true) {
            val batch = lock.withLock {
                while (ticket.result == null && allocating) {
                    try {
                        changed.await()
                    } catch (interrupted: InterruptedException) {
                        pending.remove(ticket)
                        throw interrupted
                    }
                }
                if (ticket.result != null) return ticket
                allocating = true
                pending.take(MAX_BATCH).also { repeat(it.size) { pending.removeFirst() } }
            }
            // The elected caller returns after its batch instead of draining indefinitely.
            val started = System.nanoTime()
            val result = runCatching {
                allocate(batch.size).also { check(it.size == batch.size) }
            }
            val elapsed = System.nanoTime() - started
            lock.withLock {
                batch.forEachIndexed { index, waiter ->
                    waiter.allocationNanos = elapsed
                    waiter.result = result.map { it[index] }
                }
                allocating = false
                changed.signalAll()
            }
        }
    }

    companion object { private const val MAX_BATCH = 128 }
}
