package tikronos.storage

import tikronos.diagnostics.RequestEvents
import tikronos.diagnostics.TimestampEvent
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.tikv.common.PDClient
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.operation.PDErrorHandler
import org.tikv.common.operation.ErrorHandler
import org.tikv.common.pd.PDError
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.common.util.BackOffer
import org.tikv.kvproto.PDGrpc
import org.tikv.kvproto.Pdpb

/** Batch only callers already waiting before PD allocation; never cache future timestamps. */
internal class TimestampOracle(private val allocate: (Int) -> List<TiTimestamp>) {
    constructor(pd: PDClient) : this({ count -> allocate(pd, count) })

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

    companion object {
        private const val MAX_BATCH = 128
        private const val LOGICAL_BITS = 18

        private fun allocate(pd: PDClient, count: Int): List<TiTimestamp> {
            val event = TimestampEvent.begin(count)
            return try {
                request(pd, count, event).also { event?.successful = true }
            } finally { event?.commit() }
        }

        private fun request(pd: PDClient, count: Int, event: TimestampEvent?): List<TiTimestamp> {
            val request = Pdpb.TsoRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(pd.clusterId))
                .setCount(count).build()
            val handler = PDErrorHandler<Pdpb.TsoResponse>(
                { if (it.header.hasError()) PDError.buildFromPdpbError(it.header.error) else null }, pd,
            )
            var rpcStarted = 0L
            val measured = if (event == null) handler else object : ErrorHandler<Pdpb.TsoResponse> {
                override fun handleResponseError(budget: BackOffer, response: Pdpb.TsoResponse?): Boolean {
                    if (rpcStarted != 0L) event.rpcNanos += System.nanoTime() - rpcStarted
                    rpcStarted = 0
                    return handler.handleResponseError(budget, response)
                }

                override fun handleRequestError(budget: BackOffer, error: Exception): Boolean {
                    if (rpcStarted != 0L) event.rpcNanos += System.nanoTime() - rpcStarted
                    rpcStarted = 0
                    return handler.handleRequestError(budget, error)
                }
            }
            val response = try { pd.callWithRetry(
                ConcreteBackOffer.newTsoBackOff(pd.clusterId), PDGrpc.getTsoMethod(),
                {
                    if (event != null) { event.attempts++; rpcStarted = System.nanoTime() }
                    request
                }, measured,
            ) } finally {
                if (event != null && rpcStarted != 0L) event.rpcNanos += System.nanoTime() - rpcStarted
            }
            check(response.count == count) { "PD returned an unexpected timestamp count" }
            val timestamp = response.timestamp
            check(timestamp.suffixBits in 0 until LOGICAL_BITS)
            val step = 1L shl timestamp.suffixBits
            val first = timestamp.logical - (count - 1) * step
            check(first >= 0 && timestamp.logical < (1L shl LOGICAL_BITS))
            return List(count) { TiTimestamp(timestamp.physical, first + it * step) }
        }
    }
}
