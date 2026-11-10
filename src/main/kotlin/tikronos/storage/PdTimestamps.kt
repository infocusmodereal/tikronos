package tikronos.storage

import org.tikv.common.PDClient
import org.tikv.common.TikronosPdAccess
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.operation.ErrorHandler
import org.tikv.common.operation.PDErrorHandler
import org.tikv.common.pd.PDError
import org.tikv.common.policy.RetryMaxMs
import org.tikv.common.util.BackOffer
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.PDGrpc
import org.tikv.kvproto.Pdpb
import tikronos.diagnostics.TimestampEvent

/** TimestampOracle serializes allocation; SDK discovery and bounded retry policy remain authoritative. */
internal class PdTimestamps(private val pd: PDClient) : AutoCloseable {
    private val lifecycle = Any()
    private var stream: TsoStream? = null
    @Volatile private var closed = false
    private var last = 0L

    fun allocate(count: Int): List<TiTimestamp> {
        require(count > 0)
        val event = TimestampEvent.begin(count)
        return try {
            val budget = ConcreteBackOffer.newTsoBackOff(pd.clusterId)
            val request = Pdpb.TsoRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(pd.clusterId))
                .setCount(count).build()
            val response = RetryMaxMs.Builder<Pdpb.TsoResponse>(budget).create(handler())
                .callWithRetry({ exchange(request, event) }, PDGrpc.getTsoMethod().fullMethodName, budget)
            decode(response, count).also { event?.successful = true }
        } finally { event?.commit() }
    }

    @Suppress("TooGenericExceptionCaught") // Retire every failed exchange before the SDK decides whether to retry.
    private fun exchange(request: Pdpb.TsoRequest, event: TimestampEvent?): Pdpb.TsoResponse {
        val started = if (event == null) 0 else System.nanoTime()
        if (event != null) event.attempts++
        return try {
            current().exchange(request, pd.timeout)
        } catch (error: Exception) {
            invalidate()
            throw error
        } finally {
            if (event != null) event.rpcNanos += System.nanoTime() - started
        }
    }

    private fun current(): TsoStream = synchronized(lifecycle) {
        check(!closed) { "Timestamp allocator closed" }
        val channel = TikronosPdAccess.channel(pd)
        stream?.takeIf { it.channel === channel && !it.closed } ?: run {
            stream?.close()
            TsoStream.open(channel).also { stream = it }
        }
    }

    private fun handler(): ErrorHandler<Pdpb.TsoResponse> {
        val delegate = PDErrorHandler<Pdpb.TsoResponse>(
            { if (it.header.hasError()) PDError.buildFromPdpbError(it.header.error) else null }, pd,
        )
        return object : ErrorHandler<Pdpb.TsoResponse> {
            override fun handleResponseError(budget: BackOffer, response: Pdpb.TsoResponse?): Boolean {
                if (response == null || response.header.hasError()) invalidate()
                return delegate.handleResponseError(budget, response)
            }

            override fun handleRequestError(budget: BackOffer, error: Exception): Boolean {
                if (closed) throw error
                return delegate.handleRequestError(budget, error)
            }
        }
    }

    private fun decode(response: Pdpb.TsoResponse, count: Int): List<TiTimestamp> =
        try {
            timestampBatch(response, count, pd.clusterId, last).also { last = it.last().version }
        } catch (error: IllegalStateException) {
            invalidate()
            throw error
        }

    private fun invalidate() = synchronized(lifecycle) {
        stream?.close()
        stream = null
    }

    override fun close() = synchronized(lifecycle) {
        closed = true
        invalidate()
    }
}
