package tikronos.storage

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.tikv.kvproto.PDGrpc
import org.tikv.kvproto.Pdpb
import org.tikv.shade.io.grpc.CallOptions
import org.tikv.shade.io.grpc.Channel
import org.tikv.shade.io.grpc.ClientCall
import org.tikv.shade.io.grpc.Metadata
import org.tikv.shade.io.grpc.Status

/** One outstanding allocation per stream. A failed stream is never reused for another batch. */
internal class TsoStream private constructor(val channel: Channel) : AutoCloseable {
    private val lock = Any()
    private val call = channel.newCall(PDGrpc.getTsoMethod(), CallOptions.DEFAULT)
    private var pending: CompletableFuture<Pdpb.TsoResponse>? = null
    private var failure: RuntimeException? = null

    val closed: Boolean get() = synchronized(lock) { failure != null }

    @Suppress("ThrowsCount") // Distinct remote failure, response timeout and caller interruption.
    fun exchange(request: Pdpb.TsoRequest, timeoutMs: Long): Pdpb.TsoResponse {
        val result = synchronized(lock) {
            failure?.let { throw Status.fromThrowable(it).withCause(it).asRuntimeException() }
            check(pending == null) { "Only one timestamp batch may be in flight" }
            CompletableFuture<Pdpb.TsoResponse>().also {
                pending = it
                call.sendMessage(request)
            }
        }
        return try {
            result.get(timeoutMs, TimeUnit.MILLISECONDS)
        } catch (error: ExecutionException) {
            throw Status.fromThrowable(error).withCause(error).asRuntimeException()
        } catch (error: TimeoutException) {
            val timeout = Status.DEADLINE_EXCEEDED.withDescription("PD timestamp response timed out")
                .withCause(error).asRuntimeException()
            fail(timeout, cancel = true)
            throw timeout
        } catch (error: InterruptedException) {
            val cancelled = Status.CANCELLED.withDescription("Interrupted awaiting PD timestamp")
                .withCause(error).asRuntimeException()
            fail(cancelled, cancel = true)
            Thread.currentThread().interrupt()
            throw cancelled
        }
    }

    private fun start() = synchronized(lock) {
        call.start(object : ClientCall.Listener<Pdpb.TsoResponse>() {
            override fun onMessage(message: Pdpb.TsoResponse) {
                synchronized(lock) {
                    val result = pending
                    pending = null
                    if (result == null) {
                        fail(Status.INTERNAL.withDescription("Unexpected PD timestamp response")
                            .asRuntimeException(), cancel = true)
                    } else {
                        result.complete(message)
                        call.request(1)
                    }
                }
            }

            override fun onClose(status: Status, trailers: Metadata) {
                val error = if (status.isOk) Status.UNAVAILABLE.withDescription("PD timestamp stream closed")
                    else status
                fail(error.asRuntimeException(), cancel = false)
            }
        }, Metadata())
        if (failure == null) call.request(1)
    }

    private fun fail(error: RuntimeException, cancel: Boolean) = synchronized(lock) {
        if (failure == null) {
            failure = error
            pending?.completeExceptionally(error)
            pending = null
            if (cancel) call.cancel("Closing timestamp stream", error)
        }
    }

    override fun close() = fail(
        Status.CANCELLED.withDescription("Timestamp allocator closed").asRuntimeException(), cancel = true,
    )

    companion object {
        fun open(channel: Channel): TsoStream = TsoStream(channel).also { it.start() }
    }
}
