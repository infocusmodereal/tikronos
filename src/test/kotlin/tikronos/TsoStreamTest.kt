package tikronos

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Timeout
import org.tikv.common.exception.GrpcException
import org.tikv.common.policy.RetryMaxMs
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.PDGrpc
import org.tikv.kvproto.Pdpb
import org.tikv.shade.io.grpc.Channel
import org.tikv.shade.io.grpc.Status
import org.tikv.shade.io.grpc.StatusRuntimeException
import org.tikv.shade.io.grpc.inprocess.InProcessChannelBuilder
import org.tikv.shade.io.grpc.inprocess.InProcessServerBuilder
import org.tikv.shade.io.grpc.stub.ServerCallStreamObserver
import org.tikv.shade.io.grpc.stub.StreamObserver
import tikronos.storage.TsoStream

@Timeout(10)
class TsoStreamTest {
    @Test
    fun `successive allocations reuse a stream without closing the shared channel`() {
        val opened = AtomicInteger()
        withServer({ responses ->
            opened.incrementAndGet()
            observer { responses.onNext(response(it.count.toLong())) }
        }) { channel ->
            TsoStream.open(channel).use { stream ->
                assertEquals(2L, stream.exchange(request(2), 1000).timestamp.logical)
                assertEquals(3L, stream.exchange(request(3), 1000).timestamp.logical)
                assertEquals(1, opened.get())
            }
            TsoStream.open(channel).use { stream ->
                assertEquals(4L, stream.exchange(request(4), 1000).timestamp.logical)
            }
            assertEquals(2, opened.get())
        }
    }

    @Test
    fun `unsolicited response retires an idle stream`() {
        val cancelled = CountDownLatch(1)
        withServer({ responses ->
            (responses as ServerCallStreamObserver<Pdpb.TsoResponse>).setOnCancelHandler {
                cancelled.countDown()
            }
            observer {
                responses.onNext(response(10))
                responses.onNext(response(20))
            }
        }) { channel ->
            TsoStream.open(channel).use { stream ->
                assertEquals(10L, stream.exchange(request(), 1000).timestamp.logical)
                assertTrue(cancelled.await(1, TimeUnit.SECONDS))
                assertTrue(stream.closed)
                val error = assertFailsWith<StatusRuntimeException> { stream.exchange(request(), 1000) }
                assertEquals(Status.Code.INTERNAL, error.status.code)
            }
        }
    }

    @Test
    fun `timeout retires the stream and a late reply cannot satisfy a new stream`() {
        val responses = CopyOnWriteArrayList<StreamObserver<Pdpb.TsoResponse>>()
        val freshRequest = CountDownLatch(1)
        withServer({ response ->
            responses.add(response)
            observer { if (responses.size == 2) freshRequest.countDown() }
        }) { channel ->
            TsoStream.open(channel).use { expired ->
                val error = assertFailsWith<StatusRuntimeException> { expired.exchange(request(), 20) }
                assertEquals(Status.Code.DEADLINE_EXCEEDED, error.status.code)
                assertTrue(expired.closed)
                assertFailsWith<StatusRuntimeException> { expired.exchange(request(), 1000) }
                TsoStream.open(channel).use { fresh ->
                    Executors.newSingleThreadExecutor().use { pool ->
                        val result = pool.submit<Pdpb.TsoResponse> { fresh.exchange(request(), 2000) }
                        assertTrue(freshRequest.await(1, TimeUnit.SECONDS))
                        // gRPC rejects the abandoned server response; it cannot complete the new call.
                        val late = assertFailsWith<StatusRuntimeException> {
                            responses.first().onNext(response(10))
                        }
                        assertEquals(Status.Code.CANCELLED, late.status.code)
                        assertFailsWith<TimeoutException> { result.get(20, TimeUnit.MILLISECONDS) }
                        responses.last().onNext(response(20))
                        assertEquals(20L, result.get(1, TimeUnit.SECONDS).timestamp.logical)
                    }
                }
            }
        }
    }

    @Test
    fun `remote completion and local close release blocked allocation callers`() {
        withServer({ responses -> observer { responses.onCompleted() } }) { channel ->
            TsoStream.open(channel).use { stream ->
                val error = assertFailsWith<StatusRuntimeException> { stream.exchange(request(), 1000) }
                assertEquals(Status.Code.UNAVAILABLE, error.status.code)
            }
        }
        val received = CountDownLatch(1)
        withServer({ observer { received.countDown() } }) { channel ->
            TsoStream.open(channel).use { stream ->
                Executors.newSingleThreadExecutor().use { pool ->
                    val result = pool.submit<Status.Code> {
                        assertFailsWith<StatusRuntimeException> { stream.exchange(request(), 5000) }.status.code
                    }
                    assertTrue(received.await(1, TimeUnit.SECONDS))
                    stream.close()
                    assertEquals(Status.Code.CANCELLED, result.get(1, TimeUnit.SECONDS))
                }
            }
        }
    }

    @Test
    fun `interruption bypasses SDK retry and preserves the caller interrupt`() {
        val received = CountDownLatch(1)
        val interrupted = AtomicBoolean()
        val done = CompletableFuture<Unit>()
        withServer({ observer { received.countDown() } }) { channel ->
            TsoStream.open(channel).use { stream ->
                val thread = Thread {
                    try {
                        val budget = ConcreteBackOffer.newTsoBackOff(0)
                        val error = assertFailsWith<GrpcException> {
                            RetryMaxMs.Builder<Pdpb.TsoResponse>(budget).create(null).callWithRetry(
                                { stream.exchange(request(), 5000) }, PDGrpc.getTsoMethod().fullMethodName, budget,
                            )
                        }
                        assertEquals(Status.Code.CANCELLED, Status.fromThrowable(error).code)
                        interrupted.set(Thread.currentThread().isInterrupted)
                        done.complete(Unit)
                    } catch (failure: AssertionError) { done.completeExceptionally(failure) }
                }
                thread.start()
                try {
                    assertTrue(received.await(1, TimeUnit.SECONDS))
                    thread.interrupt()
                    done.get(1, TimeUnit.SECONDS)
                    assertTrue(interrupted.get())
                    assertTrue(stream.closed)
                } finally { thread.interrupt(); thread.join(1000) }
            }
        }
    }

    private fun withServer(
        connect: (StreamObserver<Pdpb.TsoResponse>) -> StreamObserver<Pdpb.TsoRequest>,
        action: (Channel) -> Unit,
    ) {
        val name = InProcessServerBuilder.generateName()
        val service = object : PDGrpc.PDImplBase() {
            override fun tso(responseObserver: StreamObserver<Pdpb.TsoResponse>) = connect(responseObserver)
        }
        val server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start()
        val channel = InProcessChannelBuilder.forName(name).build()
        try { action(channel) } finally {
            channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS)
            server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS)
        }
    }

    private fun observer(receive: (Pdpb.TsoRequest) -> Unit) = object : StreamObserver<Pdpb.TsoRequest> {
        override fun onNext(value: Pdpb.TsoRequest) = receive(value)
        override fun onError(error: Throwable) = Unit
        override fun onCompleted() = Unit
    }

    private fun request(count: Int = 1) = Pdpb.TsoRequest.newBuilder().setCount(count).build()
    private fun response(logical: Long) = Pdpb.TsoResponse.newBuilder()
        .setTimestamp(Pdpb.Timestamp.newBuilder().setPhysical(1).setLogical(logical)).build()
}
