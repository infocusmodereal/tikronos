package tikronos

import tikronos.diagnostics.RequestEvents
import tikronos.diagnostics.RequestEvent
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.withCharset
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.httpMethod
import io.ktor.server.request.receiveChannel
import io.ktor.server.request.uri
import io.ktor.server.response.respondBytes
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.UUID
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import tikronos.config.HttpLimits
import tikronos.protocol.DynamoRequest
import tikronos.protocol.DynamoResponse
import tikronos.protocol.RequestHeaders

/** HTTP admission and body reads never occupy a blocking storage thread. */
internal class HttpApi(
    private val api: Dynamo,
    private val storage: CoroutineDispatcher,
    private val limits: HttpLimits,
) {
    private val slots = Semaphore(limits.maxRequests)
    private val executions = Semaphore(limits.storageWorkers)
    private val bodies = RequestBodies()
    @Volatile private var draining = false

    suspend fun drain(timeoutMs: Long) {
        draining = true
        withTimeoutOrNull(timeoutMs) { repeat(limits.maxRequests) { slots.acquire() } }
    }

    suspend fun handle(call: ApplicationCall) {
        call.response.headers.append(HttpHeaders.Date,
            HTTP_DATE.format(Instant.now()))
        val uri = call.request.uri
        val path = uri.substringBefore('?')
        when {
            path == "/healthz" -> health(call)
            path != "/" -> plain(call, HttpStatusCode.NotFound, "404 page not found\n")
            call.request.httpMethod != HttpMethod.Post -> {
                call.response.headers.append(HttpHeaders.Allow, "POST")
                plain(call, HttpStatusCode.MethodNotAllowed, "Method Not Allowed\n")
            }
            else -> request(call, uri.contains('?'))
        }
    }

    private suspend fun request(call: ApplicationCall, hasQuery: Boolean) {
        call.response.headers.append("x-amzn-RequestId", UUID.randomUUID().toString())
        val headers = RequestHeaders(call.request.headers.names().map { it.lowercase(Locale.ROOT) }
            .distinct().associateWith { call.request.headers.getAll(it).orEmpty() })
        val event = RequestEvents.begin(headers.first("X-Amz-Target"))
        try {
            bodies.validateLength(call.request.headers[HttpHeaders.ContentLength])
            api.validateHeaders(headers)
            if (draining) {
                event?.rejection = "draining"
                throw throttled()
            }
            if (!slots.tryAcquire()) {
                event?.rejection = "admission"
                throw throttled()
            }
            try {
                bodies.consume(call.receiveChannel()) { body ->
                    respond(call, execute(DynamoRequest(headers, body, hasQuery), event))
                }
            } finally {
                slots.release()
            }
        } catch (error: ApiError) {
            event?.status = error.status
            if (error.message == "Body budget is full") event?.rejection = "body_budget"
            call.response.headers.append(HttpHeaders.Connection, "close")
            respond(call, error(HttpStatusCode.fromValue(error.status), error.code, error.message.orEmpty()))
        } finally {
            event?.commit()
        }
    }

    private suspend fun execute(
        input: DynamoRequest, event: RequestEvent?,
    ): DynamoResponse {
        var acquired = false
        return try {
            val queued = if (event == null) 0 else System.nanoTime()
            val ready = withTimeoutOrNull(QUEUE_TIMEOUT_MS) {
                executions.acquire()
                acquired = true
                true
            }
            event?.queueNanos = System.nanoTime() - queued
            if (ready != true) {
                event?.rejection = "queue"
                throw throttled()
            }
            withContext(storage) { RequestEvents.within(event) { api.handle(input) } }
        } finally {
            if (acquired) executions.release()
        }
    }

    private fun throttled() =
        ApiError("ProvisionedThroughputExceededException", "Request admission is full or draining")

    private suspend fun health(call: ApplicationCall) {
        if (call.request.httpMethod in setOf(HttpMethod.Get, HttpMethod.Head)) {
            plain(call, HttpStatusCode.OK, "ok\n")
        } else {
            call.response.headers.append(HttpHeaders.Allow, "GET, HEAD")
            plain(call, HttpStatusCode.MethodNotAllowed, "Method Not Allowed\n")
        }
    }

    private suspend fun plain(call: ApplicationCall, status: HttpStatusCode, text: String) =
        call.respondBytes(text.encodeToByteArray(), ContentType.Text.Plain.withCharset(Charsets.UTF_8), status)

    private suspend fun respond(call: ApplicationCall, response: DynamoResponse) =
        call.respondBytes(response.body, ContentType.parse("application/x-amz-json-1.0"),
            HttpStatusCode.fromValue(response.status))

    private fun error(status: HttpStatusCode, code: String, message: String) =
        DynamoResponse(status.value, json.writeValueAsBytes(obj("__type" to code, "message" to message)))

    companion object {
        private const val QUEUE_TIMEOUT_MS = 5000L
        private val HTTP_DATE = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
            .withZone(ZoneOffset.UTC)
    }
}
