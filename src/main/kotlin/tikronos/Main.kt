package tikronos

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.URI
import tikronos.storage.Storage
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession

private fun HttpExchange.reply(status: Int, text: String, allow: String? = null) {
    val bytes = text.toByteArray(Charsets.UTF_8)
    responseHeaders.set("Content-Type", "text/plain; charset=utf-8")
    if (allow != null) responseHeaders.set("Allow", allow)
    if (requestMethod == "HEAD") {
        responseHeaders.set("Content-Length", bytes.size.toString())
        sendResponseHeaders(status, -1)
    } else {
        sendResponseHeaders(status, bytes.size.toLong())
        responseBody.write(bytes)
    }
}

private fun addressArgument(args: Array<String>): String = when {
    args.isEmpty() -> "127.0.0.1:8000"
    args.size == 2 && args[0] in setOf("-listen", "--listen") -> args[1]
    args.size == 1 && args[0].substringBefore('=') in setOf("-listen", "--listen")
        && '=' in args[0] -> args[0].substringAfter('=')
    else -> error("Usage: tikronos [-listen host:port]")
}

private fun listenAddress(args: Array<String>): InetSocketAddress {
    val address = addressArgument(args)
    val wildcard = address.startsWith(':')
    val endpoint = URI(if (wildcard) "http://localhost$address" else "http://$address")
    require(endpoint.host != null && endpoint.port in 0..MAX_PORT && endpoint.rawPath.isEmpty()
        && endpoint.rawQuery == null && endpoint.rawFragment == null && endpoint.rawUserInfo == null) {
        "Expected host:port"
    }
    return if (wildcard) InetSocketAddress(endpoint.port)
        else InetSocketAddress(endpoint.host, endpoint.port)
}

private fun signature(): Signature? {
    val access = System.getenv("TIKRONOS_ACCESS_KEY")
    val secret = System.getenv("TIKRONOS_SECRET_KEY")
    require((access == null && secret == null) || (!access.isNullOrBlank() && !secret.isNullOrBlank())) {
        "Set both TIKRONOS_ACCESS_KEY and TIKRONOS_SECRET_KEY"
    }
    return access?.let { Signature(it, requireNotNull(secret), System.getenv("TIKRONOS_REGION") ?: "us-east-1") }
}

fun main(args: Array<String>) {
    val bind = listenAddress(args)
    val signature = signature()
    require(signature != null || bind.address?.isLoopbackAddress == true) {
        "Credentials required on non-loopback listeners"
    }
    val session = TiSession.create(TiConfiguration.createDefault(System.getenv("TIKRONOS_PD") ?: "127.0.0.1:2379"))
    val storage = Storage(session)
    val api = Dynamo(storage, signature)
    val maintenance = Maintenance(storage)
    val server = HttpServer.create(bind, 0)
    val workers = Executors.newFixedThreadPool(WORKERS)
    server.executor = workers
    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop(1)
        workers.shutdown()
        try {
            workers.awaitTermination(DRAIN_SECONDS, TimeUnit.SECONDS)
        } finally {
            try {
                maintenance.close()
            } finally {
                session.close()
            }
        }
    })
    server.createContext("/") { exchange -> exchange.use { route(it, api) } }
    server.start()
    maintenance.start()
}

private fun route(exchange: HttpExchange, api: Dynamo) {
    with(exchange) {
        when (requestURI.rawPath) {
            "/healthz" -> when (requestMethod) {
                "GET", "HEAD" -> reply(HttpURLConnection.HTTP_OK, "ok\n")
                else -> reply(HttpURLConnection.HTTP_BAD_METHOD, "Method Not Allowed\n", "GET, HEAD")
            }
            "/" -> when (requestMethod) {
                "POST" -> api.handle(this)
                else -> reply(HttpURLConnection.HTTP_BAD_METHOD, "Method Not Allowed\n", "POST")
            }
            else -> reply(HttpURLConnection.HTTP_NOT_FOUND, "404 page not found\n")
        }
    }
}

private const val MAX_PORT = 65535
private const val DRAIN_SECONDS = 10L
private const val WORKERS = 8
