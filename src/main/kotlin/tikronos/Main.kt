package tikronos

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI

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

fun main(args: Array<String>) {
    val address = when {
        args.isEmpty() -> "127.0.0.1:8000"
        args.size == 2 && args[0] in setOf("-listen", "--listen") -> args[1]
        args.size == 1 && args[0].substringBefore('=') in setOf("-listen", "--listen")
            && '=' in args[0] -> args[0].substringAfter('=')
        else -> error("Usage: tikronos [-listen host:port]")
    }
    val wildcard = address.startsWith(':')
    val endpoint = URI(if (wildcard) "http://localhost$address" else "http://$address")
    require(endpoint.host != null && endpoint.port in 0..65535 && endpoint.rawPath.isEmpty()
        && endpoint.rawQuery == null && endpoint.rawFragment == null && endpoint.rawUserInfo == null) {
        "Expected host:port"
    }
    val bind = if (wildcard) InetSocketAddress(endpoint.port)
        else InetSocketAddress(endpoint.host, endpoint.port)
    val server = HttpServer.create(bind, 0)
    server.createContext("/") { exchange ->
        exchange.use {
            when (it.requestURI.rawPath) {
                "/healthz" -> when (it.requestMethod) {
                    "GET", "HEAD" -> it.reply(200, "ok\n")
                    else -> it.reply(405, "Method Not Allowed\n", "GET, HEAD")
                }
                "/" -> when (it.requestMethod) {
                    "POST" -> it.reply(501, "DynamoDB operations are not implemented yet\n")
                    else -> it.reply(405, "Method Not Allowed\n", "POST")
                }
                else -> it.reply(404, "404 page not found\n")
            }
        }
    }
    server.start()
}
