package tikronos

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.node.ObjectNode
import com.sun.net.httpserver.HttpExchange
import tikronos.model.ConditionFailed
import tikronos.storage.Storage
import tikronos.storage.ContentionLimitExceeded
import java.util.UUID
import java.net.HttpURLConnection
import java.util.logging.Level
import java.util.logging.Logger

internal class Dynamo(db: Storage, private val signature: Signature?) {
    private val tables = Tables(db)
    private val items = Items(db, tables)

    @Suppress("TooGenericExceptionCaught") // HTTP boundary: log unexpected failures and return an SDK-readable error.
    fun handle(exchange: HttpExchange) {
        exchange.responseHeaders.set("x-amzn-RequestId", UUID.randomUUID().toString())
        exchange.responseHeaders.set("Content-Type", "application/x-amz-json-1.0")
        val response = try {
            HttpURLConnection.HTTP_OK to request(exchange)
        } catch (_: ConditionFailed) {
            HttpURLConnection.HTTP_BAD_REQUEST to
                obj("__type" to "ConditionalCheckFailedException", "message" to "Condition failed")
        } catch (_: ContentionLimitExceeded) {
            HttpURLConnection.HTTP_BAD_REQUEST to obj("__type" to "ProvisionedThroughputExceededException",
                "message" to "Concurrent writes exceeded retry budget")
        } catch (e: ApiError) {
            e.status to obj("__type" to e.code, "message" to e.message.orEmpty())
        } catch (e: Exception) {
            logger.log(Level.WARNING, "DynamoDB request failed", e)
            HttpURLConnection.HTTP_INTERNAL_ERROR to
                obj("__type" to "InternalServerError", "message" to "Storage request failed")
        }
        val bytes = json.writeValueAsBytes(response.second)
        exchange.sendResponseHeaders(response.first, bytes.size.toLong())
        exchange.responseBody.write(bytes)
    }

    private fun request(exchange: HttpExchange): ObjectNode {
        if (exchange.requestURI.rawQuery != null) invalid("Unexpected query parameters")
        val body = exchange.requestBody.readNBytes(MAX_BODY + 1)
        if (body.size > MAX_BODY) {
            throw ApiError("RequestEntityTooLarge", "Request too large", HttpURLConnection.HTTP_ENTITY_TOO_LARGE)
        }
        signature?.verify(exchange.requestHeaders, body)
        val target = exchange.requestHeaders.getFirst("X-Amz-Target").orEmpty()
        if (!target.startsWith(PREFIX)) unknown()
        val req = try {
            json.readTree(body) as? ObjectNode ?: invalid("Expected a JSON object")
        } catch (_: JsonProcessingException) {
            throw ApiError("SerializationException", "Invalid JSON")
        }
        return dispatch(target.removePrefix(PREFIX), req, exchange)
    }

    private fun dispatch(operation: String, req: ObjectNode, exchange: HttpExchange): ObjectNode =
        when (operation) {
            "DescribeEndpoints" -> {
                req.fieldsOnly()
                obj("Endpoints" to listOf(obj("Address" to exchange.requestHeaders.getFirst("Host"),
                    "CachePeriodInMinutes" to CACHE_MINUTES)))
            }
            "CreateTable" -> tables.create(req)
            "DescribeTable" -> tables.describe(req)
            "DeleteTable" -> tables.delete(req)
            "ListTables" -> tables.list(req)
            "GetItem" -> items.get(req)
            "PutItem" -> items.write(req, put = true)
            "DeleteItem" -> items.write(req, put = false)
            else -> unknown()
        }

    companion object {
        private const val CACHE_MINUTES = 60
        private const val PREFIX = "DynamoDB_20120810."
        private const val MAX_BODY = 16 * 1024 * 1024
        private val logger = Logger.getLogger(Dynamo::class.java.name)
        private fun unknown(): Nothing = throw ApiError("UnknownOperationException", "Unsupported operation")
    }
}
