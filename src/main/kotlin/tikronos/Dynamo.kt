package tikronos

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Semaphore
import tikronos.protocol.Signature

import tikronos.diagnostics.RequestEvents
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.protocol.DynamoRequest
import tikronos.protocol.DynamoResponse
import tikronos.protocol.RequestHeaders
import java.net.HttpURLConnection
import java.util.logging.Level
import java.util.logging.Logger
import tikronos.codec.wireItem
import tikronos.model.ConditionFailed
import tikronos.storage.CommitUnknown
import tikronos.storage.ContentionLimitExceeded
import tikronos.storage.Storage

internal class Dynamo(db: Storage, private val signature: Signature?, executions: Semaphore) {
    private val tables = Tables(db)
    private val catalog = Catalog(db)
    private val streams = StreamReads(db)
    private val ttl = Ttl(db)
    private val items = Items(db, tables)
    private val reads = Reads(db, tables)
    private val batches = Batches(db, tables, executions)

    fun validateHeaders(headers: RequestHeaders) = signature?.validateHeaders(headers)

    @Suppress(
        "TooGenericExceptionCaught"
    ) // HTTP boundary: log unexpected failures and return an SDK-readable error.
    fun handle(input: DynamoRequest, checkActive: () -> Unit): DynamoResponse {
        val response =
            try {
                HttpURLConnection.HTTP_OK to request(input, checkActive)
            } catch (e: ConditionFailed) {
                HttpURLConnection.HTTP_BAD_REQUEST to
                    obj(
                            "__type" to "ConditionalCheckFailedException",
                            "message" to "Condition failed",
                        )
                        .apply {
                            e.old?.let { set<ObjectNode>("Item", json.valueToTree(wireItem(it))) }
                        }
            } catch (_: ContentionLimitExceeded) {
                RequestEvents.rejection("contention")
                HttpURLConnection.HTTP_BAD_REQUEST to
                    obj(
                        "__type" to "ProvisionedThroughputExceededException",
                        "message" to "Concurrent writes exceeded retry budget",
                    )
            } catch (e: ApiError) {
                e.status to obj("__type" to e.code, "message" to e.message.orEmpty())
            } catch (e: CommitUnknown) {
                RequestEvents.rejection("unknown_outcome")
                storageFailure(e)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                storageFailure(e)
            }

        RequestEvents.status(response.first)
        return DynamoResponse(response.first, json.writeValueAsBytes(response.second))
    }

    private fun storageFailure(error: Exception): Pair<Int, ObjectNode> {
        logger.log(Level.WARNING, "DynamoDB request failed", error)
        return HttpURLConnection.HTTP_INTERNAL_ERROR to
            obj("__type" to "InternalServerError", "message" to "Storage request failed")
    }

    private fun request(input: DynamoRequest, checkActive: () -> Unit): ObjectNode {
        if (input.hasQuery) invalid("Unexpected query parameters")
        val body = input.body
        if (body.size > MAX_BODY) {
            throw ApiError(
                "RequestEntityTooLarge",
                "Request too large",
                HttpURLConnection.HTTP_ENTITY_TOO_LARGE,
            )
        }
        signature?.verify(input.headers, body, input.signature)
        val target = input.headers.first("X-Amz-Target").orEmpty()
        val prefix = when {
            target.startsWith(PREFIX) -> PREFIX
            target.startsWith(STREAM_PREFIX) -> STREAM_PREFIX
            else -> unknown()
        }
        val req =
            try {
                json.readTree(body) as? ObjectNode ?: invalid("Expected a JSON object")
            } catch (_: JsonProcessingException) {
                throw ApiError("SerializationException", "Invalid JSON")
            }
        val operation = target.removePrefix(prefix)
        return if (prefix == STREAM_PREFIX) dispatchStream(operation, req)
        else dispatch(operation, req, input, checkActive)
    }

    private fun dispatchStream(operation: String, req: ObjectNode): ObjectNode = when (operation) {
        "ListStreams" -> streams.list(req)
        "DescribeStream" -> streams.describe(req)
        "GetShardIterator" -> streams.iterator(req)
        "GetRecords" -> streams.records(req)
        else -> unknown()
    }

    private fun dispatch(
        operation: String, req: ObjectNode, input: DynamoRequest, checkActive: () -> Unit,
    ): ObjectNode =
        when (operation) {
            "DescribeEndpoints" -> {
                req.fieldsOnly()
                obj(
                    "Endpoints" to
                        listOf(
                            obj(
                                "CachePeriodInMinutes" to CACHE_MINUTES,
                            ).apply { put("Address", input.headers.first("Host")) }
                        )
                )
            }
            "CreateTable", "DescribeTable", "DeleteTable", "ListTables", "UpdateTable",
            "UpdateTimeToLive", "DescribeTimeToLive", "TagResource", "UntagResource",
            "ListTagsOfResource", "DescribeContinuousBackups" -> dispatchCatalog(operation, req)
            "Query",
            "Scan" -> reads.read(req, operation == "Query")
            "BatchGetItem" -> batches.get(req)
            "BatchWriteItem" -> batches.write(req, checkActive)
            "GetItem" -> items.get(req)
            "PutItem",
            "DeleteItem",
            "UpdateItem" -> items.write(req, operation)
            else -> unknown()
        }

    private fun dispatchCatalog(operation: String, req: ObjectNode): ObjectNode = when (operation) {
            "CreateTable" -> tables.create(req)
            "DescribeTable" -> tables.describe(req)
            "DeleteTable" -> tables.delete(req)
            "UpdateTable" -> tables.update(req)
            "UpdateTimeToLive" -> ttl.update(req)
            "DescribeTimeToLive" -> ttl.describe(req)
            "TagResource", "UntagResource", "ListTagsOfResource" -> catalog.tags(req, operation)
            "DescribeContinuousBackups" -> catalog.backups(req)
            "ListTables" -> tables.list(req)
        else -> unknown()
    }

    companion object {
        private const val CACHE_MINUTES = 60
        private const val PREFIX = "DynamoDB_20120810."
        private const val STREAM_PREFIX = "DynamoDBStreams_20120810."
        const val MAX_BODY = 16 * 1024 * 1024
        private val logger = Logger.getLogger(Dynamo::class.java.name)

        private fun unknown(): Nothing =
            throw ApiError("UnknownOperationException", "Unsupported operation")
    }
}
