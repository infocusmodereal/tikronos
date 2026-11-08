package tikronos

import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFailsWith
import tikronos.protocol.RequestHeaders
import tikronos.protocol.Signature

class SignatureTest {
    private val verifier = Signature("example", "example-test-secret", "us-east-1")
    private val body = "{}".encodeToByteArray()
    private val now = Instant.parse("2025-01-01T00:00:00Z")

    @Test
    fun `admission context still binds the body and expires after queueing`() {
        val headers = RequestHeaders(values())
        val auth = verifier.validateHeaders(headers, now.plusSeconds(299))
        verifier.verify(headers, body, auth, now.plusSeconds(300))
        assertFailsWith<ApiError> { verifier.verify(headers, byteArrayOf(), auth, now) }
        assertFailsWith<ApiError> { verifier.verify(headers, body, auth, now.plusSeconds(301)) }
        val badHash = RequestHeaders(values().apply { put("X-Amz-Content-Sha256", listOf("0".repeat(64))) })
        assertFailsWith<ApiError> {
            verifier.verify(badHash, body, verifier.validateHeaders(badHash, now), now)
        }
        val changed = RequestHeaders(values().apply { put("X-Amz-Target", listOf("DynamoDB_20120810.DeleteTable")) })
        assertFailsWith<ApiError> {
            verifier.verify(changed, body, verifier.validateHeaders(changed, now), now)
        }
    }

    @Test
    fun `missing or foreign admission contexts cannot authorize a request`() {
        val headers = RequestHeaders(values())
        val auth = verifier.validateHeaders(headers, now)
        assertFailsWith<ApiError> { verifier.verify(headers, body, null, now) }
        assertFailsWith<ApiError> { verifier.verify(RequestHeaders(values()), body, auth, now) }
        assertFailsWith<ApiError> {
            Signature("example", "example-test-secret", "us-east-1").verify(headers, body, auth, now)
        }
    }

    @Test
    fun `credential scope and signed headers are rejected before admission`() {
        val authorization = values().getValue("Authorization").single()
        val malformed = listOf(
            "Authorization" to authorization.replace("example/", "other/"),
            "Authorization" to authorization.replace("us-east-1", "us-west-2"),
            "Authorization" to authorization.replace("20250101", "20250102"),
            "Authorization" to authorization.replace("host;x-amz-date", "x-amz-date;host"),
            "Authorization" to authorization.replace("host;", "host;host;"),
            "Authorization" to authorization.replace(";x-amz-target", ""),
            "X-Amz-Security-Token" to "unsupported-session-token",
            "X-Amz-Date" to "20250101T250000Z",
        )
        for ((name, value) in malformed) {
            assertFailsWith<ApiError>(name) {
                verifier.validateHeaders(RequestHeaders(values().apply { put(name, listOf(value)) }), now)
            }
        }
    }

    @Test
    fun `date key rotation stays correct under concurrent requests across midnight`() {
        // Fixed signatures generated independently with botocore SigV4Auth, not this verifier.
        val headers = listOf(
            RequestHeaders(values("20250101T235959Z",
                "eefadb7d46ac06ddc55151e592abad7e9e964ba880ab9a9d0060a136e0bf0c1d")),
            RequestHeaders(values("20250102T000001Z",
                "349adb0f0d5b2edc3567e8487b2e6494f2bfd52bb8167889a444c25989656e20")),
        )
        val midnight = Instant.parse("2025-01-02T00:00:00Z")
        val contexts = headers.map { verifier.validateHeaders(it, midnight) }
        Executors.newFixedThreadPool(8).use { pool ->
            val tasks = List(8) { worker ->
                pool.submit {
                    repeat(64) { iteration ->
                        val index = (worker + iteration) % headers.size
                        verifier.verify(headers[index], body, contexts[index], midnight)
                    }
                }
            }
            tasks.forEach { it.get(5, TimeUnit.SECONDS) }
        }
        val wrongDay = RequestHeaders(values("20250102T000001Z",
            "eefadb7d46ac06ddc55151e592abad7e9e964ba880ab9a9d0060a136e0bf0c1d"))
        assertFailsWith<ApiError> {
            verifier.verify(wrongDay, body, verifier.validateHeaders(wrongDay, midnight), midnight)
        }
    }

    private fun values(
        date: String = "20250101T000000Z",
        signature: String = "273e2020bfe60fcd8f379da90a0bba217726b4945533ce4fcdc2ae4270d47c68",
    ) = mutableMapOf(
        "Host" to listOf("localhost:8000"),
        "X-Amz-Date" to listOf(date),
        "X-Amz-Target" to listOf("DynamoDB_20120810.ListTables"),
        "Authorization" to listOf(
            "AWS4-HMAC-SHA256 Credential=example/${date.take(8)}/us-east-1/dynamodb/aws4_request, " +
                "SignedHeaders=host;x-amz-date;x-amz-target, Signature=$signature",
        ),
    )
}
