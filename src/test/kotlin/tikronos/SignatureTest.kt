package tikronos

import tikronos.protocol.RequestHeaders
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertFailsWith

class SignatureTest {
    @Test
    fun `SDK signature binds the operation and body and expires`() {
        val values = mutableMapOf<String, List<String>>().apply {
            put("Host", listOf("localhost:8000"))
            put("X-Amz-Date", listOf("20250101T000000Z"))
            put("X-Amz-Target", listOf("DynamoDB_20120810.ListTables"))
            put("Authorization", listOf(
                "AWS4-HMAC-SHA256 Credential=example/20250101/us-east-1/dynamodb/aws4_request, " +
                "SignedHeaders=host;x-amz-date;x-amz-target, " +
                "Signature=273e2020bfe60fcd8f379da90a0bba217726b4945533ce4fcdc2ae4270d47c68"))
        }
        val verifier = Signature("example", "example-test-secret", "us-east-1")
        val now = Instant.parse("2025-01-01T00:00:00Z")
        val body = "{}".encodeToByteArray()
        verifier.verify(RequestHeaders(values), body, now)
        assertFailsWith<ApiError> { verifier.verify(RequestHeaders(values), byteArrayOf(), now) }
        assertFailsWith<ApiError> { verifier.verify(RequestHeaders(values), body, now.plusSeconds(301)) }
        values["X-Amz-Target"] = listOf("DynamoDB_20120810.DeleteTable")
        assertFailsWith<ApiError> { verifier.verify(RequestHeaders(values), body, now) }
    }
}
