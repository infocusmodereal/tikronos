package tikronos

import com.sun.net.httpserver.Headers
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** SigV4 for the single DynamoDB POST / endpoint (no query, permanent credentials). */
internal class Signature(private val access: String, private val secret: String, private val region: String) {
    fun verify(headers: Headers, body: ByteArray, now: Instant = Instant.now()) {
        val auth = authorization.matchEntire(headers.getFirst("Authorization").orEmpty()) ?: denied()
        val day = auth.groups["day"]!!.value
        val names = auth.groups["names"]!!.value
        val signature = auth.groups["signature"]!!.value
        val date = headers.getFirst("X-Amz-Date") ?: denied()
        if (auth.groups["id"]!!.value != access || auth.groups["region"]!!.value != region) denied()
        if (!date.startsWith(day) || headers.containsKey("X-Amz-Security-Token")) denied()
        validateTime(date, now)
        val signed = names.split(';')
        if (signed != signed.distinct().sorted() || !signed.containsAll(required)) denied()
        val hash = sha256(body)
        if (headers.getFirst("X-Amz-Content-Sha256")?.let { it != hash } == true) denied()
        val canonicalHeaders = signed.joinToString("\n") { name ->
            val values = headers[name] ?: denied()
            "$name:" + values.joinToString(",") { it.trim().replace(whitespace, " ") }
        }
        val canonical = "POST\n/\n\n$canonicalHeaders\n\n$names\n$hash"
        val scope = "$day/$region/dynamodb/aws4_request"
        val toSign = "AWS4-HMAC-SHA256\n$date\n$scope\n" + sha256(canonical.encodeToByteArray())
        val key = scope.split('/').fold(("AWS4" + secret).encodeToByteArray(), ::hmac)
        if (!MessageDigest.isEqual(hmac(key, toSign), HexFormat.of().parseHex(signature))) denied()
    }

    private fun validateTime(date: String, now: Instant) {
        val time = try {
            Instant.from(dateFormat.parse(date))
        } catch (_: DateTimeParseException) {
            denied()
        }
        if (Duration.between(time, now).abs() > MAX_SKEW) denied()
    }

    companion object {
        private val MAX_SKEW = Duration.ofMinutes(5)
        private val authorization = Regex(
            """AWS4-HMAC-SHA256 Credential=(?<id>[^/ ,]+)/(?<day>\d{8})/(?<region>[^/ ,]+)/dynamodb/aws4_request, """ +
                """SignedHeaders=(?<names>[a-z0-9;-]+), Signature=(?<signature>[0-9a-f]{64})"""
        )
        private val dateFormat = DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmssX")
            .withResolverStyle(java.time.format.ResolverStyle.STRICT)
        private val required = setOf("host", "x-amz-date", "x-amz-target")
        private val whitespace = Regex("[ \t]+")
        private fun denied(): Nothing = throw ApiError("AccessDeniedException", "Signature verification failed")
        private fun sha256(bytes: ByteArray) =
            HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
        private fun hmac(key: ByteArray, value: String): ByteArray = Mac.getInstance("HmacSHA256").run {
            init(SecretKeySpec(key, algorithm))
            doFinal(value.encodeToByteArray())
        }
    }
}
