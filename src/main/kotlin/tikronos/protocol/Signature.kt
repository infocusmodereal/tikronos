package tikronos.protocol

import tikronos.ApiError
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
    @Volatile private var signingKey: DayKey? = null

    fun verify(
        headers: RequestHeaders, body: ByteArray, context: SignatureContext?, now: Instant = Instant.now(),
    ) {
        val auth = context as? Prepared ?: denied()
        if (auth.owner !== this || auth.headers !== headers) denied()
        // Admission validation cannot authorize a request that expired while reading or queued.
        validateTime(auth.time, now)
        val day = auth.day
        val names = auth.names
        val date = auth.date
        val signed = auth.signed
        val hash = sha256(body)
        if (headers.first("X-Amz-Content-Sha256")?.let { it != hash } == true) denied()
        val canonicalHeaders = signed.joinToString("\n") { name ->
            val values = headers[name] ?: denied()
            "$name:" + values.joinToString(",") { it.trim().replace(whitespace, " ") }
        }
        val canonical = "POST\n/\n\n$canonicalHeaders\n\n$names\n$hash"
        val scope = "$day/$region/dynamodb/aws4_request"
        val toSign = "AWS4-HMAC-SHA256\n$date\n$scope\n" + sha256(canonical.encodeToByteArray())
        if (!MessageDigest.isEqual(hmac(key(day), toSign), auth.signature)) denied()
    }

    fun validateHeaders(headers: RequestHeaders, now: Instant = Instant.now()): SignatureContext {
        val auth = authorization.matchEntire(headers.first("Authorization").orEmpty()) ?: denied()
        val date = headers.first("X-Amz-Date") ?: denied()
        val day = auth.groups["day"]!!.value
        if (auth.groups["id"]!!.value != access || auth.groups["region"]!!.value != region) denied()
        if (!date.startsWith(day) || headers.contains("X-Amz-Security-Token")) denied()
        val time = parseTime(date)
        validateTime(time, now)
        val signed = auth.groups["names"]!!.value.split(';')
        if (signed != signed.distinct().sorted() || !signed.containsAll(required)) denied()
        return Prepared(headers, day, date, time,
            signed, HexFormat.of().parseHex(auth.groups["signature"]!!.value))
    }

    private fun key(day: String): ByteArray {
        signingKey?.takeIf { it.day == day }?.let { return it.bytes }
        val bytes = listOf(day, region, "dynamodb", "aws4_request")
            .fold(("AWS4" + secret).encodeToByteArray(), ::hmac)
        // One immutable entry per verifier; concurrent misses may derive the same key harmlessly.
        signingKey = DayKey(day, bytes)
        return bytes
    }

    private class DayKey(val day: String, val bytes: ByteArray)

    private inner class Prepared(
        val headers: RequestHeaders,
        val day: String,
        val date: String,
        val time: Instant,
        val signed: List<String>,
        val signature: ByteArray,
    ) : SignatureContext {
        val names = signed.joinToString(";")
        val owner: Signature get() = this@Signature
    }

    private fun parseTime(date: String): Instant = try {
        Instant.from(dateFormat.parse(date))
    } catch (_: DateTimeParseException) {
        denied()
    }

    private fun validateTime(time: Instant, now: Instant) {
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
