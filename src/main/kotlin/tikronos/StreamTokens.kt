package tikronos

import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import tikronos.model.StreamPosition
import tikronos.storage.Storage

/** Persistent authentication makes iterators portable across restarts and independent JVMs. */
internal class StreamTokens(private val db: Storage) {
    private val encoder = Base64.getUrlEncoder().withoutPadding()
    private val decoder = Base64.getUrlDecoder()
    private val secret: ByteArray by lazy {
        db.atomic { tx ->
            tx.read(SECRET) ?: ByteArray(SECRET_BYTES).also {
                SecureRandom().nextBytes(it)
                tx.write(SECRET, it)
            }
        }
    }

    fun encode(position: StreamPosition): String {
        val body = json.writeValueAsBytes(obj("arn" to position.arn,
            "after" to position.after, "expires" to position.expires))
        return encoder.encodeToString(body) + "." + encoder.encodeToString(sign(body))
    }

    fun decode(token: String, now: Long): StreamPosition {
        if (token.length > MAX_TOKEN_BYTES) invalid("Invalid ShardIterator")
        val pieces = token.split('.')
        if (pieces.size != 2) invalid("Invalid ShardIterator")
        val body = decodePart(pieces[0])
        if (!MessageDigest.isEqual(sign(body), decodePart(pieces[1]))) invalid("Invalid ShardIterator")
        val node = json.readTree(body)
        val result = StreamPosition(node["arn"].textValue(), node["after"].longValue(), node["expires"].longValue())
        if (result.expires <= now) throw ApiError("ExpiredIteratorException", "Shard iterator expired")
        return result
    }

    private fun decodePart(value: String): ByteArray = try {
        decoder.decode(value)
    } catch (_: IllegalArgumentException) {
        invalid("Invalid ShardIterator")
    }

    private fun sign(body: ByteArray): ByteArray = Mac.getInstance("HmacSHA256").run {
        init(SecretKeySpec(secret, "HmacSHA256"))
        doFinal(body)
    }

    companion object {
        private const val SECRET = "tikronos/v1/stream-token-secret"
        private const val SECRET_BYTES = 32
        private const val MAX_TOKEN_BYTES = 2048
    }
}
