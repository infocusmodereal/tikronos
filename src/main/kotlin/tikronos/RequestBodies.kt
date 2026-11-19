package tikronos

import io.ktor.http.HttpStatusCode
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.availableForRead
import io.ktor.utils.io.readAvailable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout

/** Charge actual chunks and their final copy, never an untrusted declared length. */
internal class RequestBodies(private val budget: BodyBudget = BodyBudget(BUFFER_BYTES)) {

    fun bufferSize(value: String?): Int {
        val length = value?.toLongOrNull()
        if (length != null && length > Dynamo.MAX_BODY) tooLarge()
        return length?.coerceIn(1, CHUNK_BYTES.toLong())?.toInt() ?: CHUNK_BYTES
    }

    suspend fun <T> consume(input: ByteReadChannel, bufferSize: Int = CHUNK_BYTES, block: suspend (ByteArray) -> T): T {
        var reserved = 0
        try {
            val bytes = try {
                withTimeout(TOTAL_TIMEOUT_MS) {
                    read(input, bufferSize) { count ->
                        budget.reserve(count)
                        reserved += count
                    }
                }
            } catch (_: TimeoutCancellationException) {
                throw ApiError("RequestTimeout", "Request body timed out", HttpStatusCode.RequestTimeout.value)
            }
            return block(bytes)
        } finally {
            budget.release(reserved)
        }
    }

    private suspend fun read(input: ByteReadChannel, bufferSize: Int, reserve: (Int) -> Unit): ByteArray {
        var buffer = ByteArray(bufferSize)
        val chunks = mutableListOf<ByteArray>()
        var size = 0
        var filled = 0
        while (true) {
            val count = readChunk(input, buffer, filled)
            if (count < 0) break
            if (size + count > Dynamo.MAX_BODY) tooLarge()
            size += count
            filled += count
            if (filled == buffer.size) {
                reserve(filled)
                chunks += buffer
                buffer = ByteArray(if (size == bufferSize && bufferSize < CHUNK_BYTES) 1 else CHUNK_BYTES)
                filled = 0
            }
        }
        if (filled == 0 && chunks.size == 1) return chunks.single()
        if (filled > 0) {
            reserve(filled)
            chunks += buffer.copyOf(filled)
        }
        reserve(size)
        val result = ByteArray(size)
        var offset = 0
        for (chunk in chunks) {
            chunk.copyInto(result, offset)
            offset += chunk.size
        }
        return result
    }

    private suspend fun readChunk(input: ByteReadChannel, buffer: ByteArray, offset: Int): Int {
        // This is the only reader. Buffered bytes and end-of-stream need no idle timer.
        return if (input.availableForRead > 0 || input.isClosedForRead) {
            input.readAvailable(buffer, offset, buffer.size - offset)
        } else {
            withTimeout(IDLE_TIMEOUT_MS) { input.readAvailable(buffer, offset, buffer.size - offset) }
        }
    }

    private fun tooLarge(): Nothing =
        throw ApiError("RequestEntityTooLarge", "Request too large", HttpStatusCode.PayloadTooLarge.value)

    companion object {
        private const val BUFFER_BYTES = 64 * 1024 * 1024
        private const val CHUNK_BYTES = 8192
        private const val IDLE_TIMEOUT_MS = 3000L
        private const val TOTAL_TIMEOUT_MS = 30_000L
    }
}
