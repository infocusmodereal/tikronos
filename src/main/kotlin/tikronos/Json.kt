package tikronos

import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode

internal val json: JsonMapper = JsonMapper.builder()
    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .build()

internal class ApiError(val code: String, message: String, val status: Int = 400) : RuntimeException(message)

internal fun invalid(message: String): Nothing = throw ApiError("ValidationException", message)

internal fun JsonNode.string(name: String): String =
    get(name)?.takeIf { it.isTextual }?.textValue() ?: invalid("Expected string field $name")

internal fun JsonNode.obj(name: String): ObjectNode = get(name) as? ObjectNode ?: invalid("Expected object $name")

internal fun JsonNode.fieldsOnly(vararg names: String) {
    fieldNames().asSequence().firstOrNull { it !in names }?.let { invalid("Unsupported field $it") }
}

internal fun obj(vararg pairs: Pair<String, Any>): ObjectNode = json.valueToTree(pairs.toMap())
