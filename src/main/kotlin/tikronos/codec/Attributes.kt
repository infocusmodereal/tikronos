package tikronos.codec

import com.fasterxml.jackson.databind.JsonNode
import java.util.Base64
import tikronos.ApiError
import tikronos.invalid
import tikronos.model.Item
import tikronos.model.MAX_DEPTH
import tikronos.model.Value
import tikronos.model.attributeName
import tikronos.json
import com.fasterxml.jackson.databind.node.ObjectNode

internal fun wireItem(item: Item): ObjectNode = json.createObjectNode().apply {
    item.forEach { (name, value) -> set<ObjectNode>(name, value.wire()) }
}

internal fun Value.wire(): ObjectNode = json.createObjectNode().also { node ->
    when (this) {
        is Value.Text, is Value.Number, is Value.Binary -> node.put(type, scalarText())
        is Value.Bool -> node.put(type, value)
        Value.Null -> node.put(type, true)
        is Value.Document -> node.set<ObjectNode>(type, wireItem(fields))
        is Value.Sequence -> node.putArray(type).also { array -> values.forEach { array.add(it.wire()) } }
        is Value.Members -> node.putArray(type).also { array ->
            values.map { it.scalarText() }.sorted().forEach(array::add)
        }
    }
}

private fun Value.scalarText(): String = when (this) {
    is Value.Text -> value
    is Value.Number -> value.toPlainString()
    is Value.Binary -> base64
    else -> error("Expected a string, number or binary value")
}

internal fun parseValue(node: JsonNode, depth: Int = 0): Value {
    if (!node.isObject || node.size() != 1) invalid("Invalid attribute value")
    val (type, data) = node.properties().single()
    return when (type) {
        "S",
        "N",
        "B" -> scalar(type, data)
        "BOOL",
        "NULL" -> booleanValue(type, data)
        "M",
        "L" -> document(type, data, depth)
        "SS",
        "NS",
        "BS" -> {
            if (!data.isArray) invalid("Expected set")
            Value.set(type, data.map { scalar(type.dropLast(1), it) })
        }
        else -> invalid("Unknown attribute type $type")
    }
}

private fun document(type: String, data: JsonNode, depth: Int): Value {
    if (depth >= MAX_DEPTH) invalid("Document is too deep")
    if (type == "M") return Value.map(parseItem(data, depth + 1))
    if (!data.isArray) invalid("Expected list")
    return Value.list(data.map { parseValue(it, depth + 1) })
}

private fun booleanValue(type: String, data: JsonNode): Value {
    if (!data.isBoolean || type == "NULL" && !data.booleanValue()) invalid("Invalid $type")
    return if (type == "NULL") Value.Null else Value.Bool(data.booleanValue())
}

private fun scalar(type: String, data: JsonNode): Value {
    val value =
        data.textValue()
            ?: throw ApiError("SerializationException", "Expected $type as a JSON string")
    return when (type) {
        "N" -> {
            if (value.length > MAX_NUMERIC_TEXT) invalid("Number literal too long")
            try {
                Value.number(value.toBigDecimal())
            } catch (_: NumberFormatException) {
                invalid("Invalid numeric string")
            } catch (_: ArithmeticException) {
                invalid("Number outside DynamoDB range")
            }
        }
        "B" ->
            try {
                val decoded = Base64.getDecoder().decode(value)
                Value.Binary(Base64.getEncoder().encodeToString(decoded), decoded.size)
            } catch (_: IllegalArgumentException) {
                invalid("Invalid base64")
            }
        else -> Value.text(value)
    }
}

internal fun parseItem(node: JsonNode, depth: Int = 0): Item {
    if (!node.isObject) invalid("Expected attribute map")
    return node.properties().associate { (name, value) ->
        attributeName(name) to parseValue(value, depth)
    }
}

private const val MAX_NUMERIC_TEXT = 4096
