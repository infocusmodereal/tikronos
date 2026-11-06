package tikronos.model

import com.fasterxml.jackson.databind.JsonNode
import java.math.BigDecimal
import java.util.Base64
import tikronos.invalid
import tikronos.obj

/**
 * Normalized immutable attributes. Set equality is unordered and numbers have a unique decimal
 * scale.
 */
internal sealed class Value(val type: String) {
    abstract fun size(): Int

    internal data class Text(val value: String) : Value("S") {
        override fun size() = value.encodeToByteArray().size
    }

    internal data class Number(val value: BigDecimal) : Value("N") {
        override fun size() = (value.precision() + 1) / 2 + 1
    }

    internal data class Binary(val base64: String, val length: Int) : Value("B") {
        override fun size() = length
    }

    internal data class Bool(val value: Boolean) : Value("BOOL") {
        override fun size() = 1
    }

    internal data object Null : Value("NULL") {
        override fun size() = 1
    }

    internal data class Document(val fields: Item) : Value("M") {
        private val bytes = DOCUMENT_OVERHEAD + fields.size + itemSize(fields)

        override fun size() = bytes
    }

    internal data class Sequence(val values: List<Value>) : Value("L") {
        private val bytes = DOCUMENT_OVERHEAD + values.sumOf { it.size() + 1 }

        override fun size() = bytes
    }

    internal data class Members(val tag: String, val values: Set<Value>) : Value(tag) {
        override fun size() = values.sumOf(Value::size)
    }

    fun wire(): JsonNode =
        obj(
            type to
                when (this) {
                    is Text -> value
                    is Number -> value.toPlainString()
                    is Binary -> base64
                    is Bool -> value
                    Null -> true
                    is Document -> wireItem(fields)
                    is Sequence -> values.map(Value::wire)
                    is Members -> values.map { it.wire()[it.type].textValue() }.sorted()
                }
        )

    fun stringValue() = (this as Text).value

    fun decimal() = (this as Number).value

    fun bytes(): ByteArray = Base64.getDecoder().decode((this as Binary).base64)

    fun entries() = (this as Sequence).values

    fun members() = (this as Document).fields

    fun elements() = (this as Members).values

    fun compare(other: Value): Int? =
        when {
            this is Number && other is Number -> value.compareTo(other.value)
            this is Text && other is Text ->
                java.util.Arrays.compareUnsigned(
                    value.encodeToByteArray(),
                    other.value.encodeToByteArray(),
                )
            this is Binary && other is Binary ->
                java.util.Arrays.compareUnsigned(bytes(), other.bytes())
            else -> null
        }

    companion object {
        fun number(value: BigDecimal): Number {
            val n = value.stripTrailingZeros()
            val exponent = n.precision().toLong() - n.scale() - 1
            if (n.precision() > MAX_PRECISION || exponent !in -MIN_EXPONENT_MAGNITUDE..MAX_EXPONENT)
                invalid("Number outside DynamoDB range")
            return Number(n)
        }

        fun text(value: String) = Text(validatedString(value))

        fun list(values: List<Value>) =
            Sequence(values).also {
                // Bound intermediate list_append results as well as final item writes.
                if (it.size() > MAX_ITEM_BYTES) invalid("List exceeds item size limit")
            }

        fun map(values: Item) = Document(values)

        fun set(type: String, values: Collection<Value>): Value {
            val unique = values.toSet()
            if (unique.isEmpty() || unique.size != values.size)
                invalid("Invalid empty set or duplicate element")
            return Members(type, unique)
        }

        fun parse(node: JsonNode, depth: Int = 0): Value {
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
                    set(type, data.map { scalar(type.dropLast(1), it) })
                }
                else -> invalid("Unknown attribute type $type")
            }
        }

        private fun document(type: String, data: JsonNode, depth: Int): Value {
            if (depth >= MAX_DEPTH) invalid("Document is too deep")
            if (type == "M") return map(parseItem(data, depth + 1))
            if (!data.isArray) invalid("Expected list")
            return list(data.map { parse(it, depth + 1) })
        }

        private fun booleanValue(type: String, data: JsonNode): Value {
            if (!data.isBoolean || type == "NULL" && !data.booleanValue()) invalid("Invalid $type")
            return if (type == "NULL") Null else Bool(data.booleanValue())
        }

        private fun scalar(type: String, data: JsonNode): Value {
            val value = data.textValue() ?: invalid("Expected $type string")
            return when (type) {
                "N" -> {
                    if (value.length > MAX_NUMERIC_TEXT) invalid("Number literal too long")
                    try {
                        number(value.toBigDecimal())
                    } catch (_: NumberFormatException) {
                        invalid("Invalid decimal")
                    } catch (_: ArithmeticException) {
                        invalid("Number outside DynamoDB range")
                    }
                }
                "B" ->
                    try {
                        val decoded = Base64.getDecoder().decode(value)
                        Binary(Base64.getEncoder().encodeToString(decoded), decoded.size)
                    } catch (_: IllegalArgumentException) {
                        invalid("Invalid base64")
                    }
                else -> text(value)
            }
        }
    }
}

internal typealias Item = Map<String, Value>

internal fun validatedString(s: String): String {
    if (s.encodeToByteArray().decodeToString() != s) invalid("Invalid Unicode string")
    return s
}

internal fun parseItem(node: JsonNode, depth: Int = 0): Item {
    if (!node.isObject) invalid("Expected attribute map")
    return node.properties().associate { (name, value) ->
        if (name.isEmpty()) invalid("Empty attribute name")
        validatedString(name) to Value.parse(value, depth)
    }
}

internal fun wireItem(item: Item) = item.mapValues { it.value.wire() }

internal fun itemSize(item: Item) =
    item.entries.sumOf { (name, value) -> name.encodeToByteArray().size + value.size() }

internal const val MAX_ITEM_BYTES = 400 * 1024
private const val MAX_NUMERIC_TEXT = 4096
private const val MAX_DEPTH = 32
private const val MAX_PRECISION = 38
private const val MIN_EXPONENT_MAGNITUDE = 130
private const val MAX_EXPONENT = 125
private const val DOCUMENT_OVERHEAD = 3
