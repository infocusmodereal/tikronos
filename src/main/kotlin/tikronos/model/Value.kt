package tikronos.model

import java.math.BigDecimal
import java.util.Base64
import tikronos.invalid

/**
 * Normalized immutable attributes. Set equality is unordered and numbers have a unique decimal
 * scale.
 */
internal sealed class Value(val type: String) {
    abstract fun size(): Int

    internal open val depth: Int = 0

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
        override val depth = 1 + (fields.values.maxOfOrNull(Value::depth) ?: 0)
        private val bytes = DOCUMENT_OVERHEAD + fields.size + itemSize(fields)

        override fun size() = bytes
    }

    internal data class Sequence(val values: List<Value>) : Value("L") {
        override val depth = 1 + (values.maxOfOrNull(Value::depth) ?: 0)
        private val bytes = DOCUMENT_OVERHEAD + values.sumOf { it.size() + 1 }

        override fun size() = bytes
    }

    internal data class Members(val tag: String, val values: Set<Value>) : Value(tag) {
        override fun size() = values.sumOf(Value::size)
    }

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
            if (n.precision() > MAX_PRECISION)
                invalid("Cannot represent number with more than $MAX_PRECISION significant digits")
            if (exponent < -MIN_EXPONENT_MAGNITUDE) invalid("Numeric underflow")
            if (exponent > MAX_EXPONENT) invalid("Numeric overflow")
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
    }
}

internal typealias Item = Map<String, Value>

internal fun validatedString(s: String): String {
    if (s.encodeToByteArray().decodeToString() != s) invalid("Invalid Unicode string")
    return s
}

internal fun itemSize(item: Item) =
    item.entries.sumOf { (name, value) -> name.encodeToByteArray().size + value.size() }

internal const val MAX_ITEM_BYTES = 400 * 1024
internal const val MAX_DEPTH = 32
internal const val MAX_PRECISION = 38
private const val MIN_EXPONENT_MAGNITUDE = 130
private const val MAX_EXPONENT = 125
private const val DOCUMENT_OVERHEAD = 3
