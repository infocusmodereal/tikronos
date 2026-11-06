package tikronos.storage

import java.util.HexFormat
import java.util.zip.CRC32
import org.tikv.shade.com.google.protobuf.ByteString
import tikronos.invalid
import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.Value

internal fun Table.key(item: Item, exact: Boolean = true): Item {
    val result =
        listOfNotNull(partitionKey, sortKey).associateWith { attr ->
            val v = item[attr] ?: invalid("Missing key $attr")
            val hash = attr == partitionKey
            validateKeyValue(v, if (hash) partitionType else sortType, hash)
            v
        }
    if (exact && result.size != item.size) invalid("Unexpected key attributes")
    return result
}

internal fun Table.prefix() = Storage.itemKey(id, "")

internal fun Table.partitionPrefix(value: Value): String {
    if (hashed) {
        val encoded = ordered(value)
        val hash = CRC32().apply { update(encoded.encodeToByteArray()) }.value
        return prefix() + uintHex(hash) + "!" + encoded + "!"
    }
    // Pre-release catalog records have only a string HASH key and retain their raw address.
    return prefix() + value.stringValue()
}

internal fun Table.address(key: Item): String =
    partitionPrefix(key.getValue(partitionKey)) + (sortKey?.let { ordered(key.getValue(it)) } ?: "")

internal fun ordered(value: Value): String =
    when (value.type) {
        "S" -> HexFormat.of().formatHex(value.stringValue().encodeToByteArray())
        "B" -> HexFormat.of().formatHex(value.bytes())
        "N" -> {
            val n = value.decimal().stripTrailingZeros()
            if (n.signum() == 0) "1"
            else {
                val magnitude =
                    (n.precision() - n.scale() - 1 + EXPONENT_BIAS)
                        .toString()
                        .padStart(EXPONENT_WIDTH, '0') +
                        n.unscaledValue().abs().toString().padEnd(SIGNIFICAND_WIDTH, '0')
                if (n.signum() > 0) "2$magnitude"
                else "0" + magnitude.map { '9' - (it - '0') }.joinToString("")
            }
        }
        else -> invalid("Invalid key type")
    }

private const val MAX_HASH_BYTES = 2048
private const val MAX_SORT_BYTES = 1024
private const val EXPONENT_BIAS = 130
private const val SIGNIFICAND_WIDTH = 38

private const val EXPONENT_WIDTH = 3

internal fun validateKeyValue(v: Value, type: String?, hash: Boolean) {
    if (v.type != type) invalid("Key type mismatch")
    if (v.size() !in 1..(if (hash) MAX_HASH_BYTES else MAX_SORT_BYTES))
        invalid("Invalid key length")
}

private const val HASH_SPACE = 1L shl 32

private fun uintHex(value: Long) = HexFormat.of().toHexDigits(value.toInt())

/**
 * New tables partition by their stored hash prefix. Old HASH-string tables use their existing raw
 * byte ranges.
 */
internal fun Table.segmentEdge(segment: Int, total: Int): ByteString {
    return when (segment) {
        0 -> bytes(prefix())
        total -> prefixEnd(prefix())
        else -> interiorEdge(segment, total)
    }
}

private fun Table.interiorEdge(segment: Int, total: Int): ByteString {
    val cut = HASH_SPACE * segment / total
    if (hashed) return bytes(prefix() + uintHex(cut))
    return bytes(prefix())
        .concat(
            ByteString.copyFrom(
                java.nio.ByteBuffer.allocate(Int.SIZE_BYTES).putInt(cut.toInt()).array()
            )
        )
}
