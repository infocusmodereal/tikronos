package tikronos

import tikronos.expression.Operand
import tikronos.expression.Predicate
import tikronos.model.Table
import tikronos.model.Value
import tikronos.storage.ordered
import tikronos.storage.partitionPrefix
import tikronos.storage.prefix
import tikronos.storage.validateKeyValue

/** Half-open TiKV range. Encoded sort keys use only hexadecimal or decimal digits. */
internal data class KeyRange(val partition: Value, val start: String, val end: String)

internal fun Table.queryRange(predicate: Predicate?): KeyRange {
    val checks = flatten(predicate).map { it.keyFirst() }.associateByUniqueKey()
    val hash = checks[partitionKey] ?: invalid("Key condition missed partition key $partitionKey")
    if (hash.op != "=" || checks.keys.any { it != partitionKey && it != sortKey })
        invalid("Key condition operator not supported")
    val partition = hash.literal(1, partitionType)
    validateKeyValue(partition, partitionType, true)
    val prefix = partitionPrefix(partition)
    val check = sortKey?.let { checks[it] }
    return when {
        sortKey == null -> KeyRange(partition, prefix, prefix + "\u0000")
        check == null -> KeyRange(partition, prefix, prefix + "g")
        else -> sortRange(partition, prefix, check)
    }
}

private fun Table.sortRange(partition: Value, prefix: String, check: Predicate.Check): KeyRange {
    val value = check.literal(1, requireNotNull(sortType))
    val bound = prefix + ordered(value)
    val after = bound + "\u0000"
    val (low, high) =
        when (check.op) {
            "=" -> bound to after
            "<" -> prefix to bound
            "<=" -> prefix to after
            ">" -> after to prefix + "g"
            ">=" -> bound to prefix + "g"
            "BETWEEN" -> {
                val last = prefix + ordered(check.literal(2, sortType))
                if (last < bound) invalid("Invalid BETWEEN bounds")
                bound to last + "\u0000"
            }
            "begins_with" -> {
                if (sortType == "N") invalid("begins_with requires string or binary")
                bound to bound + "g"
            }
            else -> invalid("Unsupported KeyConditionExpression operator")
        }
    return KeyRange(partition, low, high)
}

private fun flatten(p: Predicate?): List<Predicate.Check> =
    when (p) {
        is Predicate.Check -> listOf(p)
        is Predicate.Junction ->
            if (p.and) p.children.flatMap(::flatten) else invalid("OR is invalid on keys")
        else -> invalid("Expected KeyConditionExpression or KeyConditions")
    }

private fun List<Predicate.Check>.associateByUniqueKey(): Map<String, Predicate.Check> {
    val keyed = associateBy {
        val path = (it.args.first() as? Operand.Field)?.path ?: invalid("Expected key attribute")
        if (path.steps.isNotEmpty()) invalid("Invalid nested key path")
        path.root
    }
    if (keyed.size != size) invalid("Only one condition per key is supported")
    return keyed
}

private fun Predicate.Check.literal(index: Int, type: String): Value =
    (args.getOrNull(index) as? Operand.Literal)?.value?.takeIf { it.type == type }
        ?: invalid("KeyConditionExpression operator or operand type not supported")

private fun Predicate.Check.keyFirst(): Predicate.Check {
    if (args.first() !is Operand.Literal || args.getOrNull(1) !is Operand.Field) return this
    val inverse = mapOf("=" to "=", "<" to ">", ">" to "<", "<=" to ">=", ">=" to "<=")
    return inverse[op]?.let { Predicate.Check(it, args.reversed()) } ?: this
}
