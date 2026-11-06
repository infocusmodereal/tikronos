package tikronos.expression

import java.math.BigDecimal
import tikronos.invalid
import tikronos.model.Item
import tikronos.model.Value

internal sealed interface Operand {
    fun read(item: Item): Value?

    data class Field(val path: Path) : Operand {
        override fun read(item: Item) = path.read(item)
    }

    data class Literal(val value: Value) : Operand {
        override fun read(item: Item) = value
    }

    data class Function(val name: String, val args: List<Operand>) : Operand {
        override fun read(item: Item): Value? {
            val a = args.first().read(item)
            return when (name) {
                "size" ->
                    a?.let {
                        val size =
                            when (it.type) {
                                "S",
                                "B" -> it.size()
                                "M" -> it.members().size
                                "L" -> it.entries().size
                                "SS",
                                "NS",
                                "BS" -> it.elements().size
                                else -> return null
                            }
                        Value.number(size.toBigDecimal())
                    }
                "if_not_exists" -> a ?: args[1].read(item)
                "list_append" -> {
                    val b = args[1].read(item)
                    if (a?.type != "L" || b?.type != "L") invalid("list_append requires lists")
                    Value.list(a.entries() + b.entries())
                }
                else -> error("Unexpected function")
            }
        }
    }
}

internal sealed interface Predicate {
    fun matches(item: Item): Boolean

    data class Junction(val and: Boolean, val children: List<Predicate>) : Predicate {
        override fun matches(item: Item) =
            if (and) children.all { it.matches(item) } else children.any { it.matches(item) }
    }

    data class Not(val child: Predicate) : Predicate {
        override fun matches(item: Item) = !child.matches(item)
    }

    data class Check(val op: String, val args: List<Operand>) : Predicate {
        init {
            val literals = args.filterIsInstance<Operand.Literal>().map { it.value }
            val ordered = setOf("S", "N", "B")
            if (
                op in setOf("<", ">", "<=", ">=", "BETWEEN") && literals.any { it.type !in ordered }
            )
                invalid("Invalid operand type for $op")
            if (op == "begins_with" && literals.any { it.type !in setOf("S", "B") })
                invalid("Invalid BEGINS_WITH operand for begins_with")
            if (op == "contains" && args[0] is Operand.Field && args[0] == args[1])
                invalid("contains operands must be distinct")
            if (op == "BETWEEN") validateBounds()
        }

        private fun validateBounds() {
            val low = (args[1] as? Operand.Literal)?.value ?: return
            val high = (args[2] as? Operand.Literal)?.value ?: return
            if ((low.compare(high) ?: invalid("BETWEEN type mismatch")) > 0)
                invalid("Invalid BETWEEN bounds")
        }

        override fun matches(item: Item): Boolean {
            val a = args[0].read(item)
            val b = args.getOrNull(1)?.read(item)
            return when (op) {
                "attribute_exists" -> a != null
                "attribute_not_exists" -> a == null
                "IN" -> a != null && args.drop(1).any { it.read(item) == a }
                "BETWEEN" -> {
                    val c = args[2].read(item)
                    b != null &&
                        c != null &&
                        a?.compare(b)?.let { it >= 0 } == true &&
                        a.compare(c)?.let { it <= 0 } == true
                }
                else -> binary(op, a, b)
            }
        }
    }
}

private fun binary(op: String, a: Value?, b: Value?): Boolean {
    if (a == null || b == null) return op == "<>"
    return when (op) {
        "attribute_type" -> a.type == b.stringValue()
        "contains" -> contains(a, b)
        "begins_with" -> beginsWith(a, b)
        "=" -> a == b
        "<>" -> a != b
        else -> a.compare(b)?.let { compare(it, op) } == true
    }
}

private fun compare(result: Int, op: String) =
    when (op) {
        "<" -> result < 0
        ">" -> result > 0
        "<=" -> result <= 0
        ">=" -> result >= 0
        else -> error("Unknown comparison")
    }

internal fun beginsWith(a: Value, b: Value): Boolean =
    a.type == b.type &&
        when (a.type) {
            "S" -> a.stringValue().startsWith(b.stringValue())
            "B" ->
                a.bytes().let { bytes ->
                    b.bytes().let { prefix ->
                        bytes.size >= prefix.size && prefix.indices.all { bytes[it] == prefix[it] }
                    }
                }
            else -> false
        }

private fun contains(a: Value, b: Value): Boolean =
    when (a.type) {
        "S" -> b.type == "S" && a.stringValue().contains(b.stringValue())
        "B" ->
            b is Value.Binary &&
                java.util.Collections.indexOfSubList(a.bytes().asList(), b.bytes().asList()) >= 0
        "L" -> b in a.entries()
        "SS",
        "NS",
        "BS" -> b in a.elements()
        else -> false
    }

internal data class Action(
    val op: String,
    val path: Path,
    val operand: Operand?,
    val math: String? = null,
    val right: Operand? = null,
) {
    fun evaluate(item: Item): Value? {
        if (op == "REMOVE") return null
        val v = operand?.read(item) ?: invalid("Missing update operand")
        val old = path.read(item)
        return when (op) {
            "SET" -> computeSet(v, item)
            "APPEND" -> appendLegacy(old, v)
            "ADD" ->
                if (v.type == "N") {
                    if (old != null && old.type != "N") invalid("ADD type mismatch")
                    Value.number((old?.decimal() ?: BigDecimal.ZERO) + v.decimal())
                } else {
                    checkSet(v, old)
                    Value.set(v.type, old?.elements().orEmpty() + v.elements())
                }
            "DELETE" -> {
                checkSet(v, old)
                (old?.elements().orEmpty() - v.elements())
                    .takeIf { it.isNotEmpty() }
                    ?.let { Value.set(v.type, it) }
            }
            else -> error("Unknown update action")
        }
    }

    private fun appendLegacy(old: Value?, operand: Value): Value {
        if (old != null && old !is Value.Sequence) invalid("Invalid operand type for ADD")
        return Value.list(old?.entries().orEmpty() + operand.entries())
    }

    private fun computeSet(v: Value, item: Item): Value =
        if (math == null) v
        else {
            val rhs = right?.read(item)
            if (v.type != "N" || rhs?.type != "N") invalid("Arithmetic requires numbers")
            Value.number(
                if (math == "+") v.decimal() + rhs.decimal() else v.decimal() - rhs.decimal()
            )
        }

    private fun checkSet(v: Value, old: Value?) {
        if (v.type !in setOf("SS", "NS", "BS") || old != null && old.type != v.type)
            invalid("Invalid operand type for set operation")
    }
}

internal fun Predicate.paths(): List<Path> =
    when (this) {
        is Predicate.Junction -> children.flatMap(Predicate::paths)
        is Predicate.Not -> child.paths()
        is Predicate.Check -> args.flatMap { it.paths() }
    }

private fun Operand.paths(): List<Path> =
    when (this) {
        is Operand.Field -> listOf(path)
        is Operand.Function -> args.flatMap { it.paths() }
        is Operand.Literal -> emptyList()
    }
