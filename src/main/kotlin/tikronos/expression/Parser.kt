package tikronos.expression

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.codec.parseValue
import tikronos.codec.wire
import tikronos.invalid
import tikronos.model.Value
import tikronos.obj
import tikronos.string

internal class Bindings(req: ObjectNode) {
    private val names =
        if (req.has("ExpressionAttributeNames")) req.obj("ExpressionAttributeNames") else null
    private val values =
        if (req.has("ExpressionAttributeValues")) req.obj("ExpressionAttributeValues") else null
    private val usedNames = mutableSetOf<String>()
    private val usedValues = mutableMapOf<String, Value>()

    fun name(s: String): String {
        if (s.startsWith('#')) {
            if (s.length == 1) invalid("Empty name alias")
            usedNames += s
            return names?.string(s)?.takeIf(String::isNotEmpty) ?: invalid("Unbound name $s")
        }
        if (!identifier.matches(s) || s.uppercase() in reserved)
            invalid("Syntax error: invalid document path or reserved name $s")
        return s
    }

    fun value(s: String): Value {
        if (s.length == 1) invalid("Empty value alias")
        return usedValues.getOrPut(s) { parseValue(values?.get(s) ?: invalid("Unbound value $s")) }
    }

    fun finish() {
        if (names != null && (usedNames.isEmpty() || usedNames.size != names.size()))
            invalid(
                "Unused ExpressionAttributeNames: ${names.fieldNames().asSequence().toSet() - usedNames}"
            )
        if (values != null && (usedValues.isEmpty() || usedValues.size != values.size()))
            invalid(
                "Unused ExpressionAttributeValues: ${values.fieldNames().asSequence().toSet() - usedValues.keys}"
            )
    }

    companion object {
        private val identifier = Regex("[A-Za-z][A-Za-z0-9_]*")
        private val reserved =
            checkNotNull(Bindings::class.java.getResourceAsStream("/tikronos/reserved.txt"))
                .bufferedReader()
                .use { it.readLines().toSet() }
    }
}

// One production per method keeps the shared grammar and token cursor together.
@Suppress("TooManyFunctions")
internal class Parser(source: String, private val env: Bindings, private val field: String) {
    private val tokens: List<String>
    private var i = 0

    init {
        if (source.encodeToByteArray().size !in 1..MAX_EXPRESSION_BYTES)
            invalid("Expression is empty or exceeds $MAX_EXPRESSION_BYTES bytes")
        tokens = token.findAll(source).map { it.value }.filterNot { it.isBlank() }.toList()
        var depth = 0
        for (t in tokens) {
            if (t == "(") depth++
            if (depth > MAX_DEPTH) invalid("Expression too deeply nested")
            if (t == ")") depth--
        }
    }

    private fun peek() = tokens.getOrNull(i)

    private fun next(): String =
        tokens.getOrNull(i++) ?: invalid("Syntax error: incomplete expression")

    private fun eat(t: String): Boolean {
        if (!t.equals(peek(), ignoreCase = true)) return false
        i++
        return true
    }

    private fun expect(t: String) {
        if (!eat(t)) invalid("Expected $t")
    }

    private fun end() {
        if (i != tokens.size) invalid("Unexpected token ${peek()}")
    }

    private fun path(): Path {
        val root = env.name(next())
        val parts = mutableListOf<Step>()
        while (true) {
            when {
                eat(".") -> parts.add(Step.Name(env.name(next())))
                eat("[") -> {
                    val index =
                        next().toIntOrNull()?.takeIf { it >= 0 }
                            ?: invalid("$field: syntax error, invalid list index")
                    expect("]")
                    parts.add(Step.Index(index))
                }
                else -> return Path(root, parts)
            }
            if (parts.size >= MAX_DEPTH) invalid("Too many document path nesting levels")
        }
    }

    fun projection(): List<Path> {
        val paths = mutableListOf(path())
        while (eat(",")) paths += path()
        end()
        validatePaths(paths)
        return paths
    }

    private fun operand(update: Boolean): Operand {
        return when {
            peek()?.startsWith(':') == true -> Operand.Literal(env.value(next()))
            tokens.getOrNull(i + 1) != "(" -> Operand.Field(path())
            else -> functionOperand(update)
        }
    }

    private fun functionOperand(update: Boolean): Operand {
        val fn = next()
        expect("(")
        val args =
            when {
                fn == "size" && !update -> listOf(sizeOperand())
                fn == "if_not_exists" && update -> listOf(Operand.Field(path()), afterComma(update))
                fn == "list_append" && update -> listOf(operand(update), afterComma(update))
                else -> invalid("Syntax error: function $fn not allowed here")
            }
        expect(")")
        if (args.any { it is Operand.Function && it.name == "list_append" })
            invalid("list_append cannot be used as a function argument")
        return Operand.Function(fn, args)
    }

    private fun sizeOperand(): Operand {
        val arg = operand(false)
        if (peek() == ",") invalid("size requires 1 operand, received at least 2")
        if (
            arg is Operand.Function ||
                arg is Operand.Literal && arg.value.type in setOf("N", "BOOL", "NULL")
        ) {
            invalid("Invalid operand type for size")
        }
        return arg
    }

    private fun afterComma(update: Boolean): Operand {
        expect(",")
        return operand(update)
    }

    fun predicate(): Predicate = or().also { end() }

    private fun or(): Predicate {
        val children = mutableListOf(and())
        while (eat("OR")) children += and()
        return if (children.size == 1) children.single() else Predicate.Junction(false, children)
    }

    private fun and(): Predicate {
        val children = mutableListOf(unary())
        while (eat("AND")) children += unary()
        return if (children.size == 1) children.single() else Predicate.Junction(true, children)
    }

    private fun unary(): Predicate {
        var negations = 0
        while (eat("NOT")) negations++
        val pred = if (eat("(")) or().also { expect(")") } else comparison()
        return if (negations % 2 == 1) Predicate.Not(pred) else pred
    }

    private fun comparison(): Predicate {
        if (peek() in conditionFunctions) return functionPredicate()
        val left = operand(false)
        val op = next().uppercase()
        val args = mutableListOf(left)
        when (op) {
            "IN" -> {
                expect("(")
                args += operand(false)
                while (eat(",")) args += operand(false)
                if (args.size > MAX_IN_ARGS_WITH_SUBJECT) invalid("Too many IN operands")
                expect(")")
            }
            "BETWEEN" -> {
                args += operand(false)
                expect("AND")
                args += operand(false)
            }
            "=",
            "<>",
            "<",
            "<=",
            ">",
            ">=" -> args += operand(false)
            else -> invalid("Syntax error: unknown comparison $op")
        }
        return Predicate.Check(op, args)
    }

    private fun functionPredicate(): Predicate {
        val op = next()
        expect("(")
        val first =
            if (op in setOf("attribute_exists", "attribute_not_exists")) Operand.Field(path())
            else operand(false)
        val args = mutableListOf(first)
        if (op !in setOf("attribute_exists", "attribute_not_exists")) args += afterComma(false)
        expect(")")
        if (op == "attribute_type") {
            val type = (args[1] as? Operand.Literal)?.value
            if (type?.type != "S" || type.stringValue() !in types)
                invalid("Invalid attribute_type operand ${type?.wire()}")
        }
        return Predicate.Check(op, args)
    }

    fun updates(): List<Action> {
        val actions = mutableListOf<Action>()
        val clauses = mutableSetOf<String>()
        do {
            val clause = next().uppercase()
            if (clause !in updateClauses || !clauses.add(clause))
                invalid("Invalid update clause $clause")
            do {
                actions += action(clause)
            } while (eat(","))
        } while (peek() != null)
        validatePaths(actions.map(Action::path))
        return actions
    }

    private fun action(op: String): Action {
        val p = path()
        if (op != "SET") return nonSetAction(op, p)
        expect("=")
        val left = operand(true)
        return if (peek() in setOf("+", "-")) Action(op, p, left, next(), operand(true))
        else Action(op, p, left)
    }

    private fun nonSetAction(op: String, p: Path): Action {
        if (op == "REMOVE") return Action(op, p, null)
        if (peek()?.startsWith(':') != true) invalid("$op requires a value binding")
        return Action(op, p, Operand.Literal(env.value(next())))
    }

    companion object {
        private val token = Regex("""\s+|[#:][A-Za-z0-9_]+|[A-Za-z0-9_]+|<>|<=|>=|.""")
        private val conditionFunctions =
            setOf(
                "attribute_exists",
                "attribute_not_exists",
                "attribute_type",
                "begins_with",
                "contains",
            )
        private val updateClauses = setOf("ADD", "DELETE", "REMOVE", "SET")
        private val types = setOf("N", "S", "B", "NS", "SS", "BS", "BOOL", "NULL", "M", "L")
    }
}

private const val MAX_EXPRESSION_BYTES = 4096
private const val MAX_DEPTH = 32
private const val MAX_IN_ARGS_WITH_SUBJECT = 101
