package tikronos.expression

import com.fasterxml.jackson.databind.node.ObjectNode
import tikronos.codec.parseValue
import tikronos.fieldsOnly
import tikronos.invalid
import tikronos.obj
import tikronos.string

internal class RequestExpressions(private val req: ObjectNode) {
    val bindings = Bindings(req)

    init {
        val old = legacy.filter(req::has)
        // A legacy key condition can coexist with a projected read or a modern filter.
        val compatibleRead = old == listOf("KeyConditions") && !req.has("KeyConditionExpression")
        if (!compatibleRead && modern.any(req::has) && old.isNotEmpty())
            invalid("Cannot use both ${modern.filter(req::has)} and $old")
    }

    fun predicate(field: String, old: String): Predicate? =
        when {
            req.has(field) -> Parser(req.string(field), bindings, field).predicate()
            req.has(old) -> {
                val all =
                    req.obj(old).properties().map { (name, node) ->
                        legacyCheck(
                            name,
                            node as? ObjectNode ?: invalid("Invalid legacy predicate"),
                            old == "Expected",
                        )
                    }
                if (old != "KeyConditions" && all.isEmpty() && req.has("ConditionalOperator"))
                    invalid("ConditionalOperator requires non-empty $old")
                val join =
                    if (req.has("ConditionalOperator") && old != "KeyConditions")
                        req.string("ConditionalOperator")
                    else "AND"
                if (join !in setOf("AND", "OR")) invalid("Invalid ConditionalOperator")
                Predicate.Junction(join == "AND", all)
            }
            else -> null
        }

    fun projection(): List<Path>? =
        when {
            req.has("ProjectionExpression") ->
                Parser(req.string("ProjectionExpression"), bindings, "ProjectionExpression")
                    .projection()
            req.has("AttributesToGet") -> {
                val names = req.path("AttributesToGet")
                if (!names.isArray || names.isEmpty) invalid("Expected attribute names")
                names
                    .map {
                        Path(
                            it.textValue()?.takeIf(String::isNotEmpty)
                                ?: invalid("Invalid attribute name")
                        )
                    }
                    .also(::validatePaths)
            }
            else -> null
        }

    fun updates(): List<Action> =
        when {
            req.has("UpdateExpression") ->
                Parser(req.string("UpdateExpression"), bindings, "UpdateExpression").updates()
            req.has("AttributeUpdates") ->
                req.obj("AttributeUpdates").properties().map { (name, node) ->
                    node.fieldsOnly("Action", "Value")
                    val op = if (node.has("Action")) node.string("Action") else "PUT"
                    val v = node.get("Value")?.let { Operand.Literal(parseValue(it)) }
                    when {
                        op == "DELETE" && v == null -> Action("REMOVE", Path(name), null)
                        op == "ADD" && v?.value?.type == "L" -> Action("APPEND", Path(name), v)
                        op in setOf("PUT", "ADD", "DELETE") && v != null ->
                            Action(if (op == "PUT") "SET" else op, Path(name), v)
                        else -> invalid("Invalid legacy attribute update")
                    }
                }
            else -> emptyList()
        }

    fun finish() {
        bindings.finish()
        if (
            req.has("ConditionalOperator") &&
                listOf("Expected", "QueryFilter", "ScanFilter").none(req::has)
        ) {
            invalid("ConditionalOperator requires legacy conditions")
        }
    }

    companion object {
        private val modern =
            setOf(
                "UpdateExpression",
                "ConditionExpression",
                "FilterExpression",
                "KeyConditionExpression",
                "ProjectionExpression",
                "ExpressionAttributeValues",
                "ExpressionAttributeNames",
            )
        private val legacy =
            setOf(
                "Expected",
                "AttributeUpdates",
                "KeyConditions",
                "ScanFilter",
                "QueryFilter",
                "AttributesToGet",
                "ConditionalOperator",
            )
    }
}

private fun legacyCheck(name: String, node: ObjectNode, expected: Boolean): Predicate {
    val attr = Operand.Field(Path(name))
    if (expected && !node.has("ComparisonOperator")) {
        return legacyExpected(node, attr)
    }
    node.fieldsOnly("ComparisonOperator", "AttributeValueList")
    val op = node.string("ComparisonOperator")
    val raw = node.get("AttributeValueList")
    if (raw != null && !raw.isArray) invalid("Invalid AttributeValueList")
    val values = raw?.toList().orEmpty().map { Operand.Literal(parseValue(it)) }
    validateLegacyTypes(op, values)
    val count =
        when (op) {
            "IN" -> 1..MAX_IN_VALUES
            "BETWEEN" -> 2..2
            "NULL",
            "NOT_NULL" -> 0..0
            else -> 1..1
        }
    if (values.size !in count) invalid("Invalid number of operands for comparison $op")
    val normalized = operators[op] ?: invalid("Unknown legacy comparison")
    val test = Predicate.Check(normalized, listOf(attr) + values)
    return if (op == "NOT_CONTAINS")
        Predicate.Junction(
            true,
            listOf(Predicate.Check("attribute_exists", listOf(attr)), Predicate.Not(test)),
        )
    else test
}

private val operators =
    mapOf(
        "EQ" to "=",
        "NE" to "<>",
        "LT" to "<",
        "LE" to "<=",
        "GT" to ">",
        "GE" to ">=",
        "BETWEEN" to "BETWEEN",
        "IN" to "IN",
        "NULL" to "attribute_not_exists",
        "NOT_NULL" to "attribute_exists",
        "CONTAINS" to "contains",
        "NOT_CONTAINS" to "contains",
        "BEGINS_WITH" to "begins_with",
    )

private const val MAX_IN_VALUES = 100

private fun legacyExpected(node: ObjectNode, attr: Operand): Predicate {
    node.fieldsOnly("Value", "Exists")
    if (node.has("Exists") && !node["Exists"].isBoolean) invalid("Invalid Exists")
    val exists = node.path("Exists").asBoolean(true)
    if (exists != node.has("Value")) invalid("Invalid Expected value")
    return if (exists)
        Predicate.Check("=", listOf(attr, Operand.Literal(parseValue(node["Value"]))))
    else Predicate.Check("attribute_not_exists", listOf(attr))
}

private fun validateLegacyTypes(op: String, values: List<Operand.Literal>) {
    if (op !in setOf("IN", "CONTAINS", "NOT_CONTAINS")) return
    val types = values.map { it.value.type }.toSet()
    if (types.any { it !in setOf("S", "N", "B") } || types.size > 1)
        invalid("Invalid operand types for $op")
}
