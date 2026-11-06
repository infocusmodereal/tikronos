package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import tikronos.expression.RequestExpressions
import tikronos.expression.patch
import tikronos.model.Value

class ExpressionTest {
    @Test
    fun `invalid legacy and aliased names fail before entering the transaction`() {
        val req =
            obj("AttributeUpdates" to obj("" to obj("Action" to "PUT", "Value" to obj("S" to "x"))))
        assertFailsWith<ApiError> { RequestExpressions(req).updates() }
        val alias =
            obj(
                "UpdateExpression" to "SET #bad = :v",
                "ExpressionAttributeNames" to obj("#bad" to "\uD800"),
                "ExpressionAttributeValues" to obj(":v" to obj("S" to "x")),
            )
        assertFailsWith<ApiError> { RequestExpressions(alias).updates() }
    }

    @Test
    fun `all right hand sides see original list indices`() {
        val expressions =
            RequestExpressions(obj("UpdateExpression" to "REMOVE vals[0] SET vals[2] = vals[1]"))
        val input = mapOf("vals" to Value.list(listOf("a", "b", "c").map(Value::text)))
        val result = patch(input, expressions.updates().associate { it.path to it.evaluate(input) })
        assertEquals(listOf("b", "b").map(Value::text), result.getValue("vals").entries())
    }

    @Test
    fun `between consumes its own AND before logical precedence`() {
        val req =
            obj(
                "FilterExpression" to
                    "n BETWEEN :low AND :high AND NOT attribute_exists(x) OR attribute_exists(y)",
                "ExpressionAttributeValues" to
                    obj(":low" to obj("N" to "1"), ":high" to obj("N" to "2")),
            )
        val expressions = RequestExpressions(req)
        val predicate = requireNotNull(expressions.predicate("FilterExpression", "ScanFilter"))
        assertTrue(predicate.matches(mapOf("n" to Value.number("1.50".toBigDecimal()))))
        expressions.finish()
        req.obj("ExpressionAttributeValues").putObject(":unused").put("S", "x")
        assertFailsWith<ApiError> { expressions.finish() }
    }
}
