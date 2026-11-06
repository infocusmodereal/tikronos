package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import tikronos.expression.RequestExpressions
import tikronos.model.Index
import tikronos.model.Throughput
import tikronos.model.Table
import tikronos.model.Value
import tikronos.storage.entry
import tikronos.storage.schema
import tikronos.storage.Records
import tikronos.storage.prefix

class IndexTest {
    private val table = Table("Items", "generation", "pk", 0, null, hashed = true)
    private val index = Index("byGroup", "index-generation", "group", "S", "rank", "S")

    @Test
    fun `equal index keys preserve distinct primary items and exact string bounds`() {
        val first = mapOf("pk" to Value.text("one"), "group" to Value.text("g"), "rank" to Value.text("a"))
        val second = first + ("pk" to Value.text("two"))
        val longer = first + ("rank" to Value.text("a\u0000"))
        val req = obj("KeyConditionExpression" to "#g = :g AND #r = :r",
            "ExpressionAttributeNames" to mapOf("#g" to "group", "#r" to "rank"),
            "ExpressionAttributeValues" to mapOf(":g" to mapOf("S" to "g"), ":r" to mapOf("S" to "a")))
        val condition = RequestExpressions(req).predicate("KeyConditionExpression", "KeyConditions")
        val range = index.schema(table).queryRange(condition, true)
        val a = requireNotNull(index.entry(table, first))
        val b = requireNotNull(index.entry(table, second))
        assertFalse(a.startsWith(table.prefix()))
        assertNotEquals(a, b)
        assertTrue(a >= range.start && a < range.end)
        assertTrue(b >= range.start && b < range.end)
        assertFalse(requireNotNull(index.entry(table, longer)) < range.end)
        assertNull(index.entry(table, first - "rank"))
    }

    @Test
    fun `index inequalities and equal between bounds use the whole duplicate key group`() {
        val ranks = listOf("0", "a", "a\u0000", "b")
        val cases = mapOf("< :r" to listOf("0"), "<= :r" to listOf("0", "a"),
            "> :r" to listOf("a\u0000", "b"), ">= :r" to listOf("a", "a\u0000", "b"),
            "BETWEEN :r AND :r" to listOf("a"))
        for ((operator, expected) in cases) {
            val req = obj("KeyConditionExpression" to "#g = :g AND #r $operator",
                "ExpressionAttributeNames" to mapOf("#g" to "group", "#r" to "rank"),
                "ExpressionAttributeValues" to mapOf(":g" to mapOf("S" to "g"), ":r" to mapOf("S" to "a")))
            val condition = RequestExpressions(req).predicate("KeyConditionExpression", "KeyConditions")
            val range = index.schema(table).queryRange(condition, true)
            val included = ranks.filter { rank ->
                val item = mapOf("pk" to Value.text("row"), "group" to Value.text("g"), "rank" to Value.text(rank))
                val address = requireNotNull(index.entry(table, item))
                address >= range.start && address < range.end
            }
            assertEquals(expected, included, operator)
        }
    }

    @Test
    fun `capacity follows index projection and charges both sides of a key move`() {
        val definition = index.copy(projection = "KEYS_ONLY")
        val indexed = table.copy(indexes = listOf(definition))
        val item = mapOf("pk" to Value.text("row"), "group" to Value.text("g"), "rank" to Value.text("a"),
            "payload" to Value.text("x".repeat(8192)))
        val target = ReadTarget(indexed, definition)
        assertTrue(target.storedSize(item) < 100)
        assertEquals(1.0, writeCapacity(indexed, null, item).global[definition.name])
        assertTrue(writeCapacity(indexed, item, item + ("payload" to Value.text("changed"))).global.isEmpty())
        assertEquals(2.0, writeCapacity(indexed, item, item + ("rank" to Value.text("b"))).global[definition.name])
    }

    @Test
    fun `billing transitions clear or require capacity for every global index`() {
        val provisioned = table.copy(throughput = Throughput(2, 2),
            indexes = listOf(index.copy(throughput = Throughput(1, 1))))
        val onDemand = updateCatalog(obj("BillingMode" to "PAY_PER_REQUEST"), provisioned)
        assertNull(onDemand.throughput)
        assertNull(onDemand.indexes.single().throughput)
        val request = obj("BillingMode" to "PROVISIONED",
            "ProvisionedThroughput" to obj("ReadCapacityUnits" to 2, "WriteCapacityUnits" to 2))
        assertFailsWith<ApiError> { updateCatalog(request, onDemand) }
        request.set<com.fasterxml.jackson.databind.node.ObjectNode>("GlobalSecondaryIndexUpdates",
            json.valueToTree(listOf(obj("Update" to obj("IndexName" to index.name,
                "ProvisionedThroughput" to obj("ReadCapacityUnits" to 1, "WriteCapacityUnits" to 1))))))
        assertEquals(provisioned, updateCatalog(request, onDemand))
    }

    @Test
    fun `catalog roundtrip preserves resumable index identity and cursor`() {
        val original = table.copy(indexes = listOf(index.copy(status = "CREATING", cursor = "resume")),
            tags = mapOf("owner" to "someone"))
        assertEquals(original, Records.table(Records.encodeTable(original)))
        val item = mapOf("pk" to Value.text("one"), "group" to Value.text("g"), "rank" to Value.text("a"))
        assertNotEquals(index.entry(table, item), index.entry(table.copy(id = "replacement"), item))
    }
}
