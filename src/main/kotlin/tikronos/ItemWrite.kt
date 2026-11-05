package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode

/** A conditional write is evaluated again on the fresh snapshot after each prewrite conflict. */
internal class ItemWrite(req: ObjectNode, hash: String, private val replacement: ObjectNode?) {
    private val requireAbsent = req.has("ConditionExpression")

    init {
        if (requireAbsent) {
            val match = absent.matchEntire(req.string("ConditionExpression").trim())
                ?: invalid("Expected attribute_not_exists(partitionKey)")
            if (attributeName(match.groupValues[1], req) != hash) invalid("Condition must reference the partition key")
        }
        replacement?.let(::validateItem)
    }

    fun apply(current: ObjectNode?): ObjectNode? {
        if (requireAbsent && current != null) throw ApiError("ConditionalCheckFailedException", "Condition failed")
        return replacement
    }

    private fun validateItem(item: ObjectNode) {
        var size = 0
        for ((name, attribute) in item.properties()) {
            if (name.isEmpty() || attribute.size() != 1 || !attribute.path("S").isTextual) {
                invalid("Expected a named string attribute")
            }
            size += name.encodeToByteArray().size + attribute["S"].textValue().encodeToByteArray().size
        }
        if (size > MAX_ITEM_BYTES) invalid("Item exceeds 400 KiB")
    }

    companion object {
        private const val MAX_ITEM_BYTES = 400 * 1024
        private val absent = Regex("""attribute_not_exists\s*\(\s*(#?[a-zA-Z0-9_]+)\s*\)""")
        private val name = Regex("#?[a-zA-Z0-9_]+")
        fun attributeName(token: String, req: ObjectNode): String {
            if (!name.matches(token)) invalid("Expected a top-level attribute name")
            return if (token.startsWith('#')) req.obj("ExpressionAttributeNames").string(token) else token
        }
    }
}
