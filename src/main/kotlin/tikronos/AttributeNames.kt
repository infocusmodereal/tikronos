package tikronos

import com.fasterxml.jackson.databind.node.ObjectNode

/** Resolves expression aliases and rejects dangling bindings before evaluating an item. */
internal class AttributeNames(request: ObjectNode) {
    private val aliases = if (request.has("ExpressionAttributeNames")) request.obj("ExpressionAttributeNames") else null
    private val used = mutableSetOf<String>()

    fun resolve(token: String): String {
        if (!token.startsWith('#')) {
            if (!identifier.matches(token)) invalid("Expected a top-level attribute name")
            return token
        }
        if (!alias.matches(token)) invalid("Invalid attribute alias")
        used += token
        return aliases?.string(token)?.takeIf(String::isNotEmpty) ?: invalid("Missing attribute alias $token")
    }

    fun finish() {
        if (aliases != null && (used.isEmpty() || aliases.size() != used.size)) invalid("Unused expression names")
    }

    companion object {
        private val identifier = Regex("[A-Za-z][A-Za-z0-9]*")
        private val alias = Regex("#[A-Za-z0-9_]+")
    }
}
