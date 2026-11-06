package tikronos.protocol

import java.util.Locale

/** Keep duplicate values in wire order, with case-insensitive header names. */
internal class RequestHeaders(headers: Map<String, List<String>>) {
    private val values = headers.entries.groupBy { it.key.lowercase(Locale.ROOT) }
        .mapValues { (_, entries) -> entries.flatMap { it.value } }

    operator fun get(name: String): List<String>? = values[name.lowercase(Locale.ROOT)]
    fun first(name: String): String? = get(name)?.firstOrNull()
    fun contains(name: String): Boolean = get(name) != null
}
