package tikronos.storage

import org.tikv.common.Snapshot
import tikronos.model.Table

/** Point reads acquire validation locks; metadata uses striped validation fences. */
internal class Transaction(
    private val snapshot: Snapshot,
    private val storage: Storage,
    internal val started: Long = System.nanoTime(),
    private val cached: Boolean = true,
) {
    val version: Long get() = snapshot.version
    private val reads = linkedMapOf<String, ByteArray?>()
    internal val writes = linkedMapOf<String, ByteArray?>()
    private var scope: String? = null

    fun prefetch(keys: List<String>) {
        if (cached) keys.filter { it.startsWith(Storage.CATALOG) && it !in reads && it !in writes }
            .forEach { key -> storage.catalog.cached(key, version)?.let { reads[key] = it } }
        val missing = keys.distinct().filter { it !in reads && it !in writes }
        if (missing.isEmpty()) return
        val found = snapshot.batchGet(READ_BUDGET_MS, missing.map(String::encodeToByteArray))
            .associate { it.key.decodeToString() to it.value }
        missing.forEach { reads[it] = found[it] }
    }

    fun read(key: String): ByteArray? {
        if (writes.containsKey(key)) return writes[key]
        if (!reads.containsKey(key)) reads[key] =
            if (cached) storage.read(snapshot, key) else storage.directRead(snapshot, key)
        return reads[key]
    }

    fun table(name: String): Table? {
        val key = Storage.CATALOG + name
        return read(key)?.let { storage.catalog.decode(key, it) }
    }

    fun write(key: String, value: ByteArray?) {
        require(value == null || value.isNotEmpty()) { "Empty values are reserved for absence" }
        writes[key] = value
        if (scope == null) scope = key
        if (MetadataFences.tracks(key)) {
            MetadataFences.keys(key).forEach { writes[it] = byteArrayOf(1) }
        }
    }

    internal fun guards(): Set<String> = (reads.keys - writes.keys)
        .map { MetadataFences.guard(it, checkNotNull(scope)) }.toSet()

    companion object { private const val READ_BUDGET_MS = 3000 }
}
