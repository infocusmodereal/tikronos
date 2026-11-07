package tikronos.storage

import org.tikv.common.Snapshot

/** Point reads acquire validation locks; metadata uses striped validation fences. */
internal class Transaction(private val snapshot: Snapshot, private val storage: Storage) {
    val version: Long get() = snapshot.version
    private val reads = linkedMapOf<String, ByteArray?>()
    internal val writes = linkedMapOf<String, ByteArray?>()
    private var scope: String? = null

    fun prefetch(keys: List<String>) {
        val missing = keys.distinct().filter { it !in reads && it !in writes }
        if (missing.isEmpty()) return
        val found = snapshot.batchGet(READ_BUDGET_MS, missing.map(String::encodeToByteArray))
            .associate { it.key.decodeToString() to it.value }
        missing.forEach { reads[it] = found[it] }
    }

    fun read(key: String): ByteArray? {
        if (writes.containsKey(key)) return writes[key]
        if (!reads.containsKey(key)) reads[key] = storage.read(snapshot, key)
        return reads[key]
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
