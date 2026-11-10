package tikronos.storage

import org.tikv.common.Snapshot
import tikronos.model.Table
import tikronos.model.TableWritePlan
import tikronos.model.LeasedValue
import tikronos.model.ReadValidation

/** Point reads acquire validation locks; metadata uses striped validation fences. */
internal class Transaction(
    private val snapshot: Snapshot,
    private val storage: Storage,
    internal val started: Long = System.nanoTime(),
    private val cached: Boolean = true,
    private val allowLeases: Boolean = true,
) {
    val version: Long get() = snapshot.version
    private val reads = linkedMapOf<String, ByteArray?>()
    internal val writes = linkedMapOf<String, ByteArray?>()
    private var scope: String? = null
    private val plans = mutableMapOf<String, TableWritePlan>()
    private val covered = mutableSetOf<String>()
    private var ceiling = Long.MAX_VALUE

    fun prefetch(keys: List<String>) {
        if (cached && allowLeases) keys.filter { it.startsWith(Storage.CATALOG) && it !in reads && it !in writes }
            .forEach { key -> storage.catalog.cached(key, version)?.let { remember(key, it) } }
        val missing = keys.distinct().filter { it !in reads && it !in writes }
        if (missing.isEmpty()) return
        val found = snapshot.readBatch(missing, READ_BUDGET_MS)
        missing.forEach { reads[it] = found[it] }
    }

    fun read(key: String): ByteArray? {
        if (writes.containsKey(key)) return writes[key]
        if (!reads.containsKey(key)) {
            val lease = if (cached && allowLeases && key.startsWith(Storage.CATALOG))
                storage.catalog.leased(snapshot, key) else null
            if (lease != null) remember(key, lease) else reads[key] = storage.directRead(snapshot, key)
        }
        return reads[key]
    }

    fun table(name: String): Table? {
        val key = Storage.CATALOG + name
        return read(key)?.let { storage.catalog.decode(key, it) }
    }

    fun writePlan(name: String): TableWritePlan? {
        val table = table(name) ?: return null
        // Read-your-writes takes precedence over any leased configuration.
        return plans[Storage.CATALOG + name]?.takeIf { writes.isEmpty() } ?: loadWritePlan(table)
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

    private fun remember(key: String, lease: LeasedValue) {
        reads[key] = lease.bytes
        plans[key] = lease.plan
        covered += key
        ceiling = minOf(ceiling, lease.until)
    }

    /** Semantic changes must rewrite this catalog key, protected by its fence or lease. */
    fun coveredBy(catalog: String, keys: List<String>) {
        if (catalog in reads) covered.addAll(keys)
    }

    internal fun validation() = ReadValidation(
        (reads.keys - writes.keys - covered).map { MetadataFences.guard(it, checkNotNull(scope)) }.toSet(),
        ceiling,
    )

    companion object { private const val READ_BUDGET_MS = 3000 }
}
