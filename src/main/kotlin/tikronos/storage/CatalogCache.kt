package tikronos.storage

import java.util.LinkedHashMap
import org.tikv.common.Snapshot
import tikronos.model.Table
import tikronos.model.CatalogLease
import tikronos.model.LeasedValue

/** Read leases amortize catalog RPCs; TiKV transactions arbitrate renewal against catalog writers. */
internal class CatalogCache(private val db: Storage) {
    private data class Entry(val bytes: ByteArray, val table: Table, val from: Long, val until: Long)
    private val entries = LinkedHashMap<String, Entry>(INITIAL_CAPACITY, LOAD_FACTOR, true)
    private val loaders = Array(LOCKS) { Any() }
    private var bytes = 0L

    fun cached(key: String, version: Long): LeasedValue? = synchronized(entries) {
        entries[key]?.takeIf { version in it.from..it.until }?.let { LeasedValue(it.bytes, it.until) }
    }

    fun decode(key: String, value: ByteArray): Table = synchronized(entries) {
        entries[key]?.takeIf { it.bytes.contentEquals(value) }?.table
    } ?: Records.table(value)

    fun read(snapshot: Snapshot, key: String): ByteArray? =
        leased(snapshot, key)?.bytes ?: db.directRead(snapshot, key)

    fun leased(snapshot: Snapshot, key: String): LeasedValue? =
        cached(key, snapshot.version) ?: load(snapshot, key)

    private fun load(snapshot: Snapshot, key: String): LeasedValue? =
        synchronized(loaders[(key.hashCode() and Int.MAX_VALUE) % LOCKS]) {
            val previous = synchronized(entries) { entries[key] }
            if (previous != null && snapshot.version <= previous.until) {
                previous.takeIf { snapshot.version >= it.from }?.let { LeasedValue(it.bytes, it.until) }
            } else {
                acquire(key)?.also { remember(key, it) }
                    ?.takeIf { snapshot.version in it.from..it.until }?.let { LeasedValue(it.bytes, it.until) }
            }
        }

    private fun acquire(key: String): Entry? = db.uncachedAtomic { tx ->
        val leaseKey = CatalogLeases.key(key)
        val lease = CatalogLeases.decode(tx.read(leaseKey))
        if (lease.blockedUntil > tx.version) return@uncachedAtomic null
        val value = tx.read(key) ?: return@uncachedAtomic null
        val table = decode(key, value)
        // Backfill checkpoints change on every row; caching them delays progress without benefit.
        if (table.indexes.any { it.status == "CREATING" }) return@uncachedAtomic null
        val until = if (lease.until > tx.version) lease.until else tx.version + CatalogLeases.DURATION
        if (until != lease.until) tx.write(leaseKey, CatalogLeases.encode(CatalogLease(until)))
        Entry(value, table, tx.version, until)
    }

    /** All catalog writers, including maintenance, must pass this gate before their commit. */
    fun prepare(tx: Transaction): Boolean {
        val keys = tx.writes.keys.filter { it.startsWith(Storage.CATALOG) }
        for (key in keys) {
            val leaseKey = CatalogLeases.key(key)
            val lease = CatalogLeases.decode(tx.read(leaseKey))
            if (lease.until >= tx.version) {
                drain(key)
                return false
            }
            // This real write conflicts with any concurrent renewal, using native MVCC.
            tx.write(leaseKey, null)
        }
        return true
    }

    private fun drain(key: String) {
        val until = db.uncachedAtomic { tx ->
            val leaseKey = CatalogLeases.key(key)
            val lease = CatalogLeases.decode(tx.read(leaseKey))
            if (lease.until >= tx.version && lease.blockedUntil <= tx.version) {
                val blocked = lease.copy(blockedUntil = tx.version + CatalogLeases.DRAIN_TIMEOUT)
                tx.write(leaseKey, CatalogLeases.encode(blocked))
            }
            lease.until
        }
        while (db.snapshot().version <= until) Thread.sleep(DRAIN_POLL_MS)
    }

    private fun remember(key: String, entry: Entry) = synchronized(entries) {
        bytes -= entries.remove(key)?.bytes?.size ?: 0
        entries[key] = entry
        bytes += entry.bytes.size
        while (entries.size > MAX_ENTRIES || bytes > MAX_BYTES) {
            val oldest = entries.entries.iterator()
            bytes -= oldest.next().value.bytes.size
            oldest.remove()
        }
    }

    /** Bounded startup discovery; larger catalogs remain available through lazy loading. */
    fun warmup() {
        for (name in db.tables("", WARMUP_TABLES)) {
            read(db.snapshot(), Storage.CATALOG + name)
        }
    }

    companion object {
        private const val INITIAL_CAPACITY = 16
        private const val LOAD_FACTOR = 0.75f
        private const val LOCKS = 64
        private const val MAX_ENTRIES = 4096
        private const val MAX_BYTES = 16 * 1024 * 1024
        private const val WARMUP_TABLES = 64
        private const val DRAIN_POLL_MS = 25L
    }
}
