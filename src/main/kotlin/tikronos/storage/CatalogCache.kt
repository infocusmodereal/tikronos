package tikronos.storage

import java.util.LinkedHashMap
import java.util.concurrent.locks.ReentrantLock
import org.tikv.common.Snapshot
import tikronos.model.Table
import tikronos.model.TableWritePlan
import tikronos.model.CatalogLease
import tikronos.model.LeasedValue

/** Read leases amortize catalog RPCs; TiKV transactions arbitrate renewal against catalog writers. */
internal class CatalogCache(private val db: Storage) {
    private data class Entry(val bytes: ByteArray, val plan: TableWritePlan, val from: Long, val until: Long) {
        val weight: Int get() = bytes.size + (plan.streamArn?.length ?: 0) * CHAR_BYTES +
            (plan.ttl?.let { it.attribute.length + it.generation.length } ?: 0) * CHAR_BYTES
    }
    private val entries = LinkedHashMap<String, Entry>(INITIAL_CAPACITY, LOAD_FACTOR, true)
    private val loaders = Array(LOCKS) { ReentrantLock() }
    private var bytes = 0L

    fun cached(key: String, version: Long): LeasedValue? = synchronized(entries) {
        entries[key]?.takeIf { version in it.from..it.until }?.let { LeasedValue(it.bytes, it.until, it.plan) }
    }

    fun decode(key: String, value: ByteArray): Table = synchronized(entries) {
        entries[key]?.takeIf { it.bytes.contentEquals(value) }?.plan?.table
    } ?: Records.table(value)

    fun read(snapshot: Snapshot, key: String): ByteArray? =
        leased(snapshot, key)?.bytes ?: db.directRead(snapshot, key)

    fun leased(snapshot: Snapshot, key: String): LeasedValue? {
        val current = cached(key, snapshot.version)
        if (current != null && current.until - snapshot.version > RENEW_AHEAD) return current
        return load(snapshot, key) ?: current
    }

    private fun load(snapshot: Snapshot, key: String): LeasedValue? {
        val loader = loaders[(key.hashCode() and Int.MAX_VALUE) % LOCKS]
        // A renewal must not queue unrelated requests behind its TiKV RPCs.
        if (!loader.tryLock()) return null
        return try {
            val previous = synchronized(entries) { entries[key] }
            when {
                previous != null && snapshot.version < previous.from -> null
                previous != null && previous.until - snapshot.version > RENEW_AHEAD ->
                    LeasedValue(previous.bytes, previous.until, previous.plan)
                else -> {
                    acquire(key)?.let { remember(key, it) }
                    cached(key, snapshot.version)
                }
            }
        } catch (_: ContentionLimitExceeded) {
            // Cache acquisition is optional; the caller retains a valid lease or reads directly.
            null
        } finally {
            loader.unlock()
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
        val until = if (lease.until - tx.version > RENEW_AHEAD) lease.until
            else tx.version + CatalogLeases.DURATION
        if (until != lease.until) tx.write(leaseKey, CatalogLeases.encode(CatalogLease(until)))
        Entry(value, tx.loadWritePlan(table), tx.version, until)
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
        val previous = entries.remove(key)
        bytes -= previous?.weight ?: 0
        // Only uninterrupted leases can extend an older snapshot's commit ceiling.
        val same = previous?.bytes?.contentEquals(entry.bytes) == true && previous.plan == entry.plan
        entries[key] = if (same && entry.from <= checkNotNull(previous).until)
            entry.copy(from = previous.from) else entry
        bytes += entry.weight
        while (entries.size > MAX_ENTRIES || bytes > MAX_BYTES) {
            val oldest = entries.entries.iterator()
            bytes -= oldest.next().value.weight
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
        private const val CHAR_BYTES = 2
        private const val INITIAL_CAPACITY = 16
        private const val LOAD_FACTOR = 0.75f
        private const val LOCKS = 64
        private const val MAX_ENTRIES = 4096
        private const val MAX_BYTES = 16 * 1024 * 1024
        private const val WARMUP_TABLES = 64
        private const val DRAIN_POLL_MS = 25L
        private const val RENEW_AHEAD = CatalogLeases.DURATION / 2
    }
}
