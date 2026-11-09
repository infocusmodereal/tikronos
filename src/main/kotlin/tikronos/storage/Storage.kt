package tikronos.storage

import org.tikv.common.Snapshot
import org.tikv.common.TiSession
import java.util.concurrent.Executor

internal class Storage(
    private val session: TiSession,
    private val cleanup: Executor = Executor { it.run() },
    private val onePhase: Boolean = true,
) : AutoCloseable {
    private val pdTimestamps = PdTimestamps(session.pdClient)
    private val timestamps = TimestampOracle(pdTimestamps::allocate)
    val catalog = CatalogCache(this)

    fun snapshot(): Snapshot = session.createSnapshot(timestamps.next())

    fun transaction(): Transaction {
        val started = System.nanoTime()
        return Transaction(snapshot(), this, started)
    }

    fun read(snapshot: Snapshot, key: String): ByteArray? =
        if (key.startsWith(CATALOG)) catalog.read(snapshot, key) else directRead(snapshot, key)

    internal fun directRead(snapshot: Snapshot, key: String): ByteArray? =
        snapshot.get(key.encodeToByteArray()).takeIf { it.isNotEmpty() }

    val ranges = RangeReader(session)

    fun tables(after: String, limit: Int): List<String> {
        val start = CATALOG + after + if (after.isEmpty()) "" else "\u0000"
        return ranges
            .scan(bytes(start), prefixEnd(CATALOG), forward = true)
            .map { it.key.toStringUtf8().removePrefix(CATALOG) }
            .take(limit)
            .toList()
    }

    fun <T> atomic(initial: Transaction? = null, change: (Transaction) -> T): T =
        commit(initial, cached = true, change)

    internal fun <T> uncachedAtomic(change: (Transaction) -> T): T = commit(null, cached = false, change)

    private fun <T> commit(initial: Transaction?, cached: Boolean, change: (Transaction) -> T): T {
        var allowLeases = cached
        repeat(MAX_ATTEMPTS) {
            val tx = if (it == 0 && initial != null) initial else
                Transaction(snapshot(), this, cached = cached, allowLeases = allowLeases)
            val result = change(tx)
            check(cached || tx.writes.keys.none { key -> key.startsWith(CATALOG) }) {
                "Catalog mutations must participate in cache invalidation"
            }
            if (tx.writes.isEmpty()) return result
            if (cached && !catalog.prepare(tx)) return@repeat
            val validation = tx.validation()
            val committed = AtomicCommit(session, cleanup, timestamps::next, onePhase = onePhase)
                .commit(tx.version, tx.writes, validation.guards, tx.started, validation.commitCeiling)
            if (committed) return result
            // Long or contended writes make progress using the original full-validation path.
            if (validation.commitCeiling != Long.MAX_VALUE) allowLeases = false
        }
        throw ContentionLimitExceeded()
    }

    override fun close() = pdTimestamps.close()

    companion object {
        private const val PREFIX = "tikronos/v1/"
        const val CATALOG = "${PREFIX}table/"
        const val ITEM = "${PREFIX}item/"

        fun itemKey(id: String, value: String) = "$ITEM$id/$value"

        private const val MAX_ATTEMPTS = 8
    }
}

internal class ContentionLimitExceeded : RuntimeException()
