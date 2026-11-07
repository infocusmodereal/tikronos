package tikronos.storage

import org.tikv.common.Snapshot
import org.tikv.common.TiSession

internal class Storage(private val session: TiSession) {
    private val timestamps = TimestampOracle(session.pdClient)

    fun snapshot(): Snapshot = session.createSnapshot(timestamps.next())

    fun transaction(): Transaction {
        val started = System.nanoTime()
        return Transaction(snapshot(), this, started)
    }

    fun read(snapshot: Snapshot, key: String): ByteArray? =
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

    fun <T> atomic(initial: Transaction? = null, change: (Transaction) -> T): T {
        repeat(MAX_ATTEMPTS) {
            val tx = if (it == 0 && initial != null) initial else transaction()
            val result = change(tx)
            if (tx.writes.isEmpty()) return result
            val committed = AtomicCommit(session, timestamps::next)
                .commit(tx.version, tx.writes, tx.guards(), tx.started)
            if (committed) return result
        }
        throw ContentionLimitExceeded()
    }

    companion object {
        private const val PREFIX = "tikronos/v1/"
        const val CATALOG = "${PREFIX}table/"

        fun itemKey(id: String, value: String) = "${PREFIX}item/$id/$value"

        private const val MAX_ATTEMPTS = 8
    }
}

internal class ContentionLimitExceeded : RuntimeException()
