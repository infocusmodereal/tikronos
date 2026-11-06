package tikronos.storage

import org.tikv.common.Snapshot
import org.tikv.common.TiSession

internal class Storage(private val session: TiSession) {
    fun snapshot(): Snapshot = session.createSnapshot()

    fun read(snapshot: Snapshot, key: String): ByteArray? =
        snapshot.get(key.encodeToByteArray()).takeIf { it.isNotEmpty() }

    val ranges = RangeReader(session)

    fun tables(after: String, limit: Int): List<String> {
        val start = CATALOG + after + if (after.isEmpty()) "" else "\u0000"
        return ranges
            .scan(bytes(start), bytes(CATALOG.dropLast(1) + "0"), forward = true)
            .map { it.key.toStringUtf8().removePrefix(CATALOG) }
            .take(limit)
            .toList()
    }

    fun <T> atomic(change: (Transaction) -> T): T {
        repeat(MAX_ATTEMPTS) {
            val tx = Transaction(snapshot(), this)
            val result = change(tx)
            if (tx.writes.isEmpty()) return result
            if (AtomicCommit(session).commit(tx.version, tx.writes, tx.guards())) return result
        }
        throw ContentionLimitExceeded()
    }

    fun mutate(key: String, change: (ByteArray?) -> ByteArray?): ByteArray? = atomic { tx ->
        val old = tx.read(key)
        tx.write(key, change(old))
        old
    }

    companion object {
        private const val PREFIX = "tikronos/v1/"
        const val CATALOG = "${PREFIX}table/"

        fun itemKey(id: String, value: String) = "${PREFIX}item/$id/$value"

        private const val MAX_ATTEMPTS = 8
    }
}

internal class ContentionLimitExceeded : RuntimeException()
