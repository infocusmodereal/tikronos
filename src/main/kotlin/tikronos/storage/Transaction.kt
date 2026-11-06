package tikronos.storage

import org.tikv.common.Snapshot

/** Point reads acquire validation locks, so metadata changes cannot race an item commit. */
internal class Transaction(private val snapshot: Snapshot, private val storage: Storage) {
    val version: Long get() = snapshot.version
    private val reads = linkedMapOf<String, ByteArray?>()
    internal val writes = linkedMapOf<String, ByteArray?>()

    fun read(key: String): ByteArray? {
        if (writes.containsKey(key)) return writes[key]
        if (!reads.containsKey(key)) reads[key] = storage.read(snapshot, key)
        return reads[key]
    }

    fun write(key: String, value: ByteArray?) {
        require(value == null || value.isNotEmpty()) { "Empty values are reserved for absence" }
        writes[key] = value
    }

    internal fun guards(): Set<String> = reads.keys - writes.keys
}
