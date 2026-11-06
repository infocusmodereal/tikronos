package tikronos

import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.WriteResult
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.address

/** The shared mutation boundary for API writes, batches and expiration. */
internal class ItemWriter(private val db: Storage) {
    private val indexes = IndexMaintenance()
    private val ttl = Ttl(db)
    private val streams = Streams(db)

    fun mutate(table: Table, key: Item, change: (Item?) -> Item?): WriteResult =
        db.atomic { tx -> change(tx, table, key, change = change) }

    fun change(
        tx: Transaction,
        table: Table,
        key: Item,
        ttl: Boolean = false,
        change: (Item?) -> Item?,
    ): WriteResult {
        val current = tx.read(Storage.CATALOG + table.name)?.let(Records::table)
        if (current?.id != table.id)
            throw ApiError("ResourceNotFoundException", "Table no longer exists")
        val address = current.address(key)
        val old = tx.read(address)?.let(Records::item)
        val next = change(old)
        val encoded = next?.let(::encodeChecked)
        indexes.update(tx, current, old, next)
        this.ttl.track(tx, current, old, next)
        streams.record(tx, current, old, next, ttl)
        tx.write(address, encoded)
        return WriteResult(current, old, next)
    }
}
