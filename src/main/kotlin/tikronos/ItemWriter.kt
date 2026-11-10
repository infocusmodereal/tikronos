package tikronos

import tikronos.model.Item
import tikronos.model.Table
import tikronos.model.WriteIntent
import tikronos.model.WriteResult
import tikronos.model.PreviousItem
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.address

/** The shared mutation boundary for API writes, batches and expiration. */
internal class ItemWriter(private val db: Storage) {
    private val ttl = Ttl(db)
    private val streams = Streams(db)

    fun mutate(
        table: Table, key: Item, initial: Transaction? = null, change: (Item?) -> Item?,
    ): WriteResult = mutate(table, key, WriteIntent.Transform(change), initial)

    fun mutate(table: Table, key: Item, intent: WriteIntent, initial: Transaction? = null): WriteResult =
        db.atomic(initial) { tx -> change(tx, table, key, intent) }

    fun change(
        tx: Transaction, table: Table, key: Item, ttl: Boolean = false, change: (Item?) -> Item?,
    ): WriteResult = change(tx, table, key, WriteIntent.Transform(change), ttl)

    fun change(
        tx: Transaction, table: Table, key: Item, intent: WriteIntent, ttl: Boolean = false,
    ): WriteResult {
        val plan = tx.writePlan(table.name)
        if (plan?.table?.id != table.id)
            throw ApiError("ResourceNotFoundException", "Table no longer exists")
        val current = plan.table
        val address = current.address(key)
        val previous = if (intent is WriteIntent.Transform || plan.needsPrevious)
            PreviousItem.Read(tx.read(address)?.let(Records::item)) else PreviousItem.Unread
        val next = when (intent) {
            is WriteIntent.Replace -> intent.item
            is WriteIntent.Transform -> intent.apply((previous as PreviousItem.Read).item)
        }
        val encoded = next?.also(::validateItem)?.let(Records::encodeItem)
        if (previous is PreviousItem.Read) {
            maintainIndexes(tx, current, previous.item, next, encoded)
            this.ttl.track(tx, current, previous.item, next, plan.ttl)
            streams.record(tx, plan, previous.item, next, ttl)
        }
        tx.write(address, encoded)
        return WriteResult(current, previous, next)
    }
}
