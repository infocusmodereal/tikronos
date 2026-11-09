package tikronos

import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.Value
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.entry
import tikronos.storage.key

@Tag("tikv")
class MaximumKeyIntegrationTest {
    @Test
    fun `maximum DynamoDB keys fit secondary entries and TTL backfill candidates`() {
        withStorageSession { _, db ->
            val id = "000-maxkeys-" + UUID.randomUUID()
            val indexes = listOf(
                Index("globalKeys", UUID.randomUUID().toString(), "gpk", "S", "gsk", "S",
                    false, "ALL", emptyList(), null),
                Index("localKeys", UUID.randomUUID().toString(), "pk", "S", "lsk", "S",
                    true, "ALL", emptyList(), null),
            )
            val table = Table(id, id, "pk", Instant.now().epochSecond, null,
                sortKey = "sk", sortType = "S", hashed = true, indexes = indexes)
            db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
            val item = mapOf("pk" to Value.text("p".repeat(HASH_BYTES)),
                "sk" to Value.text("s".repeat(SORT_BYTES)), "gpk" to Value.text("g".repeat(HASH_BYTES)),
                "gsk" to Value.text("h".repeat(SORT_BYTES)), "lsk" to Value.text("l".repeat(SORT_BYTES)),
                "expires" to Value.number((Instant.now().epochSecond - 1).toBigDecimal()))
            val key = table.key(item, false)
            val writer = ItemWriter(db)
            try {
                writer.mutate(table, key) { item }
                indexes.forEach { assertNotNull(db.read(db.snapshot(), requireNotNull(it.entry(table, item)))) }
                Ttl(db).update(obj("TableName" to table.name,
                    "TimeToLiveSpecification" to obj("Enabled" to true, "AttributeName" to "expires")))
                TtlWorker(db, Ttl(db)).tick(writer)
                assertNull(db.read(db.snapshot(), table.address(key)))
                indexes.forEach { assertNull(db.read(db.snapshot(), requireNotNull(it.entry(table, item)))) }
            } finally {
                writer.mutate(table, key) { null }
                Tables(db).delete(obj("TableName" to table.name))
            }
        }
    }

    companion object {
        private const val HASH_BYTES = 2048
        private const val SORT_BYTES = 1024
    }
}
