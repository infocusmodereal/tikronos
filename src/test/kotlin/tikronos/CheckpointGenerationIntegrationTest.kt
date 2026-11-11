package tikronos

import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import tikronos.model.BackfillProgress
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.TtlPolicy
import tikronos.model.TtlState
import tikronos.model.Value
import tikronos.storage.BackfillRecords
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.TtlRecords
import tikronos.storage.address
import tikronos.storage.entry

@Tag("tikv")
class CheckpointGenerationIntegrationTest {
    @Test
    fun `stale index workers cannot publish a dropped index or recreated table`() = withStorageSession { _, db ->
        val id = UUID.randomUUID().toString()
        val index = Index("byGroup", id, "group", "S", status = "CREATING")
        val table = Table("generation-$id", id, "pk", 0, null, indexes = listOf(index))
        val item = mapOf("pk" to Value.text("row"), "group" to Value.text("g"))
        val worker = IndexBackfill(db)
        val replacement = index.copy(id = UUID.randomUUID().toString())
        val recreated = table.copy(id = UUID.randomUUID().toString())
        try {
            db.atomic { tx ->
                tx.write(table.address(item), Records.encodeItem(item))
                tx.write(BackfillRecords.index(table, index), BackfillRecords.encode(BackfillProgress("")))
            }
            for (changed in listOf(table.copy(indexes = listOf(replacement)), recreated)) {
                db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(changed)) }
                worker.backfill(table, index)
                assertEquals(changed, Records.table(assertNotNull(
                    db.read(db.snapshot(), Storage.CATALOG + table.name))))
                assertNull(db.read(db.snapshot(), requireNotNull(index.entry(table, item))))
                assertEquals("", BackfillRecords.decode(
                    db.read(db.snapshot(), BackfillRecords.index(table, index)), null).cursor)
            }
            // The same display name starts independently of the retired generation's progress.
            db.atomic { it.write(Storage.CATALOG + table.name,
                Records.encodeTable(table.copy(indexes = listOf(replacement)))) }
            worker.backfill(table, replacement)
            assertEquals("ACTIVE", Records.table(assertNotNull(
                db.read(db.snapshot(), Storage.CATALOG + table.name))).indexes.single().status)
            assertNotNull(db.read(db.snapshot(), requireNotNull(replacement.entry(table, item))))
        } finally {
            db.atomic { tx ->
                listOf(Storage.CATALOG + table.name, table.address(item),
                    BackfillRecords.index(table, index), requireNotNull(replacement.entry(table, item)))
                    .forEach { tx.write(it, null) }
            }
        }
    }

    @Test
    fun `ttl migrates legacy progress and rejects retired generations`() = withStorageSession { session, db ->
        val id = UUID.randomUUID().toString()
        val table = Table("ttl-generation-$id", id, "pk", 0, null)
        val first = mapOf("pk" to Value.text("aaa"), "expires" to Value.number(4_000_000_000L.toBigDecimal()))
        val last = first + ("pk" to Value.text("zzz"))
        val state = TtlState(table.name, id, "expires", true, id, table.address(first))
        val newer = state.copy(generation = UUID.randomUUID().toString(), cursor = "")
        val ttl = Ttl(db)
        val worker = TtlWorker(db, ttl)
        val candidates = listOf(state, newer).flatMap { policy -> listOf(first, last).map { item ->
            TtlRecords.address(requireNotNull(ttl.candidate(TtlPolicy(
                policy.attribute, policy.generation), table, item)), table.address(item))
        } }
        try {
            db.atomic { tx ->
                tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                tx.write(TtlRecords.config(id), TtlRecords.encode(state))
                listOf(first, last).forEach { tx.write(table.address(it), Records.encodeItem(it)) }
            }
            worker.backfill(state)
            assertNull(db.read(db.snapshot(), candidates[0]), "Legacy cursor must not restart from the beginning")
            assertNotNull(db.read(db.snapshot(), candidates[1]))
            assertNull(BackfillRecords.decode(db.read(db.snapshot(), BackfillRecords.ttl(state)), "").cursor)
            db.atomic {
                it.write(TtlRecords.config(id), TtlRecords.encode(newer))
                it.write(BackfillRecords.ttl(state), BackfillRecords.encode(BackfillProgress("")))
            }
            worker.backfill(state)
            assertEquals("", BackfillRecords.decode(db.read(db.snapshot(), BackfillRecords.ttl(state)), null).cursor)
            assertNull(db.read(db.snapshot(), BackfillRecords.ttl(newer)))
            Storage(session).use { other ->
                val second = TtlWorker(other, Ttl(other))
                val concurrent = CompletableFuture.runAsync { second.backfill(newer) }
                worker.backfill(newer)
                concurrent.get(30, TimeUnit.SECONDS)
            }
            assertNotNull(db.read(db.snapshot(), candidates[2]))
            assertNotNull(db.read(db.snapshot(), candidates[3]))
            db.atomic { tx ->
                tx.write(Storage.CATALOG + table.name, Records.encodeTable(table.copy(id = "new-$id")))
                tx.write(BackfillRecords.ttl(newer), null)
            }
            worker.backfill(newer)
            assertNull(db.read(db.snapshot(), BackfillRecords.ttl(newer)))
        } finally {
            db.atomic { tx ->
                (candidates + listOf(Storage.CATALOG + table.name, TtlRecords.config(id),
                    table.address(first), table.address(last), BackfillRecords.ttl(state), BackfillRecords.ttl(newer)))
                    .forEach { tx.write(it, null) }
            }
        }
    }

    @Test
    fun `unchanged tag requests do not invalidate catalog readers`() = withStorageSession { _, db ->
        val id = UUID.randomUUID().toString()
        val table = Table("tags-$id", id, "pk", 0, null, tags = mapOf("owner" to "same"))
        try {
            db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
            db.assertMetadataUnchanged(listOf(Storage.CATALOG + table.name)) {
                val catalog = Catalog(db)
                catalog.tags(obj("ResourceArn" to tableArn(table),
                    "Tags" to listOf(obj("Key" to "owner", "Value" to "same"))), "TagResource")
                catalog.tags(obj("ResourceArn" to tableArn(table), "TagKeys" to listOf("absent")), "UntagResource")
            }
            assertEquals(table, Records.table(assertNotNull(db.read(db.snapshot(), Storage.CATALOG + table.name))))
        } finally {
            db.atomic { it.write(Storage.CATALOG + table.name, null) }
        }
    }
}
