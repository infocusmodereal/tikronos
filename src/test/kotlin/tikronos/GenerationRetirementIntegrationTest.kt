package tikronos

import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import tikronos.model.Index
import tikronos.model.Value
import tikronos.storage.shard
import tikronos.model.Table
import tikronos.storage.BackfillRecords
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.StreamRecords
import tikronos.storage.prefix
import tikronos.storage.schema

@Tag("tikv")
class GenerationRetirementIntegrationTest {
    private fun id() = UUID.randomUUID().toString()

    @Test
    fun `restart after one batch and stale worker cannot erase a recreated generation or snapshot`() =
        withStorageSession { _, db ->
            val index = Index("lookup", id(), "pk", "S")
            val table = Table("rows-" + id(), id(), "pk", 1L, null, indexes = listOf(index))
            val next = table.copy(id = id(), indexes = listOf(index.copy(id = id())))
            val marker = byteArrayOf(1)
            val oldRows = (100..170).map { table.prefix() + it }
            val indexRow = index.schema(table).prefix() + "entry"
            db.atomic { tx ->
                tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                oldRows.forEach { tx.write(it, marker) }
                tx.write(indexRow, marker)
                tx.write(BackfillRecords.index(table, index), marker)
            }
            val stream = db.atomic { tx ->
                val spec = obj("StreamEnabled" to true, "StreamViewType" to "KEYS_ONLY")
                checkNotNull(Streams(db).configure(tx, table, spec))
            }
            val key = mapOf("pk" to Value.text("streamed"))
            ItemWriter(db).mutate(table, key) { key }
            val reads = StreamReads(db)
            val iterator = reads.iterator(obj("StreamArn" to stream.arn, "ShardId" to stream.shard,
                "ShardIteratorType" to "TRIM_HORIZON"))
            val before = db.snapshot()
            val tables = Tables(db)
            tables.update(obj("TableName" to table.name, "GlobalSecondaryIndexUpdates" to
                listOf(obj("Delete" to obj("IndexName" to index.name)))))
            tables.delete(obj("TableName" to table.name))
            val newRow = next.prefix() + "100"
            val newEntry = next.indexes.single().schema(next).prefix() + "entry"
            db.atomic { tx ->
                tx.write(Storage.CATALOG + table.name, Records.encodeTable(next))
                tx.write(newRow, marker)
                tx.write(newEntry, marker)
            }
            val job = RetiredRows.JOBS + table.id
            RetiredRows(db).advance(job, table.prefix())
            val resume = assertNotNull(db.read(db.snapshot(), job)).decodeToString()
            check(resume > table.prefix())
            RetiredRows(db).advance(job, table.prefix())
            assertEquals(resume, db.read(db.snapshot(), job)?.decodeToString())
            val worker = RetiredRows(db)
            var remaining = 200
            while (remaining-- > 0) {
                if (db.read(db.snapshot(), job) == null && db.read(db.snapshot(), indexRow) == null) break
                worker.tick()
            }
            val now = db.snapshot()
            assertNull(db.read(now, job))
            assertNull(db.read(now, indexRow))
            assertNull(db.read(now, BackfillRecords.index(table, index)))
            for (key in oldRows) {
                assertNull(db.read(now, key))
                assertNotNull(db.read(before, key))
            }
            assertNotNull(db.read(before, indexRow))
            assertNotNull(db.read(now, newRow))
            assertNotNull(db.read(now, newEntry))
            assertEquals("INSERT", reads.records(iterator)["Records"].single()["eventName"].asText())
        }

    @Test
    fun `filtered pages skip unrelated descriptors and survive deleting the cursor across authorities`() =
        withStorageSession { _, db ->
            val table = Table("list-" + id(), id(), "pk", 1L, null)
            val streams = Streams(db)
            val state = db.atomic { tx ->
                tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                val spec = obj("StreamEnabled" to true, "StreamViewType" to "KEYS_ONLY")
                checkNotNull(streams.configure(tx, table, spec))
            }
            val later = state.copy(arn = state.arn.replaceBefore(":000000000000", "arn:aws:dynamodb:zz-1"))
            val unrelated = StreamRecords.descriptor(state.arn.replace("/${table.name}/", "/${table.name}-extra/"))
            db.atomic { tx ->
                tx.write(StreamRecords.descriptor(later.arn), StreamRecords.encode(later))
                tx.write(unrelated, byteArrayOf(1))
            }
            try {
                val req = obj("TableName" to table.name, "Limit" to 1)
                val first = StreamReads(db).list(req)
                assertEquals(state.arn, first.string("LastEvaluatedStreamArn"))
                db.atomic { it.write(StreamRecords.descriptor(state.arn), null) }
                req.put("ExclusiveStartStreamArn", state.arn)
                val last = StreamReads(db).list(req)
                assertEquals(later.arn, last["Streams"].single()["StreamArn"].asText())
                assertNull(last["LastEvaluatedStreamArn"])
            } finally {
                db.atomic { it.write(unrelated, null) }
            }
        }

    @Test
    fun `retention keeps latest disabled descriptor and deletes detached generation only after expiry`() =
        withStorageSession { _, db ->
            val table = Table("stream-" + id(), id(), "pk", 1L, null)
            val streams = Streams(db)
            val enabled = obj("StreamEnabled" to true, "StreamViewType" to "NEW_IMAGE")
            val closed = db.atomic { tx ->
                tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                streams.configure(tx, table, enabled)
                checkNotNull(streams.disable(tx, table))
            }
            val key = StreamRecords.descriptor(closed.arn)
            val before = db.snapshot()
            val worker = StreamRetention(db)
            worker.prune(closed, checkNotNull(closed.closedAt))
            assertNotNull(streams.current(table, db.snapshot()))
            val active = db.atomic { checkNotNull(streams.configure(it, table, enabled)) }
            worker.prune(closed, closed.closedAt - 1)
            assertNotNull(db.read(db.snapshot(), key))
            worker.prune(closed, Instant.now().epochSecond)
            worker.deleteBatch(closed, listOf(1L))
            assertNull(db.read(db.snapshot(), key))
            assertNull(db.read(db.snapshot(), StreamRecords.trimKey(closed.arn)))
            assertNotNull(db.read(before, key))
            assertEquals(active, streams.current(table, db.snapshot()))
        }
}
