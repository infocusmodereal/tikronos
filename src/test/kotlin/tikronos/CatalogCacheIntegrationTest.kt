package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiSession
import tikronos.model.Table
import tikronos.model.CatalogLease
import tikronos.model.Value
import tikronos.model.WriteIntent
import tikronos.model.PreviousItem
import tikronos.storage.address
import tikronos.storage.TtlRecords
import tikronos.storage.StreamRecords
import tikronos.storage.Transaction
import tikronos.storage.loadWritePlan
import tikronos.storage.CatalogLeases
import tikronos.storage.Prewrite
import tikronos.storage.AtomicCommit
import tikronos.model.BackfillProgress
import tikronos.storage.BackfillRecords
import tikronos.storage.Records
import tikronos.storage.Storage

@Tag("tikv")
class CatalogCacheIntegrationTest {
    @Test
    fun `overlapping renewals preserve coverage for older snapshots`() = fixture { first, _, _, table ->
        val key = Storage.CATALOG + table.name
        first.catalog.read(first.snapshot(), key)
        val old = first.snapshot()
        val initial = assertNotNull(first.catalog.cached(key, old.version))
        while (first.snapshot().version < initial.until - CatalogLeases.DURATION / 2) Thread.sleep(10)
        first.catalog.read(first.snapshot(), key)
        val renewed = assertNotNull(first.catalog.cached(key, old.version))
        assertTrue(renewed.until > initial.until)
        assertEquals(table, Tables(first).find(table.name, old))
    }

    @Test
    fun `identical catalog bytes after a drained lease cannot extend old snapshot coverage`() =
        fixture { first, second, _, table ->
            val key = Storage.CATALOG + table.name
            first.catalog.read(first.snapshot(), key)
            val old = first.snapshot()
            assertNotNull(first.catalog.cached(key, old.version))
            // TTL and stream configuration changes can deliberately leave the table bytes unchanged.
            second.atomic { it.write(key, Records.encodeTable(table)) }
            first.catalog.read(first.snapshot(), key)
            assertNull(first.catalog.cached(key, old.version))
            assertEquals(table, Tables(first).find(table.name, old))
        }

    @Test
    fun `expired leased attempt retries with full validation rather than starving a long write`() =
        fixture { first, _, _, table ->
            first.catalog.warmup()
            val key = mapOf("pk" to Value.text("row"))
            val item = table.address(key)
            var attempts = 0
            first.atomic { tx ->
                val result = ItemWriter(first).change(tx, table, key, WriteIntent.Replace(key))
                assertEquals(PreviousItem.Unread, result.previous)
                val validation = tx.validation()
                if (attempts++ == 0) {
                    assertTrue(validation.commitCeiling < Long.MAX_VALUE)
                    while (first.snapshot().version <= validation.commitCeiling) Thread.sleep(25)
                } else {
                    assertEquals(Long.MAX_VALUE, validation.commitCeiling)
                    assertTrue(validation.guards.isNotEmpty())
                }
            }
            assertEquals(2, attempts)
            first.mutate(item) { null }
        }

    @Test
    fun `plain leased writes avoid metadata locks and refuse commit after schema replacement`() =
        fixture { first, second, session, table ->
            first.catalog.warmup()
            val tx = first.transaction()
            val key = mapOf("pk" to Value.text("row"))
            ItemWriter(first).change(tx, table, key, WriteIntent.Replace(key))
            val validation = tx.validation()
            assertTrue(validation.guards.isEmpty())
            assertTrue(validation.commitCeiling < Long.MAX_VALUE)
            assertEquals(setOf(table.address(key)), tx.writes.keys)
            second.atomic { it.write(Storage.CATALOG + table.name,
                Records.encodeTable(table.copy(id = UUID.randomUUID().toString()))) }
            assertFalse(AtomicCommit(session).commit(tx.version, tx.writes, validation.guards,
                ceiling = validation.commitCeiling))
            assertNull(first.read(first.snapshot(), table.address(key)))
        }

    @Test
    fun `TTL enable drains leased writes and retry records the expiration candidate`() =
        fixture { first, second, session, table ->
            first.catalog.warmup()
            val tx = first.transaction()
            val key = mapOf("pk" to Value.text("row"))
            val item = key + ("expires" to Value.number("2000000000".toBigDecimal()))
            ItemWriter(first).change(tx, table, key, WriteIntent.Replace(item))
            val validation = tx.validation()
            assertTrue(validation.guards.isEmpty())
            Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to true)))
            assertFalse(AtomicCommit(session).commit(tx.version, tx.writes, validation.guards,
                ceiling = validation.commitCeiling))
            val retry = first.transaction()
            ItemWriter(first).change(retry, table, key, WriteIntent.Replace(item))
            assertTrue(retry.writes.size > 1)
            assertTrue(retry.writes.keys.any { it != table.address(key) })
            Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to false)))
            val disabled = first.transaction()
            assertEquals(PreviousItem.Unread,
                ItemWriter(first).change(disabled, table, key, WriteIntent.Replace(item)).previous)
            second.atomic { it.write(TtlRecords.config(table.id), null) }
        }

    @Test
    fun `another instance drains cached definitions and stale writes still conflict`() =
        fixture { first, second, session, table ->
        val key = Storage.CATALOG + table.name
        first.read(first.snapshot(), key)
        val old = first.transaction()
        assertNotNull(first.catalog.cached(key, old.version))
        assertEquals(table.id, old.table(table.name)?.id)
        val item = "tikronos-test/cache/${table.id}"
        old.write(item, byteArrayOf(1))
        val replacement = table.copy(id = UUID.randomUUID().toString(), partitionKey = "replacement")
        second.atomic { it.write(key, null) }
        assertNull(first.read(first.snapshot(), key))
        second.atomic { it.write(key, Records.encodeTable(replacement)) }
        assertEquals(replacement.id, Tables(first).find(table.name).id)
        assertFalse(AtomicCommit(session).commit(old.version, old.writes, old.guards()))
        assertNull(first.read(first.snapshot(), item))
    }

    @Test
    fun `startup discovers durable definitions and older snapshots retain their schema`() =
        fixture { first, second, _, table ->
        val key = Storage.CATALOG + table.name
        val oldSnapshot = first.snapshot()
        val changed = table.copy(tags = mapOf("generation" to "new"))
        second.atomic { it.write(key, Records.encodeTable(changed)) }
        first.catalog.warmup()
        val current = first.snapshot()
        assertNotNull(first.catalog.cached(key, current.version))
        assertEquals(changed, Tables(first).find(table.name, current))
        assertEquals(table, Tables(first).find(table.name, oldSnapshot))
    }

    @Test
    fun `an abandoned catalog writer blocks renewal only until its PD deadline`() =
        fixture { first, second, _, table ->
            val key = Storage.CATALOG + table.name
            val until = second.uncachedAtomic { tx ->
                val deadline = tx.version + CatalogLeases.DURATION
                tx.write(CatalogLeases.key(key), CatalogLeases.encode(CatalogLease(0, deadline)))
                deadline
            }
            assertEquals(table, Tables(first).find(table.name))
            assertNull(first.catalog.cached(key, first.snapshot().version))
            while (first.snapshot().version <= until) Thread.sleep(25)
            first.read(first.snapshot(), key)
            assertNotNull(first.catalog.cached(key, first.snapshot().version))
        }

    @Test
    fun `a newly granted lease conflicts with a previously prepared schema change`() =
        fixture { first, second, session, table ->
            val key = Storage.CATALOG + table.name
            val ddl = second.transaction()
            ddl.write(key, Records.encodeTable(table.copy(partitionKey = "newKey")))
            assertTrue(second.catalog.prepare(ddl))
            first.read(first.snapshot(), key)
            assertNotNull(first.catalog.cached(key, first.snapshot().version))
            assertFalse(AtomicCommit(session).commit(ddl.version, ddl.writes, ddl.guards()))
            assertEquals(table, Tables(first).find(table.name))
        }

    @Test
    fun `unread replacements retain native conflicts and capacity requests read the old item`() =
        fixture { first, _, session, table ->
            first.catalog.warmup()
            val key = mapOf("pk" to Value.text("row"))
            val item = key + ("data" to Value.text("x".repeat(2048)))
            val writer = ItemWriter(first)
            val stale = first.transaction()
            val planned = writer.change(stale, table, key, WriteIntent.Replace(key))
            assertEquals(PreviousItem.Unread, planned.previous)
            assertTrue(stale.validation().guards.isEmpty())
            writer.mutate(table, key, WriteIntent.Replace(item))
            assertFalse(AtomicCommit(session).commit(stale.version, stale.writes, stale.validation().guards))
            val response = Items(first, Tables(first)).write(obj("TableName" to table.name,
                "Item" to obj("pk" to obj("S" to "row")), "ReturnConsumedCapacity" to "TOTAL"), "PutItem")
            assertFalse(response.has("Attributes"))
            assertEquals(3.0, response["ConsumedCapacity"]["CapacityUnits"].doubleValue())
            val removed = Items(first, Tables(first)).write(obj("TableName" to table.name,
                "Key" to obj("pk" to obj("S" to "row")), "ReturnValues" to "ALL_OLD"), "DeleteItem")
            assertEquals("row", removed["Attributes"]["pk"]["S"].textValue())
        }

    @Test
    fun `stream enable invalidates an absent plan and retries preserve images and fresh heads`() =
        fixture { first, second, session, table ->
            first.catalog.warmup()
            val key = mapOf("pk" to Value.text("row"))
            val writer = ItemWriter(first)
            writer.mutate(table, key, WriteIntent.Replace(key))
            val stale = first.transaction()
            writer.change(stale, table, key, WriteIntent.Replace(null))
            second.atomic { Streams(second).configure(it, table,
                obj("StreamEnabled" to true, "StreamViewType" to "NEW_AND_OLD_IMAGES")) }
            val validation = stale.validation()
            assertFalse(AtomicCommit(session).commit(stale.version, stale.writes, validation.guards,
                ceiling = validation.commitCeiling))
            val result = writer.mutate(table, key, WriteIntent.Replace(null))
            assertEquals(key, result.old)
            writer.mutate(table, key, WriteIntent.Replace(key))
            val arn = requireNotNull(Streams(second).current(table, second.snapshot())).arn
            val state = StreamRecords.decode(checkNotNull(first.read(first.snapshot(),
                StreamRecords.descriptor(arn))))
            val prefix = StreamRecords.events(state)
            val events = first.ranges.scan(tikronos.storage.bytes(prefix),
                tikronos.storage.prefixEnd(prefix), true).map { json.readTree(it.value.toByteArray()) }.toList()
            assertEquals(listOf("REMOVE", "INSERT"), events.map { it["eventName"].asText() })
            second.atomic { Streams(second).disable(it, table) }
            assertEquals(PreviousItem.Unread, writer.mutate(table, key, WriteIntent.Replace(null)).previous)
            second.atomic { tx ->
                tx.write(StreamRecords.config(table.id), null)
                tx.write(StreamRecords.descriptor(arn), null)
            }
        }

    @Test
    fun `lease acquisition ignores operational progress but still validates semantic changes`() =
        fixture { first, second, session, table ->
            second.atomic { Streams(second).configure(it, table,
                obj("StreamEnabled" to true, "StreamViewType" to "KEYS_ONLY")) }
            Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to true)))
            val tx = Transaction(first.snapshot(), first, cached = false)
            val current = assertNotNull(tx.table(table.name))
            val plan = tx.loadWritePlan(current)
            val leaseKey = CatalogLeases.key(Storage.CATALOG + table.name)
            tx.read(leaseKey)
            tx.write(leaseKey, CatalogLeases.encode(CatalogLease(tx.version + CatalogLeases.DURATION)))
            val descriptor = StreamRecords.descriptor(checkNotNull(plan.streamArn))
            val ttlKey = TtlRecords.config(table.id)
            val state = TtlRecords.state(checkNotNull(second.read(second.snapshot(), ttlKey)))
            val progressKey = BackfillRecords.ttl(state)
            second.uncachedAtomic { progress ->
                val stream = StreamRecords.decode(checkNotNull(progress.read(descriptor)))
                progress.write(descriptor, StreamRecords.encode(stream.copy(head = progress.version)))
                progress.write(progressKey, BackfillRecords.encode(BackfillProgress("progress")))
            }
            // This executes the same snapshot loader and native commit used by cache acquisition.
            assertEquals(1, tx.validation().guards.size)
            assertFalse(tx.validation().guards.contains(descriptor))
            assertTrue(AtomicCommit(session).commit(tx.version, tx.writes, tx.validation().guards))
            val stale = Transaction(first.snapshot(), first, cached = false)
            stale.loadWritePlan(assertNotNull(stale.table(table.name)))
            val item = mapOf("pk" to Value.text("stale"))
            stale.write(table.address(item), Records.encodeItem(item))
            assertEquals(1, stale.validation().guards.size)
            Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to false)))
            assertFalse(AtomicCommit(session).commit(stale.version, stale.writes, stale.validation().guards))
            second.atomic { Streams(second).disable(it, table) }
            second.atomic { progress ->
                progress.write(descriptor, null)
                progress.write(StreamRecords.config(table.id), null)
                progress.write(ttlKey, null)
                progress.write(progressKey, null)
            }
        }

    @Test
    fun `delayed one phase prewrite cannot cross a drained catalog lease after a reader`() =
        fixture { first, second, session, table ->
            val item = mapOf("pk" to Value.text("delayed"))
            first.read(first.snapshot(), Storage.CATALOG + table.name)
            val tx = first.transaction()
            ItemWriter(first).change(tx, table, item, WriteIntent.Replace(item))
            val validation = tx.validation()
            assertTrue(validation.guards.isEmpty())
            var sends = 0
            val prewrite = Prewrite(session) { stub, request ->
                assertTrue(request.tryOnePc)
                sends++
                Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                    obj("AttributeName" to "expires", "Enabled" to true)))
                // A post-DDL reader must not miss a later old-schema write at an earlier timestamp.
                session.createSnapshot().get(tikronos.storage.bytes(table.address(item)))
                stub.kvPrewrite(request).also { assertEquals(0L, it.onePcCommitTs) }
            }
            assertFalse(AtomicCommit(session, prewriter = prewrite)
                .commit(tx.version, tx.writes, validation.guards, ceiling = validation.commitCeiling))
            assertEquals(1, sends)
            assertNull(first.read(first.snapshot(), table.address(item)))
            Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to false)))
            second.atomic { it.write(TtlRecords.config(table.id), null) }
        }

    @Suppress("NestedBlockDepth") // Two independent API instances share a test session.
    private fun fixture(test: (Storage, Storage, TiSession, Table) -> Unit) {
        withStorageSession { session, first ->
            Storage(session).use { second ->
                val id = UUID.randomUUID().toString()
                val table = Table("000cache-$id", id, "pk", 0, null)
                val key = Storage.CATALOG + table.name
                try {
                    second.atomic { it.write(key, Records.encodeTable(table)) }
                    test(first, second, session, table)
                } finally {
                    second.atomic { it.write(key, null) }
                }
            }
        }
    }
}
