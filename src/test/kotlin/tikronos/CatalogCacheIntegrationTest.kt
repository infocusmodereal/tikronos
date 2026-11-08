package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import tikronos.model.Table
import tikronos.model.CatalogLease
import tikronos.model.Value
import tikronos.storage.address
import tikronos.storage.TtlRecords
import tikronos.storage.CatalogLeases
import tikronos.storage.AtomicCommit
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
            val item = "tikronos-test/cache/${table.id}"
            var attempts = 0
            first.atomic { tx ->
                assertNotNull(tx.table(table.name))
                tx.write(item, byteArrayOf(1))
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
            ItemWriter(first).change(tx, table, key) { key }
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
            ItemWriter(first).change(tx, table, key) { item }
            val validation = tx.validation()
            assertTrue(validation.guards.isEmpty())
            Ttl(second).update(obj("TableName" to table.name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to true)))
            assertFalse(AtomicCommit(session).commit(tx.version, tx.writes, validation.guards,
                ceiling = validation.commitCeiling))
            val retry = first.transaction()
            ItemWriter(first).change(retry, table, key) { item }
            assertTrue(retry.writes.size > 1)
            assertTrue(retry.writes.keys.any { it != table.address(key) })
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

    private fun fixture(test: (Storage, Storage, TiSession, Table) -> Unit) {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
            val first = Storage(session)
            val second = Storage(session)
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
