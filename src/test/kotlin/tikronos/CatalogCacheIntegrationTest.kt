package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import tikronos.model.Table
import tikronos.model.CatalogLease
import tikronos.storage.CatalogLeases
import tikronos.storage.AtomicCommit
import tikronos.storage.Records
import tikronos.storage.Storage

@Tag("tikv")
class CatalogCacheIntegrationTest {
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
