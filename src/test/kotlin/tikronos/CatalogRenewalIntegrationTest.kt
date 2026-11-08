package tikronos

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.txn.TwoPhaseCommitter
import tikronos.model.CatalogLease
import tikronos.model.Table
import tikronos.storage.CatalogLeases
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.TransactionResolution

@Tag("tikv")
class CatalogRenewalIntegrationTest {
    @Test
    fun `a stalled renewal does not block another reader of the table`() {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
            val db = Storage(session)
            val id = UUID.randomUUID().toString()
            val table = Table("renewal-$id", id, "pk", 0, null)
            val key = Storage.CATALOG + table.name
            val leaseKey = ByteString.copyFromUtf8(CatalogLeases.key(key))
            db.atomic { it.write(key, Records.encodeTable(table)) }
            val start = session.timestamp.version
            try {
                TwoPhaseCommitter(session, start, LOCK_TTL_MS).use { held ->
                    held.prewritePrimaryKey(ConcreteBackOffer.newCustomBackOff(WAIT_MS),
                        leaseKey.toByteArray(), CatalogLeases.encode(CatalogLease()))
                    assertReadersProceed(db, table) { TransactionResolution(session).resolve(leaseKey, start, 0) }
                }
            } finally {
                db.atomic { it.write(key, null) }
            }
        }
    }

    private fun assertReadersProceed(db: Storage, table: Table, release: () -> Unit) {
        val pool = Executors.newFixedThreadPool(2)
        try {
            val worker = AtomicReference<Thread>()
            val renewal = pool.submit<Table> {
                worker.set(Thread.currentThread())
                Tables(db).find(table.name)
            }
            try {
                val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS.toLong())
                while (!acquiring(worker.get()) && System.nanoTime() < deadline) Thread.sleep(5)
                assertTrue(acquiring(worker.get()), "The first reader must be inside lease acquisition")
                val reader = pool.submit<Table> { Tables(db).find(table.name) }
                assertEquals(table, reader.get(WAIT_MS.toLong(), TimeUnit.MILLISECONDS))
            } finally {
                release()
            }
            assertEquals(table, renewal.get(WAIT_MS.toLong(), TimeUnit.MILLISECONDS))
        } finally {
            pool.shutdownNow()
            assertTrue(pool.awaitTermination(WAIT_MS.toLong(), TimeUnit.MILLISECONDS))
        }
    }

    private fun acquiring(thread: Thread?): Boolean = thread?.stackTrace?.any {
        it.className == "tikronos.storage.CatalogCache" && it.methodName == "acquire"
    } == true

    companion object {
        private const val WAIT_MS = 1000
        private const val LOCK_TTL_MS = 10000L
    }
}
