package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.Value
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.entry

@Tag("tikv")
class IndexBackfillIntegrationTest {
    @Test
    fun `one undecodable catalog entry does not starve a later backfill`() {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
            val db = Storage(session)
            val suffix = UUID.randomUUID().toString()
            val broken = Storage.CATALOG + "000broken-$suffix"
            val index = Index("byGroup", UUID.randomUUID().toString(), "group", "S", status = "CREATING")
            val table = Table("000working-$suffix", suffix, "pk", 0, null, hashed = true, indexes = listOf(index))
            val item = mapOf("pk" to Value.text("row"), "group" to Value.text("g"))
            val address = table.address(item)
            val entry = requireNotNull(index.entry(table, item))
            try {
                db.atomic { tx ->
                    tx.write(broken, byteArrayOf(99))
                    tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                    tx.write(address, Records.encodeItem(item))
                }
                IndexBackfill(db).tick()
                val current = Records.table(requireNotNull(db.read(db.snapshot(), Storage.CATALOG + table.name)))
                assertEquals("ACTIVE", current.indexes.single().status)
                assertNotNull(db.read(db.snapshot(), entry))
            } finally {
                db.atomic { tx ->
                    listOf(broken, Storage.CATALOG + table.name, address, entry).forEach { tx.write(it, null) }
                }
            }
        }
    }
}
