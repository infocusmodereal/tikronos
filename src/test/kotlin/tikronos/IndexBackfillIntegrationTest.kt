package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertNull
import kotlin.test.assertNotNull
import org.junit.jupiter.api.Tag
import tikronos.model.Index
import tikronos.model.Table
import tikronos.model.Value
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.address
import tikronos.storage.entry
import tikronos.storage.bytes
import tikronos.storage.key
import tikronos.storage.prefix
import tikronos.storage.prefixEnd
import tikronos.storage.schema

@Tag("tikv")
class IndexBackfillIntegrationTest {
    @Test
    fun `backfill resumes its checkpoint after writes before and after the cursor`() {
        withStorageSession { session, db ->
            val id = UUID.randomUUID().toString()
            val index = Index("byGroup", UUID.randomUUID().toString(), "group", "S", status = "CREATING")
            val table = Table("000resume-$id", id, "pk", 0, null, hashed = true, indexes = listOf(index))
            val rows = (0 until 96).map {
                mapOf("pk" to Value.text("row-$it"), "group" to Value.text("original"))
            }.sortedBy { table.address(it) }
            fun current() = Records.table(requireNotNull(db.read(db.snapshot(), Storage.CATALOG + table.name)))
            fun stored(prefix: String) = db.ranges.scan(bytes(prefix), prefixEnd(prefix), true).toList()
            try {
                rows.chunked(16).forEach { chunk -> db.atomic { tx ->
                    chunk.forEach { tx.write(table.address(it), Records.encodeItem(it)) }
                } }
                db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
                IndexBackfill(db).tick()
                val checkpoint = requireNotNull(current().indexes.single().cursor)
                val passed = rows.filter { table.address(it) <= checkpoint }
                val pending = rows.filter { table.address(it) > checkpoint }
                assertTrue(passed.size >= 2 && pending.size >= 2)
                val writer = ItemWriter(db)
                for (item in listOf(passed.first(), pending.last())) {
                    writer.mutate(table, table.key(item, false)) { item + ("group" to Value.text("moved")) }
                }
                for (item in listOf(passed[1], pending[pending.size - 2])) {
                    writer.mutate(table, table.key(item, false)) { null }
                }
                val resumed = IndexBackfill(db)
                resumed.tick()
                val progressed = current().indexes.single()
                assertTrue(progressed.status == "ACTIVE" || requireNotNull(progressed.cursor) > checkpoint)
                repeat(4) { resumed.tick() }
                assertEquals("ACTIVE", current().indexes.single().status)
                val base = stored(table.prefix()).map { Records.item(it.value.toByteArray()) }.toSet()
                val indexed = stored(index.schema(table).prefix()).map { Records.item(it.value.toByteArray()) }.toSet()
                assertEquals(rows.size - 2, base.size)
                assertEquals(base, indexed)
            } finally {
                val addresses = stored(table.prefix()).map { it.key.toStringUtf8() } +
                    stored(index.schema(table).prefix()).map { it.key.toStringUtf8() } + (Storage.CATALOG + table.name)
                addresses.chunked(16).forEach { chunk -> db.atomic { tx ->
                    chunk.forEach { tx.write(it, null) }
                } }
            }
        }
    }

    @Test
    fun `decodable corrupt primary keys do not become active indexes`() {
        withStorageSession { session, db ->
            val id = UUID.randomUUID().toString()
            val index = Index("byGroup", UUID.randomUUID().toString(), "group", "S", status = "CREATING")
            val table = Table("000corrupt-$id", id, "pk", 0, null, hashed = true, indexes = listOf(index))
            val valid = mapOf("pk" to Value.text("row"), "group" to Value.text("g"))
            val address = table.address(valid)
            val corruptions = listOf(valid - "pk", valid + ("pk" to Value.number(1.toBigDecimal())),
                valid + ("pk" to Value.text("wrong-address")))
            try {
                for (corrupt in corruptions) {
                    db.atomic { tx ->
                        tx.write(Storage.CATALOG + table.name, Records.encodeTable(table))
                        tx.write(address, Records.encodeItem(corrupt))
                    }
                    IndexBackfill(db).tick()
                    val current = Records.table(requireNotNull(db.read(db.snapshot(), Storage.CATALOG + table.name)))
                    assertEquals("CREATING", current.indexes.single().status)
                    assertNull(current.indexes.single().cursor)
                    val prefix = index.schema(table).prefix()
                    assertTrue(db.ranges.scan(bytes(prefix), prefixEnd(prefix), true).none())
                }
            } finally {
                db.atomic { tx ->
                    tx.write(Storage.CATALOG + table.name, null)
                    tx.write(address, null)
                }
            }
        }
    }

    @Test
    fun `one undecodable catalog entry does not starve a later backfill`() {
        withStorageSession { session, db ->
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
