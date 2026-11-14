package tikronos

import java.util.UUID
import java.util.concurrent.Executor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import tikronos.storage.Storage
import tikronos.storage.table

@Tag("tikv")
class MetadataSnapshotIntegrationTest {
    @Test
    fun `pinned descriptions retain the table stream and TTL across recreation`() = withStorageSession { session, db ->
        val name = "metadata-${UUID.randomUUID()}"
        val request = obj("TableName" to name)
        val tables = Tables(db)
        val original = tables.create(createRequest(name, "pk", "NEW_IMAGE")).obj("TableDescription")
        try {
            Ttl(db).update(obj("TableName" to name, "TimeToLiveSpecification" to
                obj("AttributeName" to "expires", "Enabled" to true)))
            val key = obj("pk" to obj("S" to "row"))
            val item = obj("pk" to key["pk"], "note" to obj("S" to "original"))
            Items(db, tables).write(obj("TableName" to name, "Item" to item), "PutItem")
            Storage(session).use { reader ->
                val before = reader.snapshot()
                val deleted = tables.delete(request).obj("TableDescription")
                assertFalse(deleted.obj("StreamSpecification").flag("StreamEnabled"))
                assertEquals(original.string("LatestStreamArn"), deleted.string("LatestStreamArn"))

                val replacement = tables.create(createRequest(name, "other", "KEYS_ONLY")).obj("TableDescription")
                val latest = Tables(reader).describe(request).obj("Table")
                assertEquals(replacement, latest)
                assertNotEquals(original.string("TableId"), latest.string("TableId"))
                assertEquals(original, Tables(reader).describe(name, before))
                val projected = obj("TableName" to name, "Key" to key, "ProjectionExpression" to "note")
                val result = Items(reader, Tables(reader)).get(projected, before)
                assertEquals(obj("Item" to obj("note" to item["note"])), result)
                val ttl = Ttl(reader)
                assertEquals(obj("TimeToLiveStatus" to "ENABLED", "AttributeName" to "expires"),
                    ttl.describe(name, before).obj("TimeToLiveDescription"))
                assertEquals("DISABLED", ttl.describe(request).obj("TimeToLiveDescription").string("TimeToLiveStatus"))
            }
        } finally {
            if (db.catalog.table(db.snapshot(), name) != null) tables.delete(request)
        }
    }

    @Test
    fun `create response retains its committed stream if another writer disables it before return`() =
        withStorageSession { session, db ->
            val name = "metadata-${UUID.randomUUID()}"
            val request = obj("TableName" to name)
            val tables = Tables(db)
            // Complete the create's secondary writes, then interleave an update before the response is built.
            val afterCommit = Executor { cleanup ->
                cleanup.run()
                tables.update(obj("TableName" to name, "StreamSpecification" to obj("StreamEnabled" to false)))
            }
            Storage(session, cleanup = afterCommit).use { creator ->
                try {
                    val created = Tables(creator).create(createRequest(name, "pk", "KEYS_ONLY")).obj("TableDescription")
                    val latest = tables.describe(request).obj("Table")
                    assertTrue(created.obj("StreamSpecification").flag("StreamEnabled"))
                    assertFalse(latest.obj("StreamSpecification").flag("StreamEnabled"))
                    assertEquals(created.string("LatestStreamArn"), latest.string("LatestStreamArn"))
                } finally {
                    if (db.catalog.table(db.snapshot(), name) != null) tables.delete(request)
                }
            }
        }

    private fun createRequest(name: String, key: String, view: String) = obj(
        "TableName" to name, "BillingMode" to "PAY_PER_REQUEST",
        "AttributeDefinitions" to listOf(obj("AttributeName" to key, "AttributeType" to "S")),
        "KeySchema" to listOf(obj("AttributeName" to key, "KeyType" to "HASH")),
        "StreamSpecification" to obj("StreamEnabled" to true, "StreamViewType" to view),
    )
}
