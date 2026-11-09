package tikronos

import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import org.junit.jupiter.api.Tag
import tikronos.model.StreamPosition
import tikronos.model.Table
import tikronos.model.Value
import tikronos.model.WriteIntent
import tikronos.model.policy
import tikronos.storage.Records
import tikronos.storage.Storage
import tikronos.storage.StreamRecords
import tikronos.storage.shard
import tikronos.storage.TtlRecords
import tikronos.storage.address
import tikronos.storage.timestampStartOfSecond

@Tag("tikv")
class StreamTtlIntegrationTest {
    private val createdTables = mutableListOf<Table>()
    @Test
    fun `images and iterators survive new service instances while reenable rotates stream`() = storage { db ->
        for (view in listOf("KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES")) {
            val table = table(db)
            val streams = Streams(db)
            db.atomic { streams.configure(it, table, obj("StreamEnabled" to true, "StreamViewType" to view)) }
            val arn = streams.decorate(table, obj()).string("LatestStreamArn")
            val writer = ItemWriter(db)
            val key = mapOf("pk" to Value.text("row"))
            val first = key + ("data" to Value.text("one"))
            val second = key + ("data" to Value.text("two"))
            writer.mutate(table, key, WriteIntent.Replace(first))
            writer.mutate(table, key, WriteIntent.Replace(second))
            writer.mutate(table, key, WriteIntent.Replace(second))
            writer.mutate(table, key, WriteIntent.Replace(null))
            val reads = StreamReads(db)
            val shard = reads.describe(obj("StreamArn" to arn)).path("StreamDescription").path("Shards")[0]
                .path("ShardId").textValue()
            val token = reads.iterator(obj("StreamArn" to arn, "ShardId" to shard,
                "ShardIteratorType" to "TRIM_HORIZON")).string("ShardIterator")
            val records = StreamReads(db).records(obj("ShardIterator" to token)).path("Records")
            assertEquals(listOf("INSERT", "MODIFY", "REMOVE"), records.map { it.path("eventName").textValue() })
            val update = records[1].path("dynamodb")
            assertEquals(view in setOf("NEW_IMAGE", "NEW_AND_OLD_IMAGES"), update.has("NewImage"))
            assertEquals(view in setOf("OLD_IMAGE", "NEW_AND_OLD_IMAGES"), update.has("OldImage"))
            db.atomic { streams.disable(it, table) }
            assertFalse(StreamReads(db).records(obj("ShardIterator" to token)).has("NextShardIterator"))
            db.atomic { streams.configure(it, table, obj("StreamEnabled" to true, "StreamViewType" to view)) }
            val newArn = streams.decorate(table, obj()).string("LatestStreamArn")
            assertNotEquals(arn, newArn)
            val newShard = reads.describe(obj("StreamArn" to newArn)).path("StreamDescription").path("Shards")[0]
                .path("ShardId").textValue()
            assertNotEquals(shard, newShard)
            assertEquals("ResourceNotFoundException", assertFailsWith<ApiError> {
                reads.iterator(obj("StreamArn" to newArn, "ShardId" to shard, "ShardIteratorType" to "TRIM_HORIZON"))
            }.code)
            assertEquals(3, StreamReads(db).records(obj("ShardIterator" to token)).path("Records").size())
        }
    }

    @Test
    fun `disabled empty stream has an ordered closed sequence range`() = storage { db ->
        val table = table(db)
        val streams = Streams(db)
        db.atomic { streams.configure(it, table, obj("StreamEnabled" to true, "StreamViewType" to "KEYS_ONLY")) }
        val arn = streams.decorate(table, obj()).string("LatestStreamArn")
        db.atomic { streams.disable(it, table) }
        val shard = StreamReads(db).describe(obj("StreamArn" to arn))
            .path("StreamDescription").path("Shards")[0]
        val range = shard.path("SequenceNumberRange")
        check(range.path("StartingSequenceNumber").asText().toLong() <=
            range.path("EndingSequenceNumber").asText().toLong())
    }

    @Test
    fun `ttl discovers backfill from new service instance and protects renewed item`() = storage { db ->
        val table = table(db)
        val writer = ItemWriter(db)
        val now = Instant.now().epochSecond
        val key = mapOf("pk" to Value.text("row"))
        val old = key + ("expires" to Value.number((now - 1).toBigDecimal()))
        writer.mutate(table, key) { old }
        val streams = Streams(db)
        db.atomic { streams.configure(it, table, obj("StreamEnabled" to true, "StreamViewType" to "OLD_IMAGE")) }
        Ttl(db).update(obj("TableName" to table.name,
            "TimeToLiveSpecification" to obj("Enabled" to true, "AttributeName" to "expires")))
        val ttl = Ttl(db)
        val config = TtlRecords.state(requireNotNull(db.read(db.snapshot(), TtlRecords.config(table.id))))
        val stale = requireNotNull(ttl.candidate(checkNotNull(config.policy), table, old))
        val renewed = key + ("expires" to Value.number((now + FUTURE_SECONDS).toBigDecimal()))
        writer.mutate(table, key) { renewed }
        db.atomic { it.write(TtlRecords.address(stale, table.address(key)), TtlRecords.encode(stale)) }
        TtlWorker(db, Ttl(db)).tick(writer)
        assertEquals(renewed, Records.item(requireNotNull(db.read(db.snapshot(), table.address(key)))))
        writer.mutate(table, key) { old }
        TtlWorker(db, Ttl(db)).tick(writer)
        assertEquals(null, db.read(db.snapshot(), table.address(key)))
        val arn = streams.decorate(table, obj()).string("LatestStreamArn")
        val reads = StreamReads(db)
        val shard = reads.describe(obj("StreamArn" to arn)).path("StreamDescription").path("Shards")[0]
            .path("ShardId").textValue()
        val token = reads.iterator(obj("StreamArn" to arn, "ShardId" to shard,
            "ShardIteratorType" to "TRIM_HORIZON")).string("ShardIterator")
        val events = reads.records(obj("ShardIterator" to token)).path("Records")
        assertEquals("REMOVE", events.last().path("eventName").textValue())
        assertEquals("dynamodb.amazonaws.com", events.last().path("userIdentity").path("PrincipalId").textValue())
    }

    @Test
    fun `ttl backfills an untouched expired item and skips an earlier poison candidate`() = storage { db ->
        val table = table(db)
        val writer = ItemWriter(db)
        val key = mapOf("pk" to Value.text("existing"))
        val item = key + ("expires" to Value.number((Instant.now().epochSecond - 1).toBigDecimal()))
        writer.mutate(table, key) { item }
        Ttl(db).update(obj("TableName" to table.name,
            "TimeToLiveSpecification" to obj("Enabled" to true, "AttributeName" to "expires")))
        val poison = TtlRecords.CANDIDATES + "00000000000000000000/" + UUID.randomUUID()
        db.atomic { it.write(poison, "invalid candidate".encodeToByteArray()) }
        try {
            val worker = TtlWorker(db, Ttl(db))
            advanceUntil({ db.read(db.snapshot(), table.address(key)) == null }) { worker.tick(writer) }
            assertEquals(null, db.read(db.snapshot(), table.address(key)))
        } finally {
            db.atomic { it.write(poison, null) }
        }
    }

    @Test
    fun `ttl resumes a persisted page after writer changes on both sides of its cursor`() = storage { db ->
        // An early UUID keeps this fixture in the first bounded config page of a reused lab.
        val table = table(db, "00000000-" + UUID.randomUUID())
        val writer = ItemWriter(db)
        val now = Instant.now().epochSecond
        val keys = (0 until BACKFILL_ROWS).map { mapOf("pk" to Value.text("row-$it")) }
            .sortedBy { table.address(it) }
        for (key in keys) {
            val expiry = if (key == keys.last()) now - 1 else now + FUTURE_SECONDS
            writer.mutate(table, key) { key + ("expires" to Value.number(expiry.toBigDecimal())) }
        }
        Ttl(db).update(obj("TableName" to table.name,
            "TimeToLiveSpecification" to obj("Enabled" to true, "AttributeName" to "expires")))
        TtlWorker(db, Ttl(db)).tick(writer)
        val checkpoint = TtlRecords.state(requireNotNull(db.read(db.snapshot(), TtlRecords.config(table.id))))
        val cursor = requireNotNull(checkpoint.cursor)
        check(cursor.isNotEmpty() && cursor < table.address(keys.last()))
        val behind = keys.first { table.address(it) < cursor }
        val ahead = keys.first { table.address(it) > cursor }
        val renewed = behind + ("expires" to Value.number((now + FUTURE_SECONDS + 1).toBigDecimal()))
        writer.mutate(table, behind) { renewed }
        writer.mutate(table, ahead) { ahead + ("expires" to Value.number((now - 1).toBigDecimal())) }
        val resumed = TtlWorker(db, Ttl(db))
        resumed.tick(writer)
        assertEquals(null,
            TtlRecords.state(requireNotNull(db.read(db.snapshot(), TtlRecords.config(table.id)))).cursor)
        advanceUntil({
            TtlRecords.state(requireNotNull(db.read(db.snapshot(), TtlRecords.config(table.id)))).cursor == null &&
                db.read(db.snapshot(), table.address(ahead)) == null &&
                db.read(db.snapshot(), table.address(keys.last())) == null
        }) { resumed.tick(writer) }
        assertEquals(renewed, Records.item(requireNotNull(db.read(db.snapshot(), table.address(behind)))))
        assertEquals(null, db.read(db.snapshot(), table.address(ahead)))
        assertEquals(null, db.read(db.snapshot(), table.address(keys.last())))
    }

    @Test
    fun `retention is enforced before cleanup and iterator authentication survives instances`() = storage { db ->
        val table = table(db)
        val streams = Streams(db)
        db.atomic { streams.configure(it, table,
            obj("StreamEnabled" to true, "StreamViewType" to "KEYS_ONLY")) }
        val arn = streams.decorate(table, obj()).string("LatestStreamArn")
        val key = mapOf("pk" to Value.text("row"))
        ItemWriter(db).mutate(table, key) { key }
        val state = StreamRecords.decode(requireNotNull(db.read(db.snapshot(), StreamRecords.descriptor(arn))))
        val expiredAt = Instant.now().epochSecond - STREAM_RETENTION_SECONDS - 1
        val expiredSequence = timestampStartOfSecond(expiredAt)
        db.atomic { tx ->
            tx.write(StreamRecords.events(state) + StreamRecords.sequence(expiredSequence), json.writeValueAsBytes(
                obj("dynamodb" to obj("ApproximateCreationDateTime" to expiredAt,
                    "SequenceNumber" to StreamRecords.sequence(expiredSequence)))))
        }
        val reads = StreamReads(db)
        val horizon = reads.iterator(obj("StreamArn" to arn, "ShardId" to state.shard,
            "ShardIteratorType" to "TRIM_HORIZON")).string("ShardIterator")
        assertEquals(1, reads.records(obj("ShardIterator" to horizon)).path("Records").size())
        val old = StreamTokens(db).encode(StreamPosition(arn, 0, Instant.now().epochSecond + FUTURE_SECONDS))
        assertEquals("TrimmedDataAccessException", assertFailsWith<ApiError> {
            reads.records(obj("ShardIterator" to old))
        }.code)
        val expired = StreamTokens(db).encode(StreamPosition(arn, 0, Instant.now().epochSecond - 1))
        assertEquals("ExpiredIteratorException", assertFailsWith<ApiError> {
            StreamReads(db).records(obj("ShardIterator" to expired))
        }.code)
        assertEquals("ValidationException", assertFailsWith<ApiError> {
            StreamReads(db).records(obj("ShardIterator" to horizon.dropLast(1) + "!"))
        }.code)
        val poison = StreamRecords.PREFIX + "!poison-" + UUID.randomUUID()
        val expiredAddress = StreamRecords.events(state) + StreamRecords.sequence(expiredSequence)
        db.atomic { it.write(poison, "invalid stream descriptor".encodeToByteArray()) }
        try {
            val retention = StreamRetention(db)
            advanceUntil({ db.read(db.snapshot(), expiredAddress) == null }) { retention.tick() }
            assertEquals(null, db.read(db.snapshot(), expiredAddress))
        } finally {
            db.atomic { it.write(poison, null) }
        }
    }

    @Test
    fun `aborted and conflicting mutations leave no phantom stream events`() = storage { db ->
        val table = table(db)
        val streams = Streams(db)
        db.atomic { streams.configure(it, table,
            obj("StreamEnabled" to true, "StreamViewType" to "NEW_IMAGE")) }
        val arn = streams.decorate(table, obj()).string("LatestStreamArn")
        val writer = ItemWriter(db)
        val key = mapOf("pk" to Value.text("row"))
        assertFailsWith<IllegalStateException> {
            db.atomic { tx ->
                writer.change(tx, table, key) { key }
                error("Abort before prewrite")
            }
        }
        assertEquals(null, db.read(db.snapshot(), table.address(key)))
        var attempts = 0
        var abortedSequence = 0L
        db.atomic { tx ->
            writer.change(tx, table, key) { key }
            attempts++
            if (attempts == 1) {
                abortedSequence = tx.version
                db.mutate(Storage.CATALOG + table.name) { it }
            }
        }
        assertEquals(2, attempts)
        val state = StreamRecords.decode(requireNotNull(db.read(db.snapshot(), StreamRecords.descriptor(arn))))
        assertEquals(null, db.read(db.snapshot(),
            StreamRecords.events(state) + StreamRecords.sequence(abortedSequence)))
        val reads = StreamReads(db)
        val horizon = reads.iterator(obj("StreamArn" to arn, "ShardId" to state.shard,
            "ShardIteratorType" to "TRIM_HORIZON")).string("ShardIterator")
        assertEquals(1, reads.records(obj("ShardIterator" to horizon)).path("Records").size())
    }

    private fun advanceUntil(done: () -> Boolean, tick: () -> Unit) {
        repeat(MAX_TICKS) {
            if (done()) return
            tick()
        }
        check(done()) { "Background work did not advance" }
    }

    private fun table(db: Storage, id: String = UUID.randomUUID().toString()): Table {
        val table = Table("test-$id", id, "pk", Instant.now().epochSecond, null, hashed = true)
        db.atomic { it.write(Storage.CATALOG + table.name, Records.encodeTable(table)) }
        createdTables += table
        return table
    }

    private fun storage(action: (Storage) -> Unit) {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        withStorageSession(endpoint) { _, db ->
            try {
                action(db)
            } finally {
                for (table in createdTables) {
                    Tables(db).delete(obj("TableName" to table.name))
                    assertEquals(null, db.read(db.snapshot(), TtlRecords.config(table.id)))
                }
            }
        }
    }

    companion object {
        private const val FUTURE_SECONDS = 3600L
        private const val MAX_TICKS = 64
        private const val BACKFILL_ROWS = 40
    }
}
