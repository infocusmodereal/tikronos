package tikronos

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import tikronos.storage.Storage

/** Durable cursors live in TiKV; any process may advance a bounded unit of work. */
internal class Maintenance(db: Storage) : AutoCloseable {
    private val executor = Executors.newSingleThreadScheduledExecutor()
    private val indexes = IndexBackfill(db)
    private val ttl = TtlWorker(db, Ttl(db))
    private val retired = RetiredRows(db)
    private val streams = StreamRetention(db)
    private val writer = ItemWriter(db)

    fun start() {
        executor.scheduleWithFixedDelay({
            maintenanceAttempt("index backfill") { indexes.tick() }
            maintenanceAttempt("TTL") { ttl.tick(writer) }
            maintenanceAttempt("retired generations") { retired.tick() }
            maintenanceAttempt("stream retention") { streams.tick() }
        }, 0, 1, TimeUnit.SECONDS)
    }

    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(DRAIN_SECONDS, TimeUnit.SECONDS)) {
            executor.shutdownNow()
            check(executor.awaitTermination(DRAIN_SECONDS, TimeUnit.SECONDS)) { "Maintenance did not stop" }
        }
    }

    companion object {
        private const val DRAIN_SECONDS = 10L
    }
}

@Suppress("TooGenericExceptionCaught") // A failed maintenance unit must not starve unrelated tables.
internal fun maintenanceAttempt(label: String, work: () -> Unit) {
    try {
        work()
    } catch (error: Exception) {
        Logger.getLogger(Maintenance::class.java.name).log(Level.WARNING, "$label failed; will retry", error)
    }
}
