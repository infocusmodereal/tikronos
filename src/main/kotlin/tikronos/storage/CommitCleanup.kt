package tikronos.storage

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/** Only durable commits enter this queue; saturation applies backpressure to the caller. */
internal class CommitCleanup(workers: Int = WORKERS, capacity: Int = CAPACITY) : Executor, AutoCloseable {
    private val pool = ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(capacity), { task -> Thread(task, "tikronos-commit-cleanup") })

    override fun execute(command: Runnable) {
        try {
            pool.execute(command)
        } catch (_: RejectedExecutionException) {
            command.run()
        }
    }

    override fun close() {
        pool.shutdown()
        try {
            if (!pool.awaitTermination(DRAIN_SECONDS, TimeUnit.SECONDS)) pool.shutdownNow()
        } catch (_: InterruptedException) {
            pool.shutdownNow()
            Thread.currentThread().interrupt()
        }
        // Any unfinished secondary remains recoverable from its durable primary in TiKV.
    }

    companion object {
        private const val WORKERS = 8
        private const val CAPACITY = 64
        private const val DRAIN_SECONDS = 10L
    }
}
