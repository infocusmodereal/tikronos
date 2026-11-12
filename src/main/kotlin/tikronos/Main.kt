package tikronos

import kotlinx.coroutines.sync.Semaphore
import tikronos.protocol.Signature

import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import java.net.InetSocketAddress
import java.net.URI
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import tikronos.config.HttpLimits
import tikronos.storage.Storage
import tikronos.storage.CommitCleanup

private fun addressArgument(args: Array<String>): String = when {
    args.isEmpty() -> "127.0.0.1:8000"
    args.size == 2 && args[0] in setOf("-listen", "--listen") -> args[1]
    args.size == 1 && args[0].substringBefore('=') in setOf("-listen", "--listen")
        && '=' in args[0] -> args[0].substringAfter('=')
    else -> error("Usage: tikronos [-listen host:port]")
}

private fun listenAddress(args: Array<String>): InetSocketAddress {
    val address = addressArgument(args)
    val wildcard = address.startsWith(':')
    val endpoint = URI(if (wildcard) "http://localhost$address" else "http://$address")
    require(endpoint.host != null && endpoint.port in 0..MAX_PORT && endpoint.rawPath.isEmpty()
        && endpoint.rawQuery == null && endpoint.rawFragment == null && endpoint.rawUserInfo == null) {
        "Expected host:port"
    }
    return if (wildcard) InetSocketAddress(endpoint.port)
        else InetSocketAddress(endpoint.host, endpoint.port)
}

private fun signature(): Signature? {
    val access = System.getenv("TIKRONOS_ACCESS_KEY")
    val secret = System.getenv("TIKRONOS_SECRET_KEY")
    require((access == null && secret == null) || (!access.isNullOrBlank() && !secret.isNullOrBlank())) {
        "Set both TIKRONOS_ACCESS_KEY and TIKRONOS_SECRET_KEY"
    }
    return access?.let { Signature(it, requireNotNull(secret), System.getenv("TIKRONOS_REGION") ?: "us-east-1") }
}

@Suppress("TooGenericExceptionCaught") // Startup must release resources when bind or engine initialization fails.
fun main(args: Array<String>) {
    // Own shutdown ordering: Ktor's independent JVM hook would close sockets before our drain.
    System.setProperty("io.ktor.server.engine.ShutdownHook", "false")
    val bind = listenAddress(args)
    val signature = signature()
    val limits = HttpLimits.fromEnvironment()
    val onePhase = System.getenv("TIKRONOS_ONE_PHASE_COMMIT")?.toBooleanStrict() ?: true
    val cpus = Runtime.getRuntime().availableProcessors()
    require(signature != null || bind.address?.isLoopbackAddress == true) {
        "Credentials required on non-loopback listeners"
    }
    val configuration = TiConfiguration.createDefault(System.getenv("TIKRONOS_PD") ?: "127.0.0.1:2379")
    configuration.batchGetConcurrency = limits.storageWorkers
    val session = TiSession.create(configuration)
    val cleanup = CommitCleanup()
    val storage = Storage(session, cleanup, onePhase = onePhase)
    val maintenance = Maintenance(storage)
    val workers = Executors.newFixedThreadPool(limits.storageWorkers)
    val dispatcher = workers.asCoroutineDispatcher()
    val executions = Semaphore(limits.storageWorkers)
    val api = HttpApi(Dynamo(storage, signature, executions), dispatcher, limits, executions)
    val server = embeddedServer(Netty, configure = {
        connector { host = bind.hostString; port = bind.port }
        connectionGroupSize = 1
        workerGroupSize = (cpus / 2).coerceIn(2, MAX_NETWORK_THREADS)
        callGroupSize = cpus.coerceIn(2, MAX_NETWORK_THREADS)
        shutdownGracePeriod = GRACE_MS
        shutdownTimeout = DRAIN_MS
    }) {
        intercept(ApplicationCallPipeline.Call) {
            api.handle(call)
            finish()
        }
    }
    val shutdown = Thread {
        try {
            runBlocking { api.drain(DRAIN_MS) }
        } finally {
            try {
                server.stop(GRACE_MS, DRAIN_MS)
            } finally {
                closeResources(workers, maintenance, dispatcher, cleanup, storage, session)
            }
        }
    }
    Runtime.getRuntime().addShutdownHook(shutdown)
    try {
        storage.catalog.warmup()
        server.start(wait = false)
        maintenance.start()
    } catch (error: Exception) {
        cleanupStartup(shutdown, error)
        throw error
    }
}

@Suppress("TooGenericExceptionCaught") // Preserve the startup error if cleanup also fails.
private fun cleanupStartup(shutdown: Thread, error: Exception) {
    try {
        shutdown.run()
    } catch (cleanup: Exception) {
        error.addSuppressed(cleanup)
    } finally {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdown)
        } catch (stopping: IllegalStateException) {
            error.addSuppressed(stopping)
        }
    }
}

@Suppress("LongParameterList") // Explicit shutdown ordering for independently owned resources.
private fun closeResources(
    workers: ExecutorService,
    maintenance: Maintenance,
    dispatcher: ExecutorCoroutineDispatcher,
    cleanup: CommitCleanup,
    storage: Storage,
    session: TiSession,
) {
    workers.shutdown()
    try {
        if (!workers.awaitTermination(DRAIN_MS, TimeUnit.MILLISECONDS)) {
            workers.shutdownNow()
            workers.awaitTermination(DRAIN_MS, TimeUnit.MILLISECONDS)
        }
    } finally {
        fun release() {
            try {
                maintenance.close()
            } finally {
                dispatcher.close()
                cleanup.close()
                try { storage.close() } finally { session.close() }
            }
        }
        if (workers.isTerminated) release() else {
            java.util.logging.Logger.getLogger("tikronos").warning("Storage drain timed out; deferring session close")
            Thread.ofPlatform().daemon().name("storage-drain").start {
                workers.close()
                release()
            }
        }
    }
}

private const val MAX_PORT = 65535
private const val GRACE_MS = 1000L
private const val DRAIN_MS = 10_000L
private const val MAX_NETWORK_THREADS = 8
