package tikronos.config

internal data class HttpLimits(val storageWorkers: Int, val maxRequests: Int) {
    init {
        require(storageWorkers in 1..MAX_WORKERS) {
            "TIKRONOS_STORAGE_WORKERS must be in 1..$MAX_WORKERS"
        }
        require(maxRequests in storageWorkers..MAX_REQUESTS) {
            "TIKRONOS_MAX_REQUESTS must be between storage workers and $MAX_REQUESTS"
        }
    }

    companion object {
        private const val MAX_WORKERS = 256
        private const val MAX_REQUESTS = 512
        private const val WORKERS_PER_CPU = 8
        private const val DEFAULT_MAX_WORKERS = 128

        fun fromEnvironment(
            environment: Map<String, String> = System.getenv(),
            cpus: Int = Runtime.getRuntime().availableProcessors(),
        ): HttpLimits {
            val workers =
                environment["TIKRONOS_STORAGE_WORKERS"]?.toInt()
                    ?: (cpus.coerceIn(1, DEFAULT_MAX_WORKERS / WORKERS_PER_CPU) * WORKERS_PER_CPU)
            val requests = environment["TIKRONOS_MAX_REQUESTS"]?.toInt() ?: workers * 2
            return HttpLimits(workers, requests)
        }
    }
}
