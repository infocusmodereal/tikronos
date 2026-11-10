package tikronos.model

/** Total returned rows and a best-effort RPC payload target; TiKV only enforces a row limit. */
internal data class ScanOptions(
    val limit: Int = Int.MAX_VALUE,
    val keyOnly: Boolean = false,
    val targetBytes: Int = DEFAULT_TARGET_BYTES,
) {
    init {
        require(limit >= 0)
        require(targetBytes > 0)
    }

    companion object {
        private const val DEFAULT_TARGET_BYTES = 1024 * 1024
    }
}
