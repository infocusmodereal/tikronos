package tikronos.storage

/**
 * Readers lock one stripe; metadata writers update every stripe in the same transaction. A real Put
 * on each fence makes a metadata change conflict with an older reader's startTs, including a reader
 * that has not acquired its validation lock yet. Fence writes are blind: committed reader locks
 * must not invalidate a metadata update. The actual metadata remains conflict-checked.
 */
internal object MetadataFences {
    private const val STRIPES = 1024
    private const val PREFIX = "tikronos/v1/metadata-fence/"
    private const val HEX_RADIX = 16
    private const val HEX_WIDTH = 3

    fun isFence(key: String): Boolean = key.startsWith(PREFIX)

    fun isFenceNamespace(prefix: String) = isFence(prefix) && prefix != PREFIX && prefix.endsWith('/')

    fun tracks(key: String): Boolean =
        key.startsWith(Storage.CATALOG) ||
            key.startsWith(TtlRecords.CONFIGS) ||
            key.startsWith(StreamRecords.CONFIGS)

    fun namespace(key: String) = PREFIX + keyDigest(key) + "/"

    fun keys(key: String): List<String> {
        val prefix = namespace(key)
        return List(STRIPES) { prefix + it.toString(HEX_RADIX).padStart(HEX_WIDTH, '0') }
    }

    fun guard(key: String, scope: String): String =
        if (tracks(key))
            namespace(key) +
                (scope.hashCode() and (STRIPES - 1)).toString(HEX_RADIX).padStart(HEX_WIDTH, '0')
        else key
}
