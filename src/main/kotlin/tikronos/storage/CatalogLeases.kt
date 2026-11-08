package tikronos.storage

import java.nio.ByteBuffer
import tikronos.model.CatalogLease

/** PD timestamp bounds, never the application host's wall clock. */
internal object CatalogLeases {
    fun encode(lease: CatalogLease): ByteArray =
        ByteBuffer.allocate(BYTES).putLong(lease.until).putLong(lease.blockedUntil).array()

    private const val BYTES = 16
    private const val LOGICAL_BITS = 18
    const val DURATION = 1000L shl LOGICAL_BITS
    const val DRAIN_TIMEOUT = 10 * DURATION
    fun key(catalog: String) = "tikronos/v1/catalog-lease/" + catalog.removePrefix(Storage.CATALOG)
    fun decode(value: ByteArray?): CatalogLease {
        if (value == null) return CatalogLease()
        check(value.size == BYTES) { "Unknown catalog lease format" }
        return ByteBuffer.wrap(value).let { CatalogLease(it.long, it.long) }
    }
}
