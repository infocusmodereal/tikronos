package tikronos

import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import tikronos.storage.Storage

internal fun Storage.mutate(key: String, change: (ByteArray?) -> ByteArray?): ByteArray? = atomic { tx ->
    val old = tx.read(key)
    tx.write(key, change(old))
    old
}

/** Close persistent timestamp streams before releasing their shared SDK channels. */
internal fun withStorageSession(action: (TiSession, Storage) -> Unit) {
    val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
    TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
        Storage(session).use { action(session, it) }
    }
}
