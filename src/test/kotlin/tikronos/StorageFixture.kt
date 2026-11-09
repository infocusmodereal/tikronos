package tikronos

import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import tikronos.storage.Storage

/** Close persistent timestamp streams before releasing their shared SDK channels. */
internal fun withStorageSession(endpoint: String, action: (TiSession, Storage) -> Unit) {
    TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
        Storage(session).use { action(session, it) }
    }
}
