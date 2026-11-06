package tikronos

import tikronos.storage.Storage

internal fun Storage.mutate(key: String, change: (ByteArray?) -> ByteArray?): ByteArray? = atomic { tx ->
    val old = tx.read(key)
    tx.write(key, change(old))
    old
}
