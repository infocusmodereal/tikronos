package tikronos.storage

import java.security.MessageDigest
import java.util.HexFormat

/** Fixed-width identity suffixes keep derived keys within TiKV's encoded key limit. */
internal fun keyDigest(address: String): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(address.encodeToByteArray()))
