package tikronos

import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import tikronos.storage.Storage
import tikronos.storage.Transaction
import tikronos.storage.bytes

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

/** Inspect prepared locks without allowing the SDK to resolve them first. */
internal fun TiSession.readWithoutResolvingLocks(key: String, version: Long): Kvrpcpb.GetResponse {
    val region = regionManager.getRegionByKey(bytes(key))
    val store = regionManager.getStoreById(region.leader.storeId)
    val channel = channelFactory.getChannel(store.address, pdClient.hostMapping)
    val request = Kvrpcpb.GetRequest.newBuilder()
        .setContext(region.getReplicaContext(region.leader, emptySet()))
        .setKey(pdClient.codec.encodeKey(bytes(key))).setVersion(version).build()
    return TikvGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS).kvGet(request)
}

/** Native read validation detects even an identical metadata rewrite during the operation. */
internal fun Storage.assertMetadataUnchanged(keys: List<String>, operation: () -> Unit) {
    val probeKey = "test/metadata-probe/${UUID.randomUUID()}"
    val probe = Transaction(snapshot(), this, cached = false)
    keys.forEach(probe::read)
    probe.write(probeKey, byteArrayOf(1))
    try {
        operation()
        var attempts = 0
        atomic(probe) { attempts++ }
        assertEquals(1, attempts, "Operation must not invalidate metadata readers")
    } finally {
        atomic { it.write(probeKey, null) }
    }
}
