package tikronos

import java.util.concurrent.TimeUnit
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.TikvGrpc
import tikronos.storage.bytes
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
