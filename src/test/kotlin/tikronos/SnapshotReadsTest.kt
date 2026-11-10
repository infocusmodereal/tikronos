package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.tikv.common.BytePairWrapper
import org.tikv.common.Snapshot
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.util.BackOffer
import tikronos.storage.readBatch

class SnapshotReadsTest {
    @Test
    fun `unordered results retain key identity and empty values mean absence`() {
        val snapshot = object : Snapshot(TiTimestamp(1, 0), null) {
            override fun batchGet(backOffer: Int, keys: List<ByteArray>): List<BytePairWrapper> {
                assertEquals(BackOffer.GET_MAX_BACKOFF, backOffer)
                return listOf(
                    BytePairWrapper("b".encodeToByteArray(), byteArrayOf(2)),
                    BytePairWrapper("empty".encodeToByteArray(), byteArrayOf()),
                    BytePairWrapper("a".encodeToByteArray(), byteArrayOf(1)),
                )
            }
        }
        assertEquals(mapOf("a" to listOf<Byte>(1), "b" to listOf<Byte>(2)),
            snapshot.readBatch(listOf("a", "missing", "empty", "b")).mapValues { it.value.toList() })
    }

    @Test
    fun `a failed native batch is not reported as absent items`() {
        val snapshot = object : Snapshot(TiTimestamp(1, 0), null) {
            override fun batchGet(backOffer: Int, keys: List<ByteArray>): List<BytePairWrapper> =
                error("Lost batch response")
        }
        assertEquals(emptyMap(), snapshot.readBatch(emptyList()))
        assertFailsWith<IllegalStateException> { snapshot.readBatch(listOf("a", "b")) }
    }
}
