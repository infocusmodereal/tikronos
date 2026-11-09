package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.tikv.kvproto.Pdpb
import tikronos.storage.timestampBatch

class TimestampBatchTest {
    @Test
    fun `logical suffix bits produce distinct ordered timestamps`() {
        val response = response().setCount(4).setTimestamp(
            Pdpb.Timestamp.newBuilder().setPhysical(2).setLogical(31).setSuffixBits(3),
        ).build()
        val batch = timestampBatch(response, 4, 7, 0)
        assertEquals(listOf(7L, 15L, 23L, 31L), batch.map { it.logical })
        assertEquals(4, batch.map { it.version }.toSet().size)
        assertFailsWith<IllegalStateException> { timestampBatch(response, 4, 7, batch.first().version) }
    }

    @Test
    fun `invalid PD allocations never escape as usable timestamps`() {
        val invalid = listOf(
            response().setCount(2),
            response().setHeader(Pdpb.ResponseHeader.newBuilder().setClusterId(8)),
            response().setTimestamp(Pdpb.Timestamp.newBuilder().setPhysical(0).setLogical(1)),
            response().setTimestamp(Pdpb.Timestamp.newBuilder().setPhysical(Long.MAX_VALUE).setLogical(1)),
            response().setTimestamp(Pdpb.Timestamp.newBuilder().setPhysical(1).setLogical(1).setSuffixBits(18)),
            response().setTimestamp(Pdpb.Timestamp.newBuilder().setPhysical(1).setLogical(1L shl 18)),
        )
        invalid.forEach { candidate ->
            assertFailsWith<IllegalStateException> { timestampBatch(candidate.build(), 1, 7, 0) }
        }
        assertFailsWith<IllegalStateException> {
            timestampBatch(response().setCount(2).build(), 2, 7, 0)
        }
    }

    private fun response() = Pdpb.TsoResponse.newBuilder().setCount(1)
        .setHeader(Pdpb.ResponseHeader.newBuilder().setClusterId(7))
        .setTimestamp(Pdpb.Timestamp.newBuilder().setPhysical(1).setLogical(0))
}
