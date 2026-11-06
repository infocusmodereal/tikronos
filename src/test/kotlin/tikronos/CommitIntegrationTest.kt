package tikronos

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Tag
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.txn.TwoPhaseCommitter
import tikronos.storage.SingleKeyCommit

@Tag("tikv")
class CommitIntegrationTest {
    @Test
    fun `reader pushing minimum commit timestamp does not lose or repeat the prepared write`() {
        val endpoint = System.getenv("TIKRONOS_TEST_PD") ?: "127.0.0.1:2379"
        TiSession.create(TiConfiguration.createDefault(endpoint)).use { session ->
            val key = ByteString.copyFromUtf8("tikronos-test/" + UUID.randomUUID())
            val start = session.timestamp.version
            TwoPhaseCommitter(session, start).use { txn ->
                txn.prewritePrimaryKey(budget(), key.toByteArray(), byteArrayOf(1))
                val staleCommit = session.timestamp.version
                val reader = session.createSnapshot()
                assertTrue(reader.get(key).isEmpty)
                SingleKeyCommit(session).commit(key, start, staleCommit)
                assertTrue(reader.get(key).isEmpty)
                assertEquals(ByteString.copyFrom(byteArrayOf(1)), session.createSnapshot().get(key))
            }
            val cleanup = session.timestamp.version
            TwoPhaseCommitter(session, cleanup).use { txn ->
                txn.prewritePrimaryKey(budget(), key.toByteArray(), byteArrayOf())
                SingleKeyCommit(session).commit(key, cleanup, session.timestamp.version)
            }
        }
    }

    private fun budget() = ConcreteBackOffer.newCustomBackOff(BUDGET_MS)

    companion object {
        private const val BUDGET_MS = 3000
    }
}
