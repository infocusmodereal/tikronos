package tikronos.storage

import org.tikv.common.TiSession
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.RegionException
import org.tikv.common.util.BackOffFunction.BackOffFuncType
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * A reader can push minCommitTs after prewrite. Retry only an explicit rejection, keeping startTs
 * and the prepared value. Transport errors remain ambiguous and must never replay the update.
 */
internal class SingleKeyCommit(
    private val session: TiSession,
    private val timestamp: () -> TiTimestamp = { session.timestamp },
) {
    fun commit(key: ByteString, startTs: Long, initialCommitTs: Long, ceiling: Long = Long.MAX_VALUE): Long {
        val budget = ConcreteBackOffer.newCustomBackOff(COMMIT_BUDGET_MS)
        var commitTs = initialCommitTs
        while (true) {
            // Only reached before sending, or after an explicit rejection/region error.
            if (commitTs > ceiling) throw CommitRefused()
            try {
                session.regionStoreClientBuilder.build(key, budget).use { client ->
                    client.commit(budget, listOf(key), startTs, commitTs)
                }
                return commitTs
            } catch (error: KeyException) {
                val detail = error.keyErr
                if (detail?.hasCommitTsExpired() != true) throw error
                commitTs = maxOf(detail.commitTsExpired.minCommitTs, timestamp().version)
                budget.doBackOff(BackOffFuncType.BoTxnLockFast, error)
            } catch (error: RegionException) {
                budget.doBackOff(BackOffFuncType.BoRegionMiss, error)
            }
        }
    }

    companion object {
        private const val COMMIT_BUDGET_MS = 3000
    }
}

internal class CommitRefused : RuntimeException()
