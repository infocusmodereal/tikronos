package tikronos.storage

import tikronos.diagnostics.RequestEvents
import org.tikv.common.TiSession
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.RegionException
import org.tikv.common.region.RegionStoreClient
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
    private val exchange: (RegionStoreClient, ConcreteBackOffer, ByteString, Long, Long) -> Unit =
        { client, budget, key, start, commit -> client.commit(budget, listOf(key), start, commit) },
) {
    @Suppress("TooGenericExceptionCaught") // Classify failures at the primary decision boundary.
    fun commit(key: ByteString, startTs: Long, initialCommitTs: Long, ceiling: Long = Long.MAX_VALUE): Long {
        val started = RequestEvents.start()
        val decision = Decision()
        return try {
            complete(key, startTs, initialCommitTs, ceiling, decision)
        } catch (error: CommitRefused) {
            throw error
        } catch (error: Exception) {
            throw if (decision.uncertain) CommitUnknown(error) else CommitNotApplied(error)
        } finally { RequestEvents.commit(started) }
    }

    private fun complete(
        key: ByteString, startTs: Long, initialCommitTs: Long, ceiling: Long, decision: Decision,
    ): Long {
        val budget = ConcreteBackOffer.newCustomBackOff(COMMIT_BUDGET_MS)
        var commitTs = initialCommitTs
        while (true) {
            // commitTs can advance only after a definite rejection, never after ambiguity.
            if (commitTs > ceiling) throw CommitRefused()
            try {
                session.regionStoreClientBuilder.build(key, budget).use { client ->
                    decision.uncertain = true
                    exchange(client, budget, key, startTs, commitTs)
                }
                return commitTs
            } catch (error: KeyException) {
                val detail = error.keyErr
                if (detail?.hasCommitTsExpired() != true) throw error
                // This native response proves the prepared lock has not committed.
                decision.uncertain = false
                commitTs = maxOf(detail.commitTsExpired.minCommitTs, timestamp().version)
                budget.doBackOff(BackOffFuncType.BoTxnLockFast, error)
            } catch (error: RegionException) {
                // SDK retries may hide an earlier lost response. Retain uncertainty.
                budget.doBackOff(BackOffFuncType.BoRegionMiss, error)
            }
        }
    }

    private class Decision(var uncertain: Boolean = false)

    companion object {
        private const val COMMIT_BUDGET_MS = 3000
    }
}

internal class CommitRefused : RuntimeException()

internal class CommitNotApplied(cause: Throwable) : RuntimeException("Primary commit was not applied", cause)
