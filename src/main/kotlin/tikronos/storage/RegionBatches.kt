package tikronos.storage

import java.util.concurrent.TimeUnit
import org.tikv.common.TiSession
import org.tikv.common.exception.RegionException
import org.tikv.common.key.Key
import org.tikv.common.region.TiRegion
import org.tikv.common.util.BackOffFunction.BackOffFuncType
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.shade.com.google.protobuf.ByteString

/** Preserve key order and repartition after a split without replaying completed batches. */
internal class RegionBatches(private val session: TiSession) {
    fun execute(
        keys: List<ByteString>,
        size: (ByteString) -> Int = ByteString::size,
        operation: (TiRegion, List<ByteString>, ConcreteBackOffer) -> Unit,
    ) {
        val pending = keys.toMutableSet()
        val budget = ConcreteBackOffer.newCustomBackOff(BUDGET_MS)
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BUDGET_MS.toLong())
        while (pending.isNotEmpty()) {
            check(System.nanoTime() < deadline) { "Region batch deadline exceeded" }
            val region = session.regionManager.getRegionByKey(pending.first(), budget)
            var bytes = 0
            val batch =
                pending
                    .asSequence()
                    // ByteString overload treats the unbounded end as an empty finite key.
                    .takeWhile { region.contains(Key.toRawKey(it)) }
                    .take(MAX_KEYS)
                    .takeWhile { key ->
                        val next = size(key)
                        (bytes == 0 || bytes + next <= MAX_BYTES).also { if (it) bytes += next }
                    }
                    .toList()
            try {
                operation(region, batch, budget)
                pending.removeAll(batch.toSet())
            } catch (error: RegionException) {
                session.regionManager.invalidateRegion(region)
                budget.doBackOff(BackOffFuncType.BoRegionMiss, error)
            }
        }
    }

    companion object {
        private const val BUDGET_MS = 3000
        private const val MAX_KEYS = 128
        private const val MAX_BYTES = 1024 * 1024
    }
}
