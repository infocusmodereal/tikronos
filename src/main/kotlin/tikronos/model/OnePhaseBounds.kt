package tikronos.model

/** A fresh linearizability lower bound and the catalog lease's inclusive upper bound. */
internal data class OnePhaseBounds(val minimum: Long, val maximum: Long)
