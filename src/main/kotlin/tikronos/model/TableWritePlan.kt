package tikronos.model

/** Only semantic settings belong here; worker cursors and stream heads stay in storage. */
internal data class TableWritePlan(val table: Table, val ttl: TtlPolicy?, val streamArn: String?) {
    val needsPrevious: Boolean get() = table.indexes.isNotEmpty() || ttl != null || streamArn != null
}

internal data class TtlPolicy(val attribute: String, val generation: String)
