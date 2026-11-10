package tikronos.model

internal data class TtlState(
    val tableName: String,
    val tableId: String,
    val attribute: String,
    val enabled: Boolean,
    val generation: String,
    // Legacy resume seed; new checkpoints live outside the policy record.
    val cursor: String? = "",
)

internal data class ExpiryCandidate(
    val tableName: String,
    val tableId: String,
    val generation: String,
    val expiry: Long,
    val key: Item,
)

internal val TtlState.policy: TtlPolicy? get() = if (enabled) TtlPolicy(attribute, generation) else null
