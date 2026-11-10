package tikronos.model

/** Empty cursor starts discovery; null marks a completed backfill. */
internal data class BackfillProgress(val cursor: String?)
