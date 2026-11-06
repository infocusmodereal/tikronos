package tikronos.model

internal data class WriteResult(val table: Table, val old: Item?, val new: Item?)
