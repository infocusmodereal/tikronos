package tikronos.model

internal data class WriteResult(val table: Table, val previous: PreviousItem, val new: Item?) {
    constructor(table: Table, old: Item?, new: Item?) : this(table, PreviousItem.Read(old), new)

    val old: Item? get() = checkNotNull(previous as? PreviousItem.Read) {
        "This write did not request the previous item"
    }.item
}
