package tikronos.model

internal sealed interface WriteIntent {
    data class Replace(val item: Item?) : WriteIntent
    class Transform(val apply: (Item?) -> Item?) : WriteIntent

    companion object {
        fun select(replace: Boolean, capacity: Boolean, item: Item?, change: (Item?) -> Item?): WriteIntent =
            if (replace && !capacity) Replace(item) else Transform(change)
    }
}

internal sealed interface PreviousItem {
    data object Unread : PreviousItem
    data class Read(val item: Item?) : PreviousItem
}
