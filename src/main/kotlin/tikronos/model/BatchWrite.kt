package tikronos.model

import com.fasterxml.jackson.databind.node.ObjectNode

internal data class BatchWrite(
    val table: Table,
    val address: String,
    val request: ObjectNode,
    val item: Item?,
    val key: Item,
)
