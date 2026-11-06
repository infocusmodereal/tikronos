package tikronos.protocol

internal data class DynamoRequest(val headers: RequestHeaders, val body: ByteArray, val hasQuery: Boolean)
