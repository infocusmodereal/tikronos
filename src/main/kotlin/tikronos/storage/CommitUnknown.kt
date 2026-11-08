package tikronos.storage

internal class CommitUnknown(cause: Throwable) : RuntimeException("Write outcome is unknown", cause)
