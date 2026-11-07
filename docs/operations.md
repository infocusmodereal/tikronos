# Operations

## Service boundaries

TiKronos is the HTTP layer. Persistent tables, items, index checkpoints, TTL candidates and stream records live in TiKV. PD provides cluster coordination and discovery. Container-local files are not the database.

Multiple API instances can point at the same cluster, with metadata validation coordinated through TiKV. Writes to the same item and an enabled stream head still contend; adding instances alone does not prove horizontal scalability. Each instance also runs maintenance work. Validate concurrent instances and failure recovery for your workload before production use.

## Health and shutdown

`GET /healthz` and `HEAD /healthz` report process liveness only. Use a signed read such as `ListTables` to verify storage access. A successful liveness response does not mean TiKV is ready.

SIGTERM rejects new API requests, drains admitted requests for up to ten seconds, then stops the HTTP engine and storage resources. Give containers 60 seconds to stop, as the supplied Compose file does. A forced kill or lost connection can leave the result of a write unknown; reconcile state before blindly replaying mutations.

## Request limits

| Limit per API process | Value |
| --- | --- |
| Admitted requests | Twice the storage worker count by default |
| Concurrent blocking storage calls | 8 per available CPU, capped at 128 by default |
| Storage-worker queue wait | 5 seconds |
| Maximum request body | 16 MiB |
| Body inactivity deadline | 3 seconds |
| Total body-read deadline | 30 seconds |
| Shared raw-body buffer budget | 64 MiB |

`TIKRONOS_STORAGE_WORKERS` accepts 1-256 workers. `TIKRONOS_MAX_REQUESTS` must be at least the worker count and at most 512. Defaults use the JVM's available CPU count, which respects container CPU limits on supported runtimes.

The raw-buffer budget does not include all parsed JSON or native allocations. Admission pressure returns a throttling error; oversized bodies return HTTP 413 and body timeouts return HTTP 408. Clients should use bounded retries with backoff where an operation can safely be retried.

## Storage and upgrades

Keep PD and TiKV on protected networks. Use storage-level backup and recovery procedures appropriate to TiKV; the API does not provide backups. The development Compose stack has no redundancy.

Stop older pre-release instances before upgrading a cluster. Metadata readers now validate through striped fences, and metadata writers update all fences atomically; older writers do not participate in that protocol. The service reads older records and writes a versioned format. Rolling back after a new-format write requires a pre-upgrade backup. Do not assume mixed-version rolling upgrades are safe.

Integration checks exercise restart and failure paths, but host/zone failures and prolonged soak remain unvalidated. Monitor application logs, TiKV health, disk usage and request failures. Physical reclamation of retired namespaces is not yet implemented.
