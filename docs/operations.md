# Operations

## Service boundaries

TiKronos is the HTTP layer. Persistent tables, items, index checkpoints, TTL candidates and stream records live in TiKV. PD provides cluster coordination and discovery. Container-local files are not the database.

Multiple API instances can point at the same cluster, with metadata validation coordinated through TiKV. Writes to the same item and an enabled stream head still contend; adding instances alone does not prove horizontal scalability. Each instance also runs maintenance work. Validate concurrent instances and failure recovery for your workload before production use.

## Table catalog

Table definitions are durable TiKronos records in TiKV, not static application configuration.
On startup the service discovers and warms up to 64 existing tables before opening the listener;
remaining tables load on demand. Restarting against the same cluster preserves schemas and data.
Arbitrary RawKV keys and TiDB tables do not contain this catalog and are not automatically imported.

Each process caches definitions and semantic TTL/stream settings, including their absence, bounded
to 4,096 entries and 16 MiB of record and setting data (decoded objects add heap overhead). A one-second, per-table
read lease uses PD timestamps.
Renewal and schema changes conflict through native TiKV transactions. A schema writer blocks
renewal and waits for existing leases to expire before committing; schema changes can therefore
add approximately one second of latency. Abandoned renewal blocks expire after ten seconds.
Requests outside a cached definition's timestamp interval read TiKV instead of using stale metadata.
Tables with an index being backfilled also use the cache; progress is stored separately from the definition.

[TiKV MVCC](https://tikv.org/deep-dive/distributed-transaction/isolation-level/) supplies snapshot reads and write conflicts. The cache still needs coordination: snapshot
isolation alone does not invalidate process memory or validate arbitrary read dependencies.
Lease-covered writes omit metadata fence locks only while their commit timestamp remains within
the lease. Expiry, including a reader pushing the minimum commit timestamp past that limit,
causes an explicit abort and retry with catalog fences and item validation. Transport-ambiguous commits are never
replayed. TTL and stream configuration changes also drain the catalog lease; maintenance-only
cursor progress does not change item-write semantics. Semantic settings share the catalog
validation; actual stream-head and cursor writes retain native write conflicts.
The cache excludes worker cursors and mutable stream heads. Unconditional Put/Delete operations
skip the previous-item read when indexes, TTL, Streams, return values and capacity reporting do
not require it; TiKV still checks write conflicts.
The same storage gate covers API schema changes, changed tags, index activation, and deletion/recreation.
Tag requests that leave the tag set unchanged do not rewrite the catalog.

`DescribeTable` reads the definition and its stream from one TiKV snapshot. `DescribeTimeToLive`
uses one snapshot for the definition and TTL policy. Create, update and delete responses describe
the table and stream from the transaction that committed, even if another API instance changes
the stream before the response arrives.

## Maintenance checkpoints

Index and TTL backfill progress lives under `tikronos/v1/backfill/`, scoped to table and index
IDs or TTL policy generation. Each batch discovers at most 32 item keys, then rechecks current
items, metadata and expected progress in a TiKV transaction, using native batch reads. Index
batches also target at most 1 MiB of encoded entry values; one larger entry can exceed that
target to ensure progress. A byte-limited batch resumes at its last processed key, even on a
short scan. The same commit writes the derived entries and advances the checkpoint. The final
index batch publishes `ACTIVE` and removes its checkpoint atomically. TTL keeps an explicit
completed checkpoint; configuration discovery and batched progress reads share one snapshot per
page. Concurrent workers use native write conflicts; an aborted batch cannot advance progress.
Checkpoint writes do not rewrite semantic metadata, drain catalog leases or fan out metadata
fences. Activation and policy changes still do.

When a separate checkpoint is absent, workers resume the embedded cursor in older records. That
embedded cursor remains a legacy seed, not a live progress report. New policy generations and
recreated tables/indexes use independent keys. Dropping a table or index queues its immutable row prefix in the DDL
transaction. Maintenance deletes up to 512 keys per transaction using MVCC tombstones and
commits the next key with the deletions. Readers on an older valid TiKV snapshot retain their
view until normal TiKV GC. Reusing a name does not reuse its keys. Index and TTL checkpoints
are removed when their generations retire. Previously orphaned rows and checkpoints without a
retirement marker are not swept. Per-generation configuration stripes are queued with the rows;
catalog stripes remain reusable by name, so each distinct table name retains 1,024 stripe keys. A worker rotates through jobs until its 150 ms scheduling
budget expires (an in-flight transaction can exceed the budget). TTL candidates remain until their
scheduled expiry, even after retirement; a far-future timestamp can keep a candidate for years. Follow the stop-all-instances and backup/restore
upgrade policy below; mixed old/new workers and in-place rollback are unsupported.

## Stream retention

Filtered ListStreams seeks to the exact table prefix in each stored ARN authority on one snapshot.
The [ListStreams contract](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_ListStreams.html)
defines an exclusive cursor; TiKronos preserves its lexical position even if the previous ARN has expired. Closed
streams remain readable for 24 hours; maintenance removes detached descriptors and watermarks
after the last event expires. A live table keeps its latest disabled descriptor for DescribeTable.

A stream append commits the item, event and shard head in one TiKV transaction. Cleanup deletes
expired events and advances a separate per-stream trim watermark in one transaction. Concurrent
cleaners serialize on that watermark. Writers still serialize on the shard head to preserve order.

Stream readers use one snapshot for the descriptor, watermark and event reads. The last trimmed
sequence in an older descriptor remains a lower bound after upgrade. Stop all older instances and
back up before upgrading. Older readers cannot see the separate watermark after cleanup, so
rollback requires restoring the pre-upgrade backup.

## Secondary index storage

New index entries store the table and index keys plus the attributes in the index projection.
An update that changes only attributes outside an active index projection skips that index write.
Queries and scans that need an unprojected local index attribute fetch base rows in batches of up
to eight at the same TiKV snapshot. Global indexes serve projected attributes only.

Existing full-copy entries remain readable alongside projected entries. On the first write to a
legacy item, the same transaction replaces its partial index copies and marks the base row with
the new record version. Rows that are never written keep their full-copy entries. There is no bulk
rewrite. When every index write can be skipped,
a base-row update can qualify for the existing single-key one-phase commit path.
Stop all older instances and take a backup before upgrading. Older versions cannot read the new
base records or serve unprojected attributes from compact entries. Restore the pre-upgrade
backup to roll back.

## Range reads

Scans use one MVCC snapshot per request and keep TiKV's region routing and lock resolution.
Table listing, retention probes and backfill discovery use native key-only reads; maintenance
still rechecks current values transactionally before writing. Caller row limits bound fetching.
Value scans start with up to eight rows, then size batches toward 1 MiB using the largest observed
row, still capped at eight; key-only batches are capped at 128. This is a payload target, not a hard byte limit:
TiKV limits rows, and a later row can be larger. Large-value pages may trade more RPCs for less
prefetch. Query/Scan byte limits remain applied before filtering and projection.

## Health and shutdown

`GET /healthz` and `HEAD /healthz` report process liveness only. Use a signed read such as `ListTables` to verify storage access. A successful liveness response does not mean TiKV is ready.

SIGTERM rejects new API requests, drains admitted requests for up to ten seconds, then stops the HTTP engine. Cancellation stops a batch before its next pair. The service waits for started storage work before closing the TiKV session. Give containers 60 seconds to stop, as the supplied Compose file does. A forced kill or lost connection can leave the result of a write unknown; reconcile state before blindly replaying mutations.

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

## Write completion

Single-item transactions without uncovered read dependencies use native TiKV one-phase commit
(1PC). The request keeps native write-conflict checks, obtains a fresh timestamp before sending,
and bounds the commit timestamp by the catalog lease. A successful 1PC response is the durable
commit decision; an explicit fallback continues through ordinary two-phase commit (2PC).
Multi-key, metadata and index-entry transactions remain on 2PC. Set
`TIKRONOS_ONE_PHASE_COMMIT=false` to disable 1PC without changing the stored format.

A lost 1PC response or an uncertain 2PC primary commit is an unknown outcome. The API returns
HTTP 500 and does not roll back, replay the mutation or return it as an unprocessed batch item.
The write may have committed; readers resolve remaining locks through TiKV using the primary
decision or TTL. Earlier items in a failed batch may already be applied. Reconcile state before retrying a
non-idempotent mutation; SDK retries can otherwise apply it twice.

On 2PC, writes acknowledge the durable primary commit. Eight background workers complete
secondary locks, with a queue of 64 transactions and caller-thread completion when full.
Readers can resolve secondaries after a crash. Shutdown drains completion work before closing
the TiKV session. TiKV's async-commit protocol remains disabled.

Timestamp batches reuse PD's bidirectional stream through the Java SDK's current leader-aware
channel. Each batch contains only callers already waiting before allocation. A timeout or stream
failure retires that stream; retries retain the SDK's bounded policy and leader discovery.

## Request profiling

Optional Java Flight Recorder event `tikronos.Request` reports the operation, response status,
rejection reason, storage-worker queue wait, timestamp allocation time, prewrite exchange time,
primary-commit phase time, and 1PC attempt/commit/fallback counts. Attempt counts include resends,
so they need not equal commits plus fallbacks. Phase durations are summed
within a request, including retries and catalog renewal; they are not exclusive CPU time.
Timestamp time includes waiting for the timestamp allocator. Subtract `timestampAllocationNanos`
from `timestampNanos` to estimate coordination and scheduling wait. Allocation time includes the
shared batch's retries; interrupted callers without a delivered result record no allocation time.
The `tikronos.Timestamp` event records each batch's size, success, RPC-attempt count and summed
`rpcNanos`, including retried exchanges. Its remaining duration includes retry backoff and local work.
Commit time includes region lookup and retry waits; prewrite time covers the RPC exchange only. Keys, values and credentials are not recorded.
Validated batch writes can use one spare storage slot for a second item transaction. With a
single slot, full admission, an active stream, a building index or no cached write plan, the batch
runs serially. Both started transactions finish before the next pair or the response. Only confirmed
contention is returned as unprocessed. An unknown decision from either item takes precedence.

The request event counts concurrent pairs in `batchPairs` and sums their storage work across both
threads. Phase totals can exceed elapsed request time. The `tikronos.Maintenance` event measures
one visit per maintenance family, including empty scans. The scheduler waits one second after all
four visits finish.

Without an active JFR recording, request events are not allocated.
Rejection reasons distinguish `admission`, `queue`, `body_budget`, `draining`, `contention`
and `unknown_outcome`. The latter is an ambiguous write result, not a safe rejection to replay.

The `tikronos.CatalogLease` event measures complete acquisition and drain calls, including
transaction retries. Acquisition triggers distinguish `cold`, `expired` and `renewal`; outcomes
distinguish durable lease `extended` or `reused` from `missing`, `blocked` and `error`.
A completed drain reports `drained`. `attempts` counts transaction attempts and
`leaseWriteAttempts` counts write intents across retries, not successful commits or native RPCs.
Drain `polls` includes the terminal PD timestamp check; `waitNanos` includes polling and sleep.
These events also cover startup and maintenance outside an HTTP request, without table identities.

Request events include catalog acquisition/drain totals and durations, loader-busy counts and
`catalogFallbackGets`. The latter counts catalog point Gets outside the internal lease transaction;
it excludes batched prefetch. A busy loader can still return a valid existing lease without a Get.
The triggering read falls back after cold or expired acquisition because its older snapshot precedes
the acquired interval. Catalog durations overlap timestamp/prewrite/commit phases; do not add them
as exclusive costs. Hot cache hits emit no lease event. Use TiKV server metrics for native RPC counts.

For a bounded diagnostic run, add
`-XX:StartFlightRecording=filename=/tmp/tikronos.jfr,duration=120s,maxsize=128m,settings=default`
to `JAVA_OPTS`, retaining the existing heap options. Read events with
`jfr print --events tikronos.Request,tikronos.Timestamp,tikronos.CatalogLease /tmp/tikronos.jfr` from a Java 21 JDK.
Profile separately from comparative benchmarks to avoid attributing recording overhead to storage.
Wait for the recording to finish and verify a copied recording before stopping its container.

## Storage and upgrades

Keep PD and TiKV on protected networks. Use storage-level backup and recovery procedures appropriate to TiKV; the API does not provide backups. The development Compose stack has no redundancy.

Stop older pre-release instances before upgrading a cluster. Metadata writers must participate in both the catalog lease and striped-fence protocols; older writers do not drain cached definitions. Mixed-version writers can violate cache coherence. The service reads older records and writes a versioned format. Rolling back after a new-format write requires a pre-upgrade backup. Do not assume mixed-version rolling upgrades are safe.

Integration checks exercise restart and failure paths, but host/zone failures and prolonged soak remain unvalidated. Monitor application logs, TiKV health, disk usage and request failures. Disk space from tombstones is reclaimed by normal TiKV GC and compaction.
