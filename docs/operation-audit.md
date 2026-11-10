# Operation implementation audit

Reviewed on 2026-11-10 at [`2dbe055`](https://github.com/infocusmodereal/tikronos/tree/2dbe0556d97865e8d99bd41326609e851d1ac8c4). This is a code-backed engineering checklist, not a performance benchmark or a claim of full DynamoDB parity. The dispatch inventory contains **24 operations**: 20 DynamoDB and four DynamoDB Streams operations. `/healthz` is an HTTP lifecycle endpoint, not an additional DynamoDB operation. Unsupported operations in otherwise valid requests return `UnknownOperationException`; transport/signature validation and, for a recognized target prefix, JSON parsing can reject the request first.

A checked box means **the dimension was reviewed**, not that the implementation has no issues. **Confirmed** identifies behavior visible in code. **Candidate**, **risk** and **investigation** require a focused experiment or compatibility test before implementation. Existing coverage is named below; untested suggestions are not presented as reproduced defects. This PR changes documentation only.

## Recommended order

| Priority | Finding | Evidence and next step | Guarantee to preserve |
| --- | --- | --- | --- |
| 1 | F10: measure synchronous catalog renewal | [`CatalogCache.acquire`](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt#L61) can renew a lease with a 2PC write on the triggering request; metadata writers wait in `drain`. Instrument renewal/drain latency before moving renewal off-request. | PD lease bounds, old-snapshot coverage, bounded renewal work and schema invalidation. |
| 1 | F11: classify 2PC primary failures | 1PC transport ambiguity is typed as `CommitUnknown`; 2PC primary-commit errors escape as generic exceptions. Add fault injection before refining error/metric classification. | No fresh replay of uncertain commits; distinguish confirmed abort from lost decision responses. |
| 1 | F1: use native batch reads | [`Batches.get`](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Batches.kt#L27) calls Get sequentially; `Transaction.prefetch` already calls SDK `Snapshot.batchGet`. Prototype bounded multi-get and compare RPCs, p99, memory and partial results. | Same request snapshot, absent/empty-value semantics, response budgets, capacity and UnprocessedKeys. Reuse the SDK concurrency bound. |
| 1 | F2: size scans to the caller | [`RangeReader.RPC_ROWS = 8`](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/RangeReader.kt#L156) serves Query, Scan, catalog and Streams. Add a bounded row/byte demand contract; use small limits for existence/retention probes. Key-only scans need a mode that does not discard empty-valued rows. | Reverse order, region churn, lock resolution, page limits and cursor progress. |
| 2 | F3: separate semantic metadata from progress | [`IndexBackfill.checkpoint`](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/IndexBackfill.kt#L60) rewrites Table for every row; `Transaction.write` expands tracked metadata writes into 1,024 fences. TTL checkpoints and tag writes also trigger broad metadata work. Each tracked key adds its own fence set, requiring at least eight 128-key batches for those fences alone. | Crash-safe checkpoints, atomic index activation, schema/TTL/stream invalidation and old-snapshot fencing. |
| 2 | F4: remove the stream-wide write hotspot through redesign | [`Streams.record`](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Streams.kt#L70) reads and writes one descriptor/head for every changed item. Retention rewrites the same descriptor. Evaluate separate trim watermarks, multiple shards or native CDC separately. | Per-item order, committed visibility, restart/resume, images, retention and stable shard/iterator contracts. |
| 2 | F5: reduce secondary-index payloads | [`maintainIndexes`](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/IndexMaintenance.kt#L11) stores the complete item for every index, even KEYS_ONLY/INCLUDE. Measure storage and write amplification before migrating records. | Introduce real same-snapshot base Gets for unprojected LSI reads before shrinking copies; preserve sparse membership, GSI restrictions, atomic updates and old records. |
| 3 | F6: reduce codec/response copies | `Records` stores version-prefixed JSON and API paths rebuild JSON trees; batch validation also re-encodes Puts, and each index repeats the complete item encoding. Profile before a codec or wire fast path. | All Value types, number semantics, size validation, legacy record compatibility and projection/return modes. |
| 3 | F7: make control-plane snapshots explicit | `Tables.find` and `Streams.decorate` use separate snapshots. Thread one snapshot through read-only descriptions where appropriate. | No mixed-generation table/stream description; mutation responses require a defined post-commit view. |
| 3 | F8: reclaim retired data and bound stream discovery | DeleteTable and GSI deletion retire namespaces; retention removes events but keeps descriptors. Design checkpointed cleanup and selective descriptor ranges. | Active snapshots, retained streams and recreated table IDs must remain safe. |
| 3 | F9: bounded concurrency at independent boundaries | BatchWrite is serial; region batches and all three maintenance families run serially. Evaluate one bounded boundary at a time; partition maintenance ownership across instances rather than duplicating scans. | Backpressure, no child tasks queued behind waiting parents in the same pool, ordered locks, unknown outcomes and shutdown drain. |

These are code-derived opportunities, not measured speedup estimates. F4 applies only when streams are enabled; F10 also affects plain tables. Their contribution to end-to-end tail latency has not been measured by this audit.

## Native responsibilities and boundaries

- **Native primitives, local coordinator:** TiKV supplies MVCC, Raft-backed durability, optimistic write conflicts and prewrite/commit/resolve-lock primitives. TiKronos coordinates them in AtomicCommit, Prewrite, SingleKeyCommit, TransactionResolution and RegionBatches: primary selection, timestamps, TTL, batching, commit ceilings and error handling remain ours. SDK region routing and lock resolution are reused. Eligible base-item writes use native 1PC; other writes use this local 2PC coordinator. Secondary cleanup already has a bounded executor and caller backpressure.
- **Still necessary translation:** attribute/key encoding, Dynamo expressions, indexes, wire errors, API budgets, catalog lifecycle, TTL policy and Streams semantics. A native KV transaction does not interpret these contracts.
- **Metadata caching:** startup scans up to 64 existing TiKronos catalog entries; remaining definitions load lazily. The in-process LRU holds at most 4,096 entries and a 16 MiB accounting budget, with one-second PD-timestamp leases and renewal ahead of expiry. It is not an unconditional eternal schema cache. CREATING indexes bypass caching; writers drain leases or use validation fences. When an acquisition reaches a CREATING or missing table, it has already fetched a new snapshot and read the lease/catalog before the caller falls back to a direct Get. Guarded item writes cannot take the 1PC path. A renewal that extends the shared lease performs 2PC inline; another instance may instead reuse an existing lease. DDL/tag/TTL drains can wait out the remaining one-second lease plus polling/RPC delays. An existing TiKronos database is discovered; arbitrary TiDB tables or unrelated raw keys are not imported.
- **Models and structure:** shared Table, Index, Value, write plans, leases and stream/TTL state already live in `model`; wire requests live in `protocol`; `codec` handles wire attributes, `storage/Records` the persisted format, and `expression`, `config` and `diagnostics` have their own domains. Root-package service classes are a possible domain-packaging cleanup. Keep local private Page/ReadGroup helpers local unless they become shared contracts. Package moves alone do not improve throughput.
- **Async is not automatically native:** Ktor admission/body handling suspends, but storage RPCs execute on a bounded blocking pool. Replacing blocking RPCs requires matching cancellation, request budgets and commit-ambiguity semantics. Coroutines around the same blocking calls do not eliminate RPC latency.

The current 1PC path requires exactly one ordinary item mutation and no validation guards (normally a leased catalog plan), and obtains a fresh second timestamp for its commit bound. Cached timestamps cannot replace that step. Reads of keys also written rely on native write conflicts; other dependencies need guards or valid lease coverage.

**Native protocol research, not a switch to enable:** TiKV supports [multi-key 1PC within one prewrite request](https://pingcap.github.io/tidb-dev-guide/understand-tidb/1pc.html) and [Async Commit](https://pingcap.github.io/tidb-dev-guide/understand-tidb/async-commit.html). Our `onePhaseBounds`/`validateOnePhase` explicitly restrict 1PC to one item, and Prewrite never sets `use_async_commit` or its secondary list. Broader use requires a separately validated coordinator/recovery design: region splits, explicit fallback, SDK lock recovery, fresh timestamps, lease ceilings and uncertain results. Background secondary cleanup is not native Async Commit.

**Maintenance capacity:** one scheduler runs index, TTL and stream work serially, then waits one second. Per visit, each index advances at most 32 rows, each TTL config backfills at most 32 rows, each instance processes at most 32 due candidates total, and retention visits at most 16 descriptors with 64 events each. Work duration and cursor rotation reduce those ceilings. Independent instances may overlap work; they do not own disjoint partitions. Sustained expiry/stream production can outpace physical cleanup even while logical retention remains enforced.

**Error/retry boundary:** the server does not replay ambiguous commits. 1PC ambiguity is explicitly typed and instrumented; 2PC primary-commit exceptions currently reach the generic HTTP 500 path without `unknown_outcome` classification (F11). A client may retry HTTP 500 according to its policy. BatchWriteItem ambiguity returns no UnprocessedItems response, including for already-completed entries; later entries are not started by the serial loop. Client retry/idempotency semantics need explicit validation, especially for conditional or non-idempotent updates.

Native references: [N1: Java SDK Snapshot.batchGet](https://github.com/tikv/client-java/blob/v3.3.5/src/main/java/org/tikv/common/Snapshot.java) and [KVClient region batching](https://github.com/tikv/client-java/blob/v3.3.5/src/main/java/org/tikv/common/KVClient.java) expose reusable snapshot multi-get; [N2: TiKV RawKV TTL](https://tikv.org/docs/7.1/develop/rawkv/ttl/) describes a different API from this transactional record path. Neither RawKV TTL nor RawKV CAS can simply replace atomic item/index/stream writes. TiDB-oriented coprocessor APIs are not a generic evaluator for this project's JSON layout.

## Per-operation checklist

### CreateTable

Evidence: [Tables.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Tables.kt), [TableWire.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/TableWire.kt), [IndexWire.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/IndexWire.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Partial: native MVCC transactions make catalog creation and optional stream configuration atomic. Schema, billing metadata and index definitions belong to the translator.
- [x] **Code structure:** TableWire/IndexWire already assemble descriptions, and models live in model. Thread a snapshot into Streams.decorate rather than duplicating response assemblers (F7).
- [x] **Clear inefficiencies:** Confirmed: a catalog write expands to 1,024 metadata fences (F3); enabling a stream also writes a tracked stream-config key, adding another 1,024. These are sequentially batched control-plane transactions.
- [x] **Edge cases and coverage:** Duplicate names are conflict-checked and unused AttributeDefinitions are rejected by IndexWire. Add focused tests for simultaneous creates with different schemas and the existing unused-definition check; full AWS validation equivalence is unproven.
- [x] **Parallelism or async:** Creation of independent tables can overlap across requests already. Do not split catalog and stream configuration into independent asynchronous commits.

### DescribeTable

Evidence: [Tables.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Tables.kt), [Streams.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Streams.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Partial: snapshot reads and leased catalog caching are native-backed; DynamoDB response assembly is local.
- [x] **Code structure:** Candidate: let the response assembler accept the caller snapshot, rather than having Streams.decorate allocate another snapshot (F7).
- [x] **Clear inefficiencies:** Confirmed: stream decoration always obtains another timestamp and reads stream configuration, plus the descriptor when configured; the table definition itself can be cached.
- [x] **Edge cases and coverage:** Risk: find and decorate use different snapshots, so concurrent schema/stream changes can produce a mixed-version description. Add a controlled concurrent update case before changing the contract.
- [x] **Parallelism or async:** Batch known metadata keys on one snapshot where possible. Descriptor lookup depends on the configured ARN; blindly launching both reads cannot remove that dependency.

### UpdateTable

Evidence: [Tables.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Tables.kt), [TableUpdates.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/TableUpdates.kt), [IndexBackfill.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/IndexBackfill.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Partial: native transactions protect metadata changes; GSI lifecycle, leases and backfill checkpoints are application protocols.
- [x] **Code structure:** TableUpdates separates validation from persistence. Candidate: store backfill progress independently of semantic Table definitions (F3).
- [x] **Clear inefficiencies:** Confirmed: IndexBackfill commits one row and a full catalog checkpoint per transaction, each catalog rewrite generating 1,024 fences; CREATING tables also pay failed lease-acquisition work and guarded 2PC item writes that can conflict with every checkpoint (F3). Deleting a GSI leaves its data namespace behind (F8).
- [x] **Edge cases and coverage:** Covered: resumed backfill, concurrent changes and corrupt-primary-key rejection. A corrupt row can stall CREATING indefinitely, with warning logs but no persistent failed status, blocking further GSI Create/Update. Add cross-instance index delete/recreate coverage and an operator-visible failure state.
- [x] **Parallelism or async:** Backfill is already asynchronous. The worker visits at most 32 rows per index per cycle. Batches/disjoint ranges need atomic checkpoints and ACTIVE publication; native batch reads can reduce the point re-reads after scanning.

### DeleteTable

Evidence: [Tables.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Tables.kt), [Streams.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Streams.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Partial: one native transaction retires catalog/TTL metadata and disables the stream. TiKV does not know the DynamoDB table lifecycle.
- [x] **Code structure:** Deletion is centralized. A future reclamation worker should own physical cleanup separately from the API transaction.
- [x] **Clear inefficiencies:** Confirmed gap: item/index namespaces and auxiliary metadata are not physically reclaimed (F8). DeleteTable always writes catalog and TTL-config keys, hence at least 2,048 fence mutations even with TTL disabled.
- [x] **Edge cases and coverage:** Covered: stale writers cannot cross schema replacement and a recreated name gets a new table ID. Reclamation must also protect old snapshots and retained stream records.
- [x] **Parallelism or async:** Asynchronous reclamation is a candidate. Do not issue an unsafe range delete during DeleteTable or weaken the lease drain to make DDL faster.

### ListTables

Evidence: [Tables.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Tables.kt), [storage/Storage.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/Storage.kt), [storage/RangeReader.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/RangeReader.kt).

- [x] **Native TiKV use:** Yes for access: an ordered TiKV catalog range scan implements name pagination; validation and response shaping stay local.
- [x] **Code structure:** Small wrapper over Storage.tables; reuse it for bounded discovery rather than creating a second catalog index.
- [x] **Clear inefficiencies:** Confirmed: fixed eight-row KvScan RPCs fetch values although listing only consumes names (F2). A native key-only mode must bypass RangeReader's empty-value filter and retain correct locked/deleted-key behavior. The default scan snapshot also uses the SDK timestamp path, bypassing TimestampOracle.
- [x] **Edge cases and coverage:** Limit and name format are checked. Add pagination coverage across region splits, empty pages and concurrent create/delete; pages intentionally do not share one lifetime snapshot.
- [x] **Parallelism or async:** No need to fan out a page of at most 100 names. Prefer larger bounded native pages; preserve ordered LastEvaluatedTableName.

### GetItem

Evidence: [Items.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Items.kt), [storage/Storage.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/Storage.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt), [storage/Records.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/Records.kt).

- [x] **Native TiKV use:** Yes for access: one fresh MVCC snapshot, cached-or-direct schema lookup and native point Get. Both consistency modes currently use this path.
- [x] **Code structure:** Items delegates storage and expressions appropriately. Keep parsing/projection shared with batches; avoid a new Get-specific record codec.
- [x] **Clear inefficiencies:** Confirmed: stored JSON is decoded into Values and projected/encoded again. Cache hits already avoid a schema RPC; do not report metadata reads as unconditional (F6).
- [x] **Edge cases and coverage:** Partial coverage: missing items through HTTP and schema replacement in storage/catalog tests. Add GetItem-specific key-type validation and concurrent delete/recreate with projection; storage coverage is not an API-level test.
- [x] **Parallelism or async:** The item address depends on schema; there is little intra-request parallelism. SDK async calls could reduce blocked threads only with an end-to-end cancellation/backpressure design (F9).

### PutItem

Evidence: [Items.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Items.kt), [ItemWriter.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/ItemWriter.kt), [model/WriteIntent.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/model/WriteIntent.kt), [storage/AtomicCommit.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/AtomicCommit.kt).

- [x] **Native TiKV use:** Yes for conflict/commit: one ordinary item mutation with no guards can use native 1PC, normally with a leased catalog plan; guarded or multi-key mutations use the local 2PC coordinator. Dynamo conditions and auxiliary index/TTL/stream updates remain translation work.
- [x] **Code structure:** Good shared ItemWriter boundary for single, batch and TTL writes; WriteIntent explicitly distinguishes unread replacement from read-modify-write.
- [x] **Clear inefficiencies:** Plain replacement without old values, capacity or side effects already skips the old-item read. Remaining confirmed costs include JSON re-encoding, full index copies and a shared stream head when enabled (F4/F5/F6).
- [x] **Edge cases and coverage:** Covered: native conflicts, conditional Put, lease ceilings and typed 1PC ambiguity. Missing: fault-injected 2PC primary-commit errors and their classification (F11). The server avoids replay; an HTTP 500 alone cannot prevent client retries.
- [x] **Parallelism or async:** Do not acknowledge before the durable decision. Independent requests already overlap; multi-region prewrite fan-out is a higher-risk candidate, preserving lock ordering and bounded cleanup (F9).

### DeleteItem

Evidence: [Items.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Items.kt), [ItemWriter.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/ItemWriter.kt), [storage/AtomicCommit.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/AtomicCommit.kt).

- [x] **Native TiKV use:** Yes: native Del and transactional conflict detection; conditional checks and removal of dependent index/TTL/stream records are coordinated by the translator.
- [x] **Code structure:** Shares the mutation boundary and return-value logic with PutItem; no separate deletion transaction implementation is needed.
- [x] **Clear inefficiencies:** Unconditional deletion on a plain table can already omit the prior read. Enabled side effects or requested old values require it; physical namespace cleanup is a separate issue.
- [x] **Edge cases and coverage:** Covered: ALL_OLD and TTL renewal protection at the shared writer. Add DeleteItem-specific missing-item and failed-condition tests; do not infer them from conditional Put coverage. Ambiguity remains subject to F11.
- [x] **Parallelism or async:** Index/stream/TTL changes must stay atomic with the item delete. Existing secondary cleanup can run after the durable primary; do not turn required side effects into fire-and-forget tasks.

### UpdateItem

Evidence: [Items.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Items.kt), [ItemWriter.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/ItemWriter.kt), [expression/RequestExpressions.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/expression/RequestExpressions.kt), [expression/Expressions.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/expression/Expressions.kt).

- [x] **Native TiKV use:** Partial: TiKV supplies MVCC and conflict detection, but has no direct RPC for this JSON expression language. Native RawKV CAS is not a replacement for multi-key transactional side effects.
- [x] **Code structure:** Expression parsing/evaluation is shared and models live with their domain. Keep retry evaluation inside ItemWriter; compiled expression reuse must not capture the old item.
- [x] **Clear inefficiencies:** Confirmed: updates read the old item and encode a complete replacement when a new item exists (or emit Del when the result is absent). Numeric ADD is evaluated in Kotlin; a hypothetical storage increment must preserve types, conditions, return values and indexes.
- [x] **Edge cases and coverage:** Covered: concurrent updates and expression cases. Investigation: removal-only updates on an absent item return no item in Items.updated; verify expected compatibility. Known nested-list/parenthesis differences remain.
- [x] **Parallelism or async:** Read, evaluate and commit are dependent. Parallelizing these phases would be incorrect; independent batch/request work is the safer target.

### Query

Evidence: [Reads.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Reads.kt), [ReadTarget.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/ReadTarget.kt), [KeyRange.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/KeyRange.kt), [storage/RangeReader.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/RangeReader.kt).

- [x] **Native TiKV use:** Partial: key predicates become native ordered range bounds, including reverse scans. Filters, projections, COUNT and Dynamo capacity accounting run locally.
- [x] **Code structure:** ReadTarget separates table/index semantics from traversal. The private Page helper belongs near collect; moving every private helper into model would add coupling, not clarity.
- [x] **Clear inefficiencies:** Confirmed: eight-row RPCs and full JSON decode even for COUNT/projection; index rows contain complete items (F2/F5). LSI unprojected reads use that copy, not a base-table RPC; shrinking copies requires adding a real same-snapshot fetch. Generic JSON filter pushdown is not supplied by this layout/SDK.
- [x] **Edge cases and coverage:** Covered: forward/reverse ordering, filters, projections and duplicate index sort values. Known HASH-only cursor differences remain. Add exact-byte-budget and region-split boundary cases.
- [x] **Parallelism or async:** Bounded one-page lookahead is a candidate, not unlimited region fan-out. Preserve order, one snapshot, evaluated-item Limit, pre-filter byte accounting and LastEvaluatedKey.

### Scan

Evidence: [Reads.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Reads.kt), [ReadTarget.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/ReadTarget.kt), [storage/Keys.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/Keys.kt), [storage/RangeReader.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/RangeReader.kt).

- [x] **Native TiKV use:** Partial: native range scans on one MVCC snapshot; Segment/TotalSegments map to disjoint encoded ranges. Filtering and wire responses are local.
- [x] **Code structure:** Shares Query machinery deliberately. Keep segment-boundary encoding in storage and request validation in Reads.
- [x] **Clear inefficiencies:** Confirmed: fixed small fetch size and full-value decoding; selective filters may require many RPCs before producing output (F2/F6).
- [x] **Edge cases and coverage:** Segment arguments and cursor bounds are validated. Numeric/binary segmented reads have HTTP coverage. Add cross-region and legacy-key cases; do not promise a stable snapshot across multiple client pages.
- [x] **Parallelism or async:** Clients can already run parallel segments. Internal prefetch needs a shared memory budget and cancellation; more API workers alone do not reduce the RPC count.

### BatchGetItem

Evidence: [Batches.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Batches.kt), [storage/Transaction.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/Transaction.kt).

- [x] **Native TiKV use:** Underused: individual Gets use native snapshots, but this API does not call Snapshot.batchGet, which Transaction.prefetch already uses (F1).
- [x] **Code structure:** Keep response-budget accounting as one deterministic aggregation step. A small shared snapshot multi-get helper can serve this API and transaction prefetch without sharing their validation policy.
- [x] **Clear inefficiencies:** Confirmed: group.keys.forEach performs serial point reads, including keys later returned unprocessed due to response/partition budgets; projected values are serialized for sizing and again in the response.
- [x] **Edge cases and coverage:** Duplicates, 100-key maximum and 16 MiB/per-partition budgets are handled. Add multi-region missing-key and partial-failure cases; a batchGet replacement must map unordered results to keys, distinguish absent items from failed reads and normalize empty-value absence consistently with directRead.
- [x] **Parallelism or async:** Best near-term native reuse: bounded Snapshot.batchGet chunks with the same snapshot, SDK region concurrency already capped by storageWorkers, and deterministic budget aggregation. Do not create one unbounded coroutine per key.

### BatchWriteItem

Evidence: [Batches.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Batches.kt), [protocol/BatchWrite.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/protocol/BatchWrite.kt), [ItemWriter.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/ItemWriter.kt).

- [x] **Native TiKV use:** Partial: each entry uses native transactions/1PC, but the batch is a serial API loop. A raw batchPut would lose index/condition/lifecycle guarantees.
- [x] **Code structure:** Preparation and execution are separated, and all item mutations share ItemWriter. Keep independent-entry execution distinct from one all-or-nothing transaction.
- [x] **Clear inefficiencies:** Confirmed: serial entry commits; Put validation encodes once in prepare and ItemWriter encodes again. Maximum is 100 entries, differing from AWS 25.
- [x] **Edge cases and coverage:** Covered: partial contention and synthetic CommitUnknown stop execution. Real 2PC ambiguity needs fault tests (F11); failures return HTTP 500 with no per-entry result. ReturnItemCollectionMetrics=SIZE is accepted but no metrics are assembled; make support explicit.
- [x] **Parallelism or async:** Candidate: bounded independent entry concurrency with isolated accumulators. On ambiguity, account for every already-started task; preserve UnprocessedItems for definite safe rejections, not uncertain commits. Stream-enabled/backfilling tables can conflict more under fan-out; benchmark them separately and avoid child-task deadlock in the parent storage pool.

### UpdateTimeToLive

Evidence: [Ttl.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Ttl.kt), [TtlWorker.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/TtlWorker.kt), [storage/TtlRecords.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/TtlRecords.kt).

- [x] **Native TiKV use:** Partial: native transactions persist policy and delete item/index/stream changes atomically. RawKV TTL cannot replace this transactional policy and side-effect protocol (N2).
- [x] **Code structure:** Policy model is separate from the worker. Candidate: separate operational cursor writes from semantic invalidation, as for index backfill (F3).
- [x] **Clear inefficiencies:** Confirmed: policy changes write at least 2,048 fences. Each worker visit point-reads up to 32 scanned items again and rewrites a 1,024-fence config checkpoint.
- [x] **Edge cases and coverage:** Covered: renewed expiration, bad stored keys and new-instance discovery. Generation checks exist but need a stale-candidate test. Add concurrent enable/disable and stalled-maintenance cases; expiry remains asynchronous.
- [x] **Parallelism or async:** Backfill and expiration already run off-request. Only 32 due candidates total are visited per instance/cycle. Native batch prefetch and partitioned/fair worker scheduling are candidates; never bypass the final item/policy recheck.

### DescribeTimeToLive

Evidence: [Ttl.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Ttl.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Yes for access: catalog and TTL configuration use one native snapshot; status formatting is local.
- [x] **Code structure:** Small control-plane read. Reuse catalog decoding where appropriate rather than introducing a second TTL cache.
- [x] **Clear inefficiencies:** Confirmed: table bytes are decoded with Records.table even if a decoded catalog plan exists; TTL configuration is read directly. Low priority compared with batch/scans.
- [x] **Edge cases and coverage:** Missing table and absent configuration are handled. Add a concurrent delete/recreate case to ensure a policy from an old table ID is never described for a replacement.
- [x] **Parallelism or async:** TTL config address depends on table ID. No clear need for fan-out; metadata caching must preserve enabled/disabled semantics and snapshot validity.

### TagResource

Evidence: [Catalog.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Catalog.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Partial: native atomic read-modify-write prevents lost tag changes. Tag validation and limits are local.
- [x] **Code structure:** Shared changeTags path is appropriate. Candidate: move tags to separately versioned metadata if tag traffic justifies a catalog-layout migration.
- [x] **Clear inefficiencies:** Confirmed: adding tags rewrites the full Table, drains leases and generates catalog fences although item schema is unchanged (F3).
- [x] **Edge cases and coverage:** Duplicate/reserved tags and total count are validated; table ID is rechecked in the transaction. Add concurrent additions near the 50-tag limit; preserve no-lost-update behavior.
- [x] **Parallelism or async:** Independent tables can proceed concurrently. Do not parallelize updates to the same tag map without native transactional conflict checks.

### UntagResource

Evidence: [Catalog.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Catalog.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Partial: native transactional map replacement; no TiKV concept of DynamoDB resource tags.
- [x] **Code structure:** Shares changeTags with additions. Keep this shared semantic rule rather than duplicating the mutation implementation.
- [x] **Clear inefficiencies:** Confirmed: even a removal that changes no tag still rewrites catalog/fences and may drain leases. A verified no-op path is a candidate.
- [x] **Edge cases and coverage:** Non-empty list/count and string types are checked. Validation gap to investigate: tagKeys does not apply the length/reserved-prefix checks used by parseTags.
- [x] **Parallelism or async:** No compelling per-request parallelism. If optimizing no-ops, recheck within the transaction so concurrent additions are not lost.

### ListTagsOfResource

Evidence: [Catalog.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Catalog.kt), [storage/CatalogCache.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/CatalogCache.kt).

- [x] **Native TiKV use:** Yes for access: cached-or-direct catalog read; ARN checks and sorted output are local.
- [x] **Code structure:** Read behavior shares Catalog.tags with writes; split methods only if operation-specific validation grows.
- [x] **Clear inefficiencies:** At most 50 tags are sorted in memory; no obvious material cost beyond the shared catalog path. Do not add a separate cache solely for this operation.
- [x] **Edge cases and coverage:** Any NextToken is rejected and the entire tag set is returned; explicit compatibility behavior. Add malformed ARN and same-name recreation cases.
- [x] **Parallelism or async:** No useful intra-request parallelism for a bounded in-memory map.

### DescribeEndpoints

Evidence: [Dynamo.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Dynamo.kt).

- [x] **Native TiKV use:** Not applicable: this is a protocol discovery response using the request Host, with no TiKV access.
- [x] **Code structure:** A small dispatch branch is sufficient; extract only if endpoint discovery gains independent policy.
- [x] **Clear inefficiencies:** No storage RPC and no clear operation-specific performance issue.
- [x] **Edge cases and coverage:** Candidate validation: missing Host and reverse-proxy/public-host behavior. This response does not discover multiple API instances or implement load balancing.
- [x] **Parallelism or async:** No storage work to parallelize; ordinary HTTP concurrency already applies.

### DescribeContinuousBackups

Evidence: [Catalog.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Catalog.kt).

- [x] **Native TiKV use:** Only existence checking uses native catalog reads. The response is a DISABLED status stub, not a backup implementation.
- [x] **Code structure:** Small and explicit; keep backup lifecycle logic out until actual support exists.
- [x] **Clear inefficiencies:** No material cost beyond catalog lookup. Do not infer TiKV backup capability from this response.
- [x] **Edge cases and coverage:** Documented gap: PITR and backup/restore operations are unsupported. Preserve ResourceNotFound for unknown tables.
- [x] **Parallelism or async:** No useful parallel work. Future backup support needs a separate consistent restore/lifecycle design.

### ListStreams

Evidence: [StreamReads.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamReads.kt), [storage/StreamRecords.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/StreamRecords.kt).

- [x] **Native TiKV use:** Partial: native descriptor range scan, then local table-name/retention filtering; no native DynamoDB stream registry.
- [x] **Code structure:** List/describe/iterator/records share StreamReads. A bounded descriptor-query helper is reasonable if a per-table registry is introduced.
- [x] **Clear inefficiencies:** Confirmed: filtering occurs before take(limit+1), so finding a small page can scan the entire descriptor namespace. Old descriptors are not deleted by StreamRetention (F8).
- [x] **Edge cases and coverage:** Limit is validated, but ExclusiveStartStreamArn is used as a lexical cursor without checking membership. Add invalid cursor, repeated enable/disable and selective-table pagination cases.
- [x] **Parallelism or async:** Prefer a bounded key-range/index redesign over parallel scanning of every descriptor. Keep deterministic pagination and old-stream retention.

### DescribeStream

Evidence: [StreamReads.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamReads.kt), [storage/RangeReader.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/storage/RangeReader.kt).

- [x] **Native TiKV use:** Partial: native descriptor Get plus a reverse range scan for logical retention. Shards and sequence ranges are translated locally.
- [x] **Code structure:** find centralizes retention checks across stream operations; preserve that reuse if storage helpers are extracted.
- [x] **Clear inefficiencies:** Confirmed: even describe performs a reverse scan to find the last expired event; eight-row fetches may overread for firstOrNull (F2).
- [x] **Edge cases and coverage:** Covered: closed empty stream bounds and old streams after recreation. Current design has one shard; multiple-shard topology would require API and token changes.
- [x] **Parallelism or async:** Descriptor contents determine the event namespace, so the two stages are dependent. A limit-aware scan helper is preferable to speculative fan-out.

### GetShardIterator

Evidence: [StreamReads.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamReads.kt), [StreamTokens.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamTokens.kt).

- [x] **Native TiKV use:** Partial: native descriptor/retention reads and transactional creation of a persistent secret. Token authentication and expiration are API responsibilities.
- [x] **Code structure:** StreamTokens is isolated; StreamPosition is in model. Do not move secret-management logic into generic record codecs.
- [x] **Clear inefficiencies:** Confirmed: one descriptor Get, retention scan and HMAC work; first secret access performs a transaction, subsequent accesses are lazy-cached.
- [x] **Edge cases and coverage:** Covered: authenticated tokens across instances and stream rotation. Add boundaries for AT/AFTER sequence numbers, trimming, exact expiry and clock skew; host time governs token/retention checks.
- [x] **Parallelism or async:** No clear latency win from parallelism in this short dependency chain. A new token cache would need invalidation and is not justified by evidence.

### GetRecords

Evidence: [StreamReads.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamReads.kt), [StreamTokens.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamTokens.kt), [StreamRetention.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/StreamRetention.kt), [Streams.kt](https://github.com/infocusmodereal/tikronos/blob/2dbe0556d97865e8d99bd41326609e851d1ac8c4/src/main/kotlin/tikronos/Streams.kt).

- [x] **Native TiKV use:** Partial: native snapshot/range reads provide committed records; retention, ordering, images and iterator semantics are an application stream protocol.
- [x] **Code structure:** Shared find/token helpers are sound. Stream writer/reader/retention changes must be reviewed together, especially for a new shard model.
- [x] **Clear inefficiencies:** Confirmed: descriptor Get, retention reverse scan and forward record scan per poll. A 1,000-record page needs roughly 125 full eight-row data fetches, plus retention/region/possible lookahead work; each event is parsed and serialized again. Writers and retention updates share the descriptor/head (F4).
- [x] **Edge cases and coverage:** Covered: images, retained old streams, expired records and token portability. Missing stress cases: retention racing pagination and wire-size limits with large binary old/new images.
- [x] **Parallelism or async:** Retention processes only 64 events per visited stream/instance/cycle. Bounded prefetch may help readers, while trim/head separation may reduce conflicts. Native CDC is a redesign candidate, not a drop-in replacement: resolved timestamps, resume tokens, ordering, images and retention need an explicit mapping (F4).

## Evidence and validation boundaries

The source links above are pinned to the audited revision. Inventory comes from `Dynamo.dispatch`, `dispatchCatalog` and `dispatchStream`, cross-checked with [Compatibility](compatibility.md). Shared paths were traced through ItemWriter, Transaction, AtomicCommit, Prewrite, CatalogCache, RangeReader and maintenance workers; a public operation being present does not mean all its optional parameters are supported.

Relevant existing coverage:

- `CatalogCacheIntegrationTest`, `CatalogRenewalIntegrationTest`, `MetadataFencesIntegrationTest`: schema/lease renewal, stale writers and metadata conflicts.
- `AtomicIntegrationTest`, `CommitIntegrationTest`, `OnePhaseCommitIntegrationTest`, `BatchRetryTest`: concurrent mutations, commit bounds and unknown outcomes.
- `IndexBackfillIntegrationTest`, `StreamTtlIntegrationTest`: restart/checkpoints, bad stored data, stream images/iterators and expiration renewal.
- `integration/lock.py` (concurrent conditional Put), `integration/items.py`, `integration/indexes.py`, `integration/lifecycle.py`, `integration/transport.py`: API expressions, pagination, projections, lifecycle, restart and admission/drain.

At the audited revision, the suite contains 37 unit tests and 43 TiKV integration tests. The [CI run for that revision](https://github.com/infocusmodereal/tikronos/actions/runs/42838287753) passed, including HTTP lifecycle and packaged-service checks. Existing tests do not validate the unimplemented candidates above. No new performance run or exhaustive AWS conformance test was performed for this audit. Add only the smallest missing failure case when implementing a finding; do not duplicate the same contract across all callers.
