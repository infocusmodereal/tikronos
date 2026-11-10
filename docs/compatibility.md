# DynamoDB compatibility

TiKronos implements a subset of the DynamoDB and DynamoDB Streams APIs. Support here means the operation is implemented, not that every AWS behavior or parameter is identical.

| Area | Operations |
| --- | --- |
| Tables | `CreateTable`, `DescribeTable`, `UpdateTable`, `DeleteTable`, `ListTables` |
| Items | `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` |
| Reads and batches | `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem` |
| TTL | `UpdateTimeToLive`, `DescribeTimeToLive` |
| Tags | `TagResource`, `UntagResource`, `ListTagsOfResource` |
| Discovery and backup status | `DescribeEndpoints`, `DescribeContinuousBackups` |
| Streams | `ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords` |

For implementation details and follow-up opportunities, see the [operation audit](operation-audit.md).

## Data and queries

Partition and optional sort keys accept strings, numbers or binary values. Attributes can also contain booleans, nulls, maps, lists and sets. Conditions, updates, filters and projections share expression evaluation. Conditional writes re-evaluate conditions after TiKV write conflicts.

Query and Scan return up to 1 MiB before filtering, with a snapshot per request. Queries support forward and reverse traversal. Parallel Scan assigns disjoint key ranges. Use returned pagination keys; pages are separate requests, not a snapshot of the whole traversal.

Both batch operations accept up to 100 entries. `BatchWriteItem` therefore differs from AWS's 25-operation limit. Retry only returned `UnprocessedKeys` or `UnprocessedItems` with backoff. Client-side SDK validation may impose AWS limits before sending a request.

## Indexes, TTL and streams

Global and local secondary indexes support sparse keys and projections. New global indexes backfill in bounded batches with persistent checkpoints before becoming ACTIVE. TTL rechecks the current item before deleting it. Item, index and stream changes commit atomically in TiKV.

Streams expose one shard per stream, four image modes, authenticated 15-minute iterators and 24-hour retention. Re-enabling a stream creates a new ARN; retained records from the previous stream remain readable.

## Known differences and gaps

- Expression handling differs for redundant parentheses and nested `list_append`.
- Some Query/Scan cursor validation and error wording differ, including HASH-only Query with `ExclusiveStartKey`.
- Provisioned capacity is reported but not enforced. Backup status is DISABLED; no backup/restore API is implemented.
- Transaction APIs, global tables and PartiQL are not implemented.
- Cached table definitions use coordinated leases. Writes to the same item and an enabled stream head still contend; schema changes drain catalog leases. See [Operations](operations.md#table-catalog). Index copies retain complete items internally.
- Deleting a table or index retires its namespace; physical reclamation is pending.

The integration tests cover specific API, concurrency and recovery cases. Passing them does not establish complete DynamoDB parity or production readiness. See [Contributing](contributing.md#development-checks) for reproducible checks.
