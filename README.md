# TiKronos

TiKronos aims to let applications use the DynamoDB API with TiKV as their storage engine. Its first compatibility target is conditional writes for state locking.

TiKronos takes its name from TiKV and a play on Chronos and Kronos (Cronus), the Titan who devoured his children in Greek mythology. The idea is to give TiKV an appetite for DynamoDB workloads. Bring yours.

## Run locally

With JDK 17 and a running TiKV cluster, point TiKronos at its PD endpoint:

```sh
TIKRONOS_PD=127.0.0.1:2379 ./gradlew run
```

The HTTP endpoint defaults to `127.0.0.1:8000`. Loopback supports unsigned requests for development. To verify SigV4, set `TIKRONOS_ACCESS_KEY`, `TIKRONOS_SECRET_KEY` and optionally `TIKRONOS_REGION` (default `us-east-1`). Other listen addresses require credentials. Use a TLS terminator for remote access. Run `./gradlew check` for static analysis and unit tests.

When upgrading a cluster that ran an earlier pre-release, stop all old instances before starting the new ones. The service reads existing records and writes a versioned format. Rolling back after the first write requires restoring a pre-upgrade backup.

## DynamoDB compatibility

TiKronos supports table management, endpoint discovery, item CRUD, Query, Scan, BatchGetItem and BatchWriteItem. Partition and optional sort keys accept strings, numbers or binary data. Items can also contain booleans, nulls, documents and sets. Conditions, updates, filters and projections share expression evaluation. Item writes re-evaluate conditions against TiKV after a write conflict.

Query and Scan return up to 1 MiB before filtering. Forward and reverse queries read bounded TiKV ranges at a single snapshot per request. Parallel Scan assigns each worker a disjoint key range. Batch requests accept up to 100 item keys or writes. Retry the entries returned in UnprocessedKeys or UnprocessedItems. Dropping a table retires its storage namespace; physical reclamation is pending.

For Terraform's S3 backend, point the DynamoDB endpoint at TiKronos and create a table with a string key named `LockID`. The state file stays in object storage. Terraform has deprecated DynamoDB-based locking. [Backend documentation](https://developer.hashicorp.com/terraform/language/backend/s3).
