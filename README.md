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

The initial API supports endpoint discovery, table create/describe/list/delete, and item put/get/delete. Tables have one string partition key and items contain string attributes. Reads support top-level projections. Writes support `attribute_not_exists` on the partition key and `ALL_OLD`. Dropping a table retires its storage namespace; physical reclamation is pending.

For Terraform's S3 backend, point the DynamoDB endpoint at TiKronos and create a table with a string key named `LockID`. The state file stays in object storage. Terraform has deprecated DynamoDB-based locking. [Backend documentation](https://developer.hashicorp.com/terraform/language/backend/s3).
