# TiKronos

[Documentation](https://infocusmodereal.github.io/tikronos/) | [Quickstart](https://infocusmodereal.github.io/tikronos/quickstart/)

TiKronos aims to let applications use the DynamoDB API with TiKV as their storage engine. Its first compatibility target is conditional writes for state locking.

TiKronos takes its name from TiKV and a play on Chronos and Kronos (Cronus), the Titan who devoured his children in Greek mythology. The idea is to give TiKV an appetite for DynamoDB workloads. Bring yours.

## Run locally

With JDK 21 and a running TiKV cluster, point TiKronos at its PD endpoint:

```sh
TIKRONOS_PD=127.0.0.1:2379 ./gradlew run
```

The HTTP endpoint defaults to `127.0.0.1:8000`. Loopback supports unsigned requests for development. To verify SigV4, set `TIKRONOS_ACCESS_KEY`, `TIKRONOS_SECRET_KEY` and optionally `TIKRONOS_REGION` (default `us-east-1`). Other listen addresses require credentials. Use a TLS terminator for remote access. Run `./gradlew check` for static analysis and unit tests.

When upgrading a cluster that ran an earlier pre-release, stop all old instances before starting the new ones. The service reads existing records and writes a versioned format. Rolling back after the first write requires restoring a pre-upgrade backup.

## Container and distribution

TiKronos is a standalone Kotlin service using Ktor with Netty. Clients use the DynamoDB HTTP endpoint; PD and TiKV run separately. Blocking TiKV calls run on dedicated threads, with eight storage calls and sixteen incoming bodies admitted at a time. Body reads allow three seconds of inactivity and thirty seconds total, with a shared 64 MiB raw-buffer budget. Completed bodies wait up to five seconds for a storage worker. Excess requests receive a retryable throttling response before execution. Shutdown rejects new requests and drains admitted requests for up to ten seconds before stopping the HTTP engine.

For a single-node development environment:

```sh
docker compose up --build -d
```

Use endpoint `http://localhost:8000`, region `us-east-1`, access key `local` and secret `local-development-only`. These credentials are for this loopback-only example. PD and TiKV ports stay inside the Compose network. Startup may take a moment before storage is available. `GET /healthz` is a process liveness check, not a storage readiness check. Stop with `docker compose down`; named volumes retain data.

To connect to an existing cluster, build the image and provide credentials through your environment:

```sh
docker build -t tikronos:local .
docker run --rm --memory 2g -p 127.0.0.1:8000:8000 --stop-timeout 60 \
  -e TIKRONOS_PD -e TIKRONOS_ACCESS_KEY -e TIKRONOS_SECRET_KEY \
  tikronos:local
```

The container runs as UID 10001 on Java 21, with root-owned application files. Its heap defaults to 70% of the container memory limit, and the JVM exits on heap exhaustion. Override `JAVA_OPTS` to tune this; JSON parsing and native buffers also need memory. PD and every advertised TiKV address must be reachable from the container; `localhost` refers to the container itself. TLS termination remains external. Images are built locally; no registry release is published yet.

For a host with Java 21, `./gradlew installDist` creates `build/install/tikronos/bin/tikronos` and its libraries. `./gradlew distTar distZip` produces portable distributions under `build/distributions`.

## DynamoDB compatibility

TiKronos supports table management and tags, endpoint discovery, item CRUD, Query, Scan, BatchGetItem and BatchWriteItem. Partition and optional sort keys accept strings, numbers or binary data. Items can also contain booleans, nulls, documents and sets. Conditions, updates, filters and projections share expression evaluation. Item writes re-evaluate conditions against TiKV after a write conflict.

Query and Scan return up to 1 MiB before filtering. Forward and reverse queries read bounded TiKV ranges at a single snapshot per request. Parallel Scan assigns each worker a disjoint key range. Batch requests accept up to 100 item keys or writes. Retry the entries returned in UnprocessedKeys or UnprocessedItems. Dropping a table retires its storage namespace; physical reclamation is pending.

Global and local secondary indexes support sparse keys and projections. New global indexes backfill in bounded batches with persistent checkpoints before becoming ACTIVE. TTL indexes expiration candidates and rechecks the current item before deletion. Item changes, secondary indexes and stream records commit together. Streams expose one shard per stream, four image modes, authenticated 15-minute iterators and 24-hour retention. Re-enabling a stream creates a new ARN; old retained records remain readable.

This is experimental software. Metadata dependencies currently serialize writes within a table. Index copies retain complete items internally, and physical reclamation of deleted table and index namespaces is pending. Provisioned capacity is reported, not enforced; continuous backups report DISABLED. Single-node integration checks do not establish multi-node fault tolerance or production readiness.

For Terraform's S3 backend, point the DynamoDB endpoint at TiKronos and create a table with a string key named `LockID`. The state file stays in object storage. Terraform has deprecated DynamoDB-based locking. [Backend documentation](https://developer.hashicorp.com/terraform/language/backend/s3).
