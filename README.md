# TiKronos

A DynamoDB-compatible HTTP service backed by TiKV. Point your DynamoDB client at TiKronos; TiKV stores the tables and items and provides transactional persistence.

TiKronos is a standalone Kotlin application running on Ktor, Netty and Java 21. PD and TiKV run separately. It is **experimental** and implements a subset of DynamoDB, not complete API parity.

[Documentation](https://infocusmodereal.github.io/tikronos/) | [Compatibility](https://infocusmodereal.github.io/tikronos/compatibility/) | [Contributing](docs/contributing.md)

## Quickstart

With Docker Compose and the AWS CLI installed:

```sh
git clone https://github.com/infocusmodereal/tikronos.git
cd tikronos
docker compose up --build -d

export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local-development-only
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""
unset AWS_SESSION_TOKEN

aws dynamodb list-tables --endpoint-url http://localhost:8000
```

The stack exposes the API on loopback only. These are public development credentials. Storage may take a moment to start; retry the signed request if needed. The [quickstart](docs/quickstart.md) shows how to create a table and read and write items.

Stop with `docker compose down`; named volumes retain the data. Images are currently built from source. For an existing TiKV cluster, see [container and Java distribution setup](docs/configuration.md).

## What's supported

- Table management, item CRUD and conditional writes.
- Query, Scan, batch operations and secondary indexes.
- TTL and DynamoDB Streams, with documented compatibility limits.

Transaction APIs, IAM authorization and backup/restore APIs are not implemented. Read the [compatibility guide](docs/compatibility.md) and [operating and upgrade requirements](docs/operations.md) before using persistent data. Terraform locking guidance is in the [quickstart](docs/quickstart.md#terraform-locking).

## Development

With Java 21 installed:

```sh
./gradlew check installDist
```

See [Contributing](docs/contributing.md) for TiKV integration tests and documentation checks. Bug reports and focused pull requests are welcome. Report vulnerabilities through the [security policy](SECURITY.md).

## Name and license

TiKronos combines TiKV with a play on Chronos and Kronos (Cronus), the Titan who devoured his children. The idea is to give TiKV an appetite for DynamoDB workloads. Bring yours.

[MIT](LICENSE).
