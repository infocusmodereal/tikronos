# TiKronos

Give TiKV an appetite for DynamoDB workloads.

TiKronos is a standalone Kotlin service that accepts DynamoDB HTTP requests and stores data in TiKV. It runs on Ktor, Netty and Java 21. Applications connect through a DynamoDB SDK with a custom endpoint; PD and TiKV run separately.

```text
DynamoDB client -> TiKronos HTTP service -> TiKV
                         |                  |
                         +------ PD --------+
```

## Start here

- [Quickstart](quickstart.md): launch the local stack and create your first table.
- [Configuration](configuration.md): credentials, listen addresses and packaging.
- [Compatibility](compatibility.md): implemented APIs and known differences.
- [Operations](operations.md): storage, health checks, upgrades and limits.
- [Contributing](contributing.md): propose a change or report a problem.

## Project status

TiKronos is experimental. Conditional writes support state-locking use cases, and the service also implements item operations, queries, secondary indexes, TTL and streams. Implemented operations do not imply complete DynamoDB parity. Read the compatibility and operations pages before relying on it for persistent workloads.

TiKronos takes its name from TiKV and a play on Chronos and Kronos (Cronus), the Titan who devoured his children. The idea is to help TiKV devour DynamoDB workloads. Bring yours.

The project is available under the [MIT license](https://github.com/infocusmodereal/tikronos/blob/main/LICENSE).
