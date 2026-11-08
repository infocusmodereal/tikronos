# Configuration

## Runtime settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `TIKRONOS_PD` | `127.0.0.1:2379` | PD endpoint for the TiKV client |
| `TIKRONOS_ACCESS_KEY` | Unset | Access key accepted by SigV4 verification |
| `TIKRONOS_SECRET_KEY` | Unset | Matching secret; set both credential variables together |
| `TIKRONOS_REGION` | `us-east-1` | Region accepted by signature verification |
| `TIKRONOS_STORAGE_WORKERS` | 8 per available CPU, capped at 128 | Blocking storage workers; accepts 1-256 |
| `TIKRONOS_MAX_REQUESTS` | Twice the worker count | Admitted requests; between the worker count and 512 |
| `TIKRONOS_ONE_PHASE_COMMIT` | `true` | Native 1PC for eligible item writes; accepts `true` or `false` |
| `--listen host:port` | `127.0.0.1:8000` | HTTP bind address; Docker defaults to `0.0.0.0:8000` |
| `JAVA_OPTS` | Container: `-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError` | JVM settings for the distribution launcher |

Unsigned requests are allowed only when credentials are unset and the listener is on loopback. Non-loopback listeners require credentials. SigV4 uses a configured credential pair, not AWS IAM policies or per-table authorization. Keep the service on a trusted network and use a TLS terminator for remote access.

Clients must sign with the configured `TIKRONOS_REGION`. Temporary credentials with session tokens, presigned query authentication and unsigned payloads are not supported.

## Run from source

With Java 21 installed and TiKV running:

```sh
TIKRONOS_PD=127.0.0.1:2379 ./gradlew run
```

To change the listener, provide credentials in the environment and use:

```sh
./gradlew run --args='--listen 0.0.0.0:8000'
```

## Connect a container to an existing cluster

Set `TIKRONOS_PD`, `TIKRONOS_ACCESS_KEY` and `TIKRONOS_SECRET_KEY` in your environment, then run:

```sh
docker build -t tikronos:local .
docker run --rm --memory 2g -p 127.0.0.1:8000:8000 --stop-timeout 60 \
  -e TIKRONOS_PD -e TIKRONOS_ACCESS_KEY -e TIKRONOS_SECRET_KEY \
  tikronos:local
```

PD and every advertised TiKV address must be reachable from the container. `localhost` inside a container refers to that container. The image runs as UID/GID 10001 with root-owned application files. Heap, native buffers and JSON parsing all need memory; the example limit is a starting point, not a sizing guarantee.

Images are built locally; there is no published registry image yet.

## Host distributions

```sh
./gradlew installDist distTar distZip
```

Run `build/install/tikronos/bin/tikronos` on a host with Java 21. Archives under `build/distributions/` include the launcher, libraries and license; the host supplies Java.
