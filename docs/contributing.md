# Contributing

Open an issue for a reproducible bug or a focused feature proposal. Send changes through a fork and pull request to `main`. Keep code, tests and documentation proportional to the change. Describe the resulting behavior, relevant validation and remaining limits.

## Development checks

Use Java 21:

```sh
./gradlew check installDist
```

`check` runs detekt and unit tests. For integration validation, start the dedicated fixture stack with ports 2379, 20160 and 8000 available:

```sh
docker compose -f integration/compose.yaml up -d
# Wait for PD and TiKV to be ready before running the checks.
TIKRONOS_TEST_PD=127.0.0.1:2379 ./gradlew integrationTest
python3 integration/check.py
docker compose -f integration/compose.yaml down
```

The HTTP runner owns its JVM lifecycle. Use isolated development data for integration tests.

## Documentation

Use Python 3.13 and a virtual environment:

```sh
python3 -m venv .venv-docs
.venv-docs/bin/pip install -r docs/requirements.txt
.venv-docs/bin/mkdocs build --strict
.venv-docs/bin/mkdocs serve
```

Documentation lives under `docs/`. CI builds it on pull requests and publishes only from `main` to GitHub Pages. Broken local links and build warnings fail validation.

## Reviews and security

Changes to `main` require review and passing CI. The repository owner retains bypass for maintenance. Workflow runs from external contributors require maintainer approval; this is separate from approving the code change.

Report security vulnerabilities through [private vulnerability reporting](https://github.com/infocusmodereal/tikronos/security/advisories/new), not a public issue. Include affected versions, impact and a minimal reproduction without credentials or private data. See the [security policy](https://github.com/infocusmodereal/tikronos/blob/main/SECURITY.md).
