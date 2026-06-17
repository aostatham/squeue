# Release Checklist

Use this checklist from a clean checkout before tagging or publishing a release.

## Required Verification

Docker must be available for the PostgreSQL/Testcontainers suites. A skipped Docker-backed suite is a release blocker.

```sh
./mvnw clean test
./mvnw verify -Pfull-contract
cd sequenced-queue-python-client
python -m pytest
cd ..
docker build -t sequenced-queue-server:0.1.0 -f sequenced-queue-server/Dockerfile .
docker run --rm sequenced-queue-server:0.1.0 --help
find . -path "*/db/migration/*" -type f | sort
```

## Manual Checks

- OpenAPI YAML parse passes.
- Schema baseline remains `V1`.
- No stale V2/V3/V4 schema references remain outside historical release notes or design-history context.
- Java examples compile.
- Python examples compile.
- Docker-backed tests run with no skips.
- Production queue SQL remains only in `sequenced-queue-core`.
- Direct Java client required schema version matches the latest Flyway migration.
- README links to release, versioning, security, semantics, quickstart, and examples docs are valid.
- Release notes are updated in `CHANGELOG.md`.
- Maven and Python package versions match the intended release.
- Docker image build documentation is current.
- Working tree is clean before tagging.
- No force push is used.
- Final tag is `v0.1.0`.

## Artifact Review

- Maven group/artifact/version coordinates are final for the release.
- Python package name and version are final for the release.
- OpenAPI `info.version` matches the release.
- License, SCM, project URL, developer, and organization metadata are either finalized before public publishing or explicitly treated as provisional.

## Non-Goals For This Release

- CLI
- UI
- batching
- source draining
- REST/WebSocket/SSE worker wake-up
- PostgreSQL `LISTEN/NOTIFY` as durable queue storage
- database-trigger-based notification mechanisms
- broker bridges
- queue-level database configuration
- archive tables
- OAuth/OIDC
- database-backed API key lifecycle
- performance benchmark implementation
- queue semantics changes
