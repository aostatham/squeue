# Release Checklist

Use this checklist from a clean checkout before tagging or publishing a release candidate.

## Required Verification

Docker must be available for the PostgreSQL/Testcontainers suites. A skipped Docker-backed suite is a release blocker.

```sh
./mvnw clean test
./mvnw verify -Ppostgres-contract
./mvnw verify -Pdeveloper-contract
./mvnw verify -Pfull-contract
cd sequenced-queue-python-client && python -m pytest
```

## Manual Checks

- OpenAPI YAML parse passes.
- Java examples compile.
- Python examples compile.
- Docker-backed tests run with no skips.
- Production queue SQL remains only in `sequenced-queue-core`.
- Direct Java client required schema version matches the latest Flyway migration.
- README links to release, versioning, security, semantics, quickstart, and examples docs are valid.
- Release notes are updated in `CHANGELOG.md`.
- Maven and Python package versions match the intended release candidate.
- Docker image build documentation is current.
- Working tree is clean before tagging.

## Artifact Review

- Maven group/artifact/version coordinates are final for the release candidate.
- Python package name and version are final for the release candidate.
- OpenAPI `info.version` matches the release candidate.
- License, SCM, project URL, developer, and organization metadata are either finalized before public publishing or explicitly treated as provisional.

## Non-Goals For This Release

- CLI
- UI
- batching
- source draining
- LISTEN/NOTIFY
- broker bridges
- queue-level database configuration
- archive tables
- OAuth/OIDC
- database-backed API key lifecycle
- performance benchmark implementation
- queue semantics changes
