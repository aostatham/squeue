# Changelog

## [Unreleased]

## [0.1.0] - 2026-06-15

First MVP release of `sequenced-queue`.

Includes:

- PostgreSQL-backed durable source-ordered queue.
- REST server.
- Java REST client.
- Python REST client.
- Direct Java/PostgreSQL client for trusted internal deployments.
- Shared core queue semantics.
- Worker leases, heartbeat, retry/backoff, dead-letter blocking, and admin repair.
- Operational baseline with health, metrics, and admin audit.
- OpenAPI documentation.
- Docker packaging.
- Pre-release schema baseline `V1`.
- Stage 3A global size limits and `FIELD_TOO_LARGE` error semantics.

Guarantees:

- Strict per-source ordering.
- Parallel processing across different sources.
- At-least-once delivery.

Non-guarantees:

- Exactly-once side effects.
- Global ordering.
- Pub/sub or broker replacement semantics.

## [0.1.0-rc3] - 2026-06-15

- Includes Stage 3A global size limits for payloads, headers, completion results, error details, admin reasons, and admin metadata.
- Adds `FIELD_TOO_LARGE` structured error semantics.
- Adds direct Java `QueueFieldTooLargeException`.
- Preserves the pre-release schema baseline `V1`.
- Adds Stage 3A size-limit design documentation.
- Keeps direct Java/PostgreSQL client support mandatory and core-backed.
- No queue semantics changed.
- No schema migration was added.

## [0.1.0-rc2] - 2026-06-14

- Superseded before MVP release.

## [0.1.0-rc1] - 2026-06-14

First correctness and developer-experience release candidate for `sequenced-queue`.

This release candidate is not a v1.0 production maturity claim. It establishes the current queue semantics, operational checks, clients, examples, and Docker-backed verification baseline.

### Added

- PostgreSQL-backed durable queue storage with strict per-source ordering.
- REST API for enqueue, claim, complete, fail, heartbeat, source inspection, admin repair, and manual retention purge.
- Java REST client and polling worker helper.
- Python REST client and polling worker helper.
- Trusted direct Java/PostgreSQL client that delegates to the shared core implementation.
- Internal Java worker loop shared by Java REST and direct worker helpers.
- Shared `sequenced-queue-core` implementation for production SQL, queue semantics, validation, and schema compatibility checks.
- Flyway-managed pre-release schema baseline `V1`.
- API key baseline with separate worker and admin keys.
- Admin audit table and audit records for successful admin repair and retention purge operations.
- Micrometer metrics and Actuator health checks.
- Runnable Java and Python producer/worker examples.
- Docker-backed PostgreSQL contract test profiles.

### Changed

- Maven artifacts are aligned to release candidate version `0.1.0-rc1`.
- Python package metadata is aligned to PEP 440 release candidate version `0.1.0rc1`.
- OpenAPI metadata reports `0.1.0-rc1`.

### Fixed

- Direct Java and REST paths share core error codes and queue validation.
- Direct Java worker helpers suppress complete/fail after lease loss.
- Manual retention purge is bounded and limited to passable terminal statuses.
- OpenAPI coverage includes security schemes, typed response objects, examples, and route drift checks.

### Security

- Worker and admin API keys are distinct static configured secrets.
- Server startup rejects equal worker and admin API keys.
- REST error bodies avoid echoing payload, header, API key, idempotency key, and handler error text in limit failures.
- Least-privilege PostgreSQL role guidance is documented.

### Operational

- Health reports schema baseline compatibility, required schema baseline, table presence, database reachability, and recovery status.
- Metrics cover queue depth, source state, claims, completions, failures, heartbeats, lease expiries, and admin operations.
- Docker-backed PostgreSQL contract suites must run without skips for release approval.

### Developer Experience

- Developer quickstart covers PostgreSQL startup, server startup, enqueue, Java worker, Python worker, and admin inspection.
- Examples include Java REST producer, Java REST worker, Java direct worker, Python producer, and Python worker.
- Documentation covers worker lifecycle, heartbeat behavior, lease-lost behavior, retryable vs non-retryable errors, schema compatibility, security, and queue semantics.

### Known Limitations

- Delivery is at-least-once, not exactly-once. Handlers must be idempotent.
- There is no global ordering across different sources.
- There is no queue-level configuration table; queue configuration is global-only.
- There is no CLI, UI, batching, source draining, LISTEN/NOTIFY, broker bridge, archive table, OAuth/OIDC, or database-backed API key lifecycle.
- Direct Java access is trusted/internal only and bypasses REST API-key enforcement.
