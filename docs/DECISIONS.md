# DECISIONS.md - sequenced-queue

This file records architectural and implementation decisions for `sequenced-queue`.

`sequenced-queue` is a standalone PostgreSQL-backed queue component that provides durable at-least-once work dispatch with strict per-source ordering.

## SQ-001 - Build a Standalone Reusable Source-Ordered Queue Component

Status: Accepted.

Build a standalone component named `sequenced-queue`. It provides durable queue items, multiple producers, multiple consumers, per-source strict ordering, concurrent processing across different sources, worker leases, retry/backoff, dead-letter handling, Java and Python REST clients, a REST API, and an optional trusted direct Java/PostgreSQL client.

The project boundary is a reusable queue component, not a private application table.

## SQ-002 - Use PostgreSQL as the First Persistence and Queue Backend

Status: Accepted.

Use PostgreSQL as the first and only production backend for the MVP. PostgreSQL provides transactional writes, row-level locking, `FOR UPDATE SKIP LOCKED`, JSONB payload storage, indexing, and strong Testcontainers support.

The queue is not a general broker replacement.

## SQ-003 - Guarantee Ordering per `(queueName, sourceId)`

Status: Accepted.

The core invariant is:

```text
For each (queueName, sourceId), queue items are processed in sequence order and are not processed concurrently.
```

Items with different `sourceId` values may be processed concurrently. There is no global ordering across sources.

## SQ-004 - Use At-Least-Once Delivery, Not Exactly-Once Side Effects

Status: Accepted.

The queue guarantees durable at-least-once delivery. It does not guarantee exactly-once side effects. Consumers must use idempotent handlers.

## SQ-005 - Use Source-Level Leasing to Preserve Per-Source Ordering

Status: Accepted.

Workers claim a source lease before claiming an item. The source, not the individual item row, is the unit of concurrency. This prevents two workers from processing different items for the same source at the same time.

## SQ-006 - Use Queue Item Sequence Numbers per Source

Status: Accepted.

Each item receives a `sequence_no` assigned transactionally per `(queueName, sourceId)`. The database enforces uniqueness for `(queue_name, source_id, sequence_no)`.

## SQ-007 - Use HTTP/REST as the Default External Client Protocol

Status: Accepted.

Expose queue operations through REST endpoints under `/queues/{queueName}` and admin endpoints under `/admin/queues/{queueName}`.

## SQ-008 - Provide Java and Python REST Client Libraries

Status: Accepted.

Provide Java and Python REST clients and worker helpers for common producer and worker usage. These clients do not access PostgreSQL directly.

## SQ-009 - Provide a Direct Java to PostgreSQL Client for Trusted Deployments

Status: Accepted.

Provide a trusted/internal direct Java client for deployments that can safely talk to PostgreSQL. The client delegates to `sequenced-queue-core` and shares the same production SQL and semantics as the REST server.

## SQ-010 - Do Not Provide a Direct Python to PostgreSQL Client Initially

Status: Accepted.

Python support is REST-only. A direct Python/PostgreSQL client would duplicate core queue semantics outside the Java core and is intentionally not part of the MVP.

## SQ-011 - REST and Direct Java Clients Must Share the Same Queue Semantics

Status: Accepted.

REST and direct Java access both use `sequenced-queue-core`. Production queue SQL belongs in core, and both access paths must remain compatible under PostgreSQL contract tests.

## SQ-012 - Dead-Lettered Head Items Block Their Source by Default

Status: Accepted.

A `dead_lettered` head item blocks later items for the same source until an admin retry, skip, cancel, or unblock flow repairs the source.

## SQ-013 - Use Worker Leases and Heartbeat for Crash Recovery

Status: Accepted.

Claimed work uses source and item leases identified by `workerId`, `leaseId`, and `lease_until`. Workers heartbeat during processing. Expired leases are recovered without violating per-source ordering.

## SQ-014 - Use Idempotency Keys for Duplicate Enqueue Protection

Status: Accepted.

When an `idempotencyKey` is supplied, enqueue is idempotent per `(queueName, idempotencyKey)`. This prevents duplicate queue records but does not make handler side effects exactly-once.

## SQ-015 - Keep the MVP Focused

Status: Accepted.

The MVP is durable source-ordered work dispatch. It does not include broker semantics, pub/sub, fanout, priority queues, stream replay, consumer groups, CLI, UI, batching, `LISTEN/NOTIFY`, broker bridges, OAuth/OIDC, or archive tables.

## SQ-016 - Use Docker-Backed Testcontainers PostgreSQL Tests for Correctness

Status: Accepted.

Correctness depends on PostgreSQL locking and transaction behavior. Docker-backed Testcontainers tests are required for contract verification and must not silently skip.

## SQ-017 - Keep Direct PostgreSQL Client Internal/Trusted Only

Status: Accepted.

The direct Java client bypasses REST API-key security and requires direct database access. It is only for trusted internal deployments and should use a least-privilege database role.

## SQ-018 - Expose Admin Repair Operations for Blocked Sources and Queue Items

Status: Accepted.

Expose admin retry, skip, cancel, unblock, dead-letter inspection, blocked-source inspection, source item inspection, item lookup, and audit inspection. Admin mutations are audited transactionally.

## SQ-019 - Use Short Database Transactions

Status: Accepted.

Database transactions are used for enqueue, claim, complete, fail, heartbeat, recovery, and admin repair. Transactions must not be held while user handlers execute.

## SQ-020 - Use Schema Version Compatibility Checks for Direct Clients

Status: Accepted.

Direct Java schema compatibility is implemented using Flyway schema version lookup through `sequenced-queue-core`. The direct Java client currently requires schema version `3`.

Trusted direct Java deployments may call `getSchemaInfo()` or enable `validateSchemaOnBuild(true)` to fail fast when the schema is missing or incompatible.

## SQ-021 - Keep Production SQL in `sequenced-queue-core`

Status: Accepted.

Production SQL is centralized in `sequenced-queue-core`. The REST server and trusted direct Java client delegate to core rather than duplicating SQL or queue state transitions.

## SQ-022 - Add Operational Readiness Surface Before Product Features

Status: Accepted.

Stage 1 operational readiness added health, metrics, structured errors, admin inspection endpoints, and transactional admin audit before expanding product features.

## SQ-023 - Add Developer Experience Baseline Before Production Hardening

Status: Accepted.

Runnable Java/Python examples, developer quickstart, OpenAPI examples, and example compile/smoke coverage are required before Stage 3 work. Stage 2 is the developer experience baseline.

## SQ-024 - Distinguish Failed and Dead-Lettered Semantics

Status: Accepted.

`failed` is a terminal passable non-retryable failure state.

`dead_lettered` is a terminal blocking failure state.

Passable terminal states are:

- `succeeded`
- `cancelled`
- `skipped`
- `failed`

`dead_lettered` blocks the source until admin repair.

## SQ-025 - Simplify Product Surface, Not Queue Semantics

Status: Accepted.

Source ordering, leases, the head-item rule, dead-letter blocking, shared core, and PostgreSQL contract tests are non-negotiable.

CLI, UI, batching, `LISTEN/NOTIFY`, broker bridges, benchmark harness, queue-level database configuration, full API-key lifecycle, OAuth/OIDC, and archive tables are deferred.

## SQ-026 - Keep Retention Manual and Passable-Terminal Only

Status: Accepted.

Retention is a manual admin operation, not a scheduler. It deletes only old passable terminal items: `succeeded`, `cancelled`, `skipped`, and `failed`.

Retention never purges `pending`, `processing`, `retry_wait`, or `dead_lettered` rows because those statuses can affect source progression or operational repair.

Actual retention purge writes admin audit. Dry-run purge counts rows without deleting and does not write audit.

## SQ-027 - Keep API Keys Config-Only

Status: Accepted.

API keys are configured through application configuration only. There is a WORKER role for `/queues/**` and an ADMIN role for `/admin/**`; ADMIN can also call worker endpoints.

The queue database does not store API keys. Full key lifecycle management, hashed key storage, OAuth/OIDC, and identity-provider integration are deferred until there is a concrete deployment requirement.

## Resolved Former Undecided Items

The following earlier undecided items are resolved:

- Migration tool: Flyway is implemented.
- Terminal status passability: resolved by SQ-024.
- Admin audit: transactional admin audit is implemented.
- Queue-level policies: deferred; configuration remains global-only for now.
- Retention: manual passable-terminal purge only.
- API keys: config-only worker/admin keys.

## Deferred Decisions

These are intentionally deferred, not part of the current MVP surface:

- CLI shape and commands.
- UI/admin console.
- Batch claim and batch complete/fail.
- Source draining.
- PostgreSQL `LISTEN/NOTIFY` wake-ups.
- RabbitMQ/Kafka bridges.
- Benchmark harness and published performance envelopes.
- Queue-level database configuration.
- Full API-key lifecycle.
- OAuth/OIDC integration.
- Archive tables and automated retention/archive policy.
