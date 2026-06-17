# sequenced-queue Product Roadmap

## Product Position

`sequenced-queue` is a lightweight PostgreSQL-backed durable queue that guarantees sequential processing per source while allowing parallel processing across sources, with REST, Java, Python, and trusted direct Java/PostgreSQL access paths.

Its niche is:

```text
durable source-ordered work dispatch
```

It is not a replacement for Kafka, RabbitMQ, pub/sub, stream processing, fanout, or broker consumer groups.

## Current Status

```text
Stage 0 - Correctness Foundation: Passed
Stage 1 - Operational Readiness Baseline: Passed
Stage 2 - Developer Experience Baseline: Passed
Current focus - Simplification pass / Stage 3A Minimal Production Hardening
```

Docker-backed PostgreSQL/Testcontainers suites have run with no skips.

## Core Invariant

```text
For each (queueName, sourceId), queue items are processed in sequence order and are not processed concurrently.
```

Delivery semantics:

- durable at-least-once delivery
- strict per-source ordering
- no exactly-once side effects
- no global ordering across sources
- idempotent handlers required

Canonical detail lives in [semantics.md](semantics.md).

## Roadmap Overview

The current roadmap is:

```text
0. Correctness Foundation - passed
1. Operational Readiness - passed
2. Developer Experience - passed
3A. Minimal Production Hardening / Simplification Pass - current
3B. Broader Production Hardening - later
4. Scaling and Performance - later
5. Productisation and Ecosystem - later
6. Advanced Workflow/Agent Integration - later
```

## Stage 0 - Correctness Foundation

Status: Passed.

Stage 0 proved the queue engine:

- PostgreSQL schema
- `queue_source_state`
- `queue_item`
- per-source sequence numbers
- source leases
- item leases
- enqueue
- claim
- complete
- fail
- heartbeat
- lease recovery
- admin repair
- REST API
- Java REST client
- Python REST client
- trusted direct Java/PostgreSQL client
- shared core implementation
- API key filter
- REST/direct compatibility
- Docker-backed PostgreSQL contract tests with no skips

Key tests:

- `PostgresQueueContractTest`
- `SequencedQueueDirectClientTest`
- `QueueApiIntegrationTest`
- `ApiKeySecurityTest`
- `RestDirectCompatibilityTest`

## Stage 1 - Operational Readiness Baseline

Status: Passed.

Stage 1 made the queue observable, diagnosable, and repairable:

- Micrometer metrics
- Actuator health checks
- schema health details
- structured REST error responses with queue/source/item context
- admin inspection endpoints
- transactional admin audit
- admin retry/skip/cancel/unblock metrics
- OpenAPI/admin retry documentation correction
- Maven wrapper

Implemented admin inspection endpoints:

```text
GET /admin/queues/{queueName}/dead-lettered
GET /admin/queues/{queueName}/blocked-sources
GET /admin/queues/{queueName}/sources/{sourceId}/items
GET /admin/queues/{queueName}/items/{itemId}
GET /admin/queues/{queueName}/audit
```

## Stage 2 - Developer Experience Baseline

Status: Passed.

Stage 2 made the project runnable and easier to understand:

- developer quickstart
- runnable Java REST producer example
- runnable Java REST worker example
- runnable Java direct PostgreSQL worker example
- runnable Python REST producer example
- runnable Python REST worker example
- OpenAPI examples for queue and admin endpoints
- Java example Maven reactor coverage
- Python example compile tests
- example smoke tests for producer/worker paths
- deterministic worker run-once support
- safe direct Java worker helper
- route/OpenAPI drift check

Developer examples are in [../examples/README.md](../examples/README.md).

CLI is not part of Stage 2. It is deferred.

## Stage 3A - Minimal Production Hardening / Simplification Pass

Status: Current focus.

Stage 3A narrows the next work to minimum production hardening while avoiding premature product surface expansion.

Required artifacts:

- [simplification_strategy.md](simplification_strategy.md)
- [semantics.md](semantics.md)

Stage 3A scope:

- documentation alignment and simplification strategy
- canonical semantics document
- clarified `failed` vs `dead_lettered` semantics
- config-only API key model
- global-only queue configuration
- admin audit focused on admin mutations
- direct Java client remains thin over `sequenced-queue-core`
- structured logging and migration discipline
- manual retention purge only; no scheduler or archive table
- global request limits and safe structured logging

Explicitly deferred from Stage 3A:

- CLI
- UI
- batching
- source draining
- REST/WebSocket/SSE worker wake-up
- RabbitMQ/Kafka bridge
- benchmark harness
- queue-level database configuration
- full API-key lifecycle
- OAuth/OIDC
- archive tables
- automated retention/archive policy

## Stage 3B - Broader Production Hardening

Status: Later.

Potential later hardening:

- richer deployment guidance
- least-privilege database role guidance
- operational runbooks
- structured logging conventions
- migration compatibility policy
- additional admin inspection filters

These should not weaken source ordering, lease identity validation, dead-letter blocking, or the shared core implementation.

## Stage 4 - Scaling and Performance

Status: Later.

Potential later work:

- performance benchmarking
- published throughput/latency envelopes
- index tuning
- optional batching
- REST/WebSocket/SSE worker wake-up design
- partitioning/archive strategy

Batch claim and batch complete/fail are not implemented now.

## Post-MVP REST Worker Wake-Up

Direct Java workers can use PostgreSQL `LISTEN/NOTIFY` as an optional wake-up strategy because they have database access.

Pure REST clients cannot consume PostgreSQL notifications directly without also having database access. A future REST/full-distribution enhancement should provide an HTTP-native wake-up mechanism for REST workers.

Candidate approaches:

- long-poll claim endpoint
- Server-Sent Events
- WebSocket worker notification channel

Design requirements:

- queue tables remain the durable source of truth
- notifications are wake-up hints only
- clients must still claim through the normal REST claim endpoint
- missed notifications must be tolerated with fallback polling/safety sweeps
- authorization must use the existing worker/admin security model or its successor
- no exactly-once side-effect guarantee is implied

This is post-MVP/full-distribution work and is outside the core/direct Java MVP package boundary.

## Stage 5 - Productisation and Ecosystem

Status: Later.

Potential later work:

- packaging and versioning policy
- language client polish
- generated docs
- compatibility matrix
- examples for common deployment models

CLI and UI remain deferred until there is concrete need.

## Stage 6 - Advanced Workflow/Agent Integration

Status: Later.

Potential later work:

- workflow command patterns
- agent task queue patterns
- callback/event examples
- integration guidance for workflow engines

The queue should remain a queue, not become a workflow engine.

## Current Priority Order

1. Documentation alignment / simplification strategy.
2. Canonical semantics document.
3. Clarify `failed` / `dead_lettered`.
4. Minimal production hardening.
5. Manual retention purge, if not already implemented.
6. Structured logging / migration discipline.
7. Performance benchmarking later.
8. Batching later.
