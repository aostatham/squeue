# Simplification Strategy

The Stage 3A direction is to simplify the product surface without weakening the queue engine.

Principle:

```text
Simplify around the queue engine, not through the queue engine.
```

## Non-Negotiable Core Semantics

These are part of the queue engine and are not simplification targets:

- `queue_source_state`
- `queue_item`
- per-source sequence numbers
- source lease
- item lease
- `lease_id` validation
- head-item rule
- dead-letter blocking
- durable at-least-once delivery
- shared `sequenced-queue-core` implementation
- REST/direct compatibility
- production SQL centralized in `sequenced-queue-core`
- Docker-backed PostgreSQL contract tests with no skips

## Simplifiable Product Surface

These areas should remain small until usage proves more is needed:

- API key management
- queue-level configuration
- retention/archive
- admin endpoint expansion
- metrics naming
- worker wrapper feature set
- CLI
- UI
- batching
- `LISTEN/NOTIFY`
- broker bridges

## Deferred Features

The following are explicitly deferred:

- CLI
- UI
- batch claim
- batch complete/fail
- source draining
- PostgreSQL `LISTEN/NOTIFY`
- RabbitMQ/Kafka bridge
- benchmark harness
- queue-level database configuration
- full API-key lifecycle
- OAuth/OIDC
- archive tables
- automated retention/archive policy

Reconsider a deferred feature only when there is a concrete operational or product need that cannot be met by the current REST API, direct Java client, admin endpoints, metrics, health, and documentation.

## Stage 3A Focus

Stage 3A Minimal Production Hardening should keep the system deployable and understandable:

- keep API keys config-only for now
- keep queue configuration global-only for now
- document canonical semantics
- clarify `failed` vs `dead_lettered`
- keep admin audit focused on admin mutations
- keep the direct Java client thin over `sequenced-queue-core`
- add manual retention purge only; no scheduler or archive table
- avoid expanding product surface into deferred areas

## S1 Simplification Sprint Baseline

The Java REST and trusted direct Java worker helpers share `sequenced-queue-worker-core` for polling, heartbeat scheduling, no-handler failure, handler exception mapping, lease-lost suppression, graceful shutdown, and empty-queue backoff.

The shared worker module is deliberately transport-neutral. It contains no queue SQL, no PostgreSQL access, no REST client code, and no source lease or head-item ordering rules.

Public DTO names and example module boundaries remain stable for the release candidate. Renaming `ClaimItem` to `ClaimedItem` or collapsing runnable example modules is deferred until there is a compatibility window and a clearer packaging policy.
