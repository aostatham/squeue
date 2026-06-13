# DECISIONS.md — `sequenced-queue`

## Status

This file records architectural and implementation decisions for the `sequenced-queue` project.

`sequenced-queue` is a standalone, reusable, PostgreSQL-backed queue component that provides durable work dispatch with strict per-source ordering.

---

# Decision log

| ID     | Decision                                                                          | Status   |
| ------ | --------------------------------------------------------------------------------- | -------- |
| SQ-001 | Build a standalone reusable source-ordered queue component                        | Accepted |
| SQ-002 | Use PostgreSQL as the first persistence and queue backend                         | Accepted |
| SQ-003 | Guarantee ordering per `(queueName, sourceId)`                                    | Accepted |
| SQ-004 | Use at-least-once delivery, not exactly-once side effects                         | Accepted |
| SQ-005 | Use source-level leasing to preserve per-source ordering                          | Accepted |
| SQ-006 | Use queue item sequence numbers per source                                        | Accepted |
| SQ-007 | Use HTTP/REST as the default external client protocol                             | Accepted |
| SQ-008 | Provide Java and Python REST client libraries                                     | Accepted |
| SQ-009 | Provide a direct Java → PostgreSQL client for trusted deployments                 | Accepted |
| SQ-010 | Do not provide a direct Python → PostgreSQL client initially                      | Accepted |
| SQ-011 | REST and direct Java clients must share the same queue semantics                  | Accepted |
| SQ-012 | Dead-lettered head items block their source by default                            | Accepted |
| SQ-013 | Use worker leases and heartbeat for crash recovery                                | Accepted |
| SQ-014 | Use idempotency keys for duplicate enqueue protection                             | Accepted |
| SQ-015 | Keep the MVP focused; no broker, pub/sub, fanout or priority queues initially     | Accepted |
| SQ-016 | Use Testcontainers PostgreSQL integration tests for correctness                   | Accepted |
| SQ-017 | Keep direct PostgreSQL client internal/trusted only                               | Accepted |
| SQ-018 | Expose admin repair operations for blocked sources and failed items               | Accepted |
| SQ-019 | Use short database transactions; never hold transactions during handler execution | Accepted |
| SQ-020 | Use schema version compatibility checks for direct clients                        | Accepted |
| SQ-021 | Keep production SQL in `sequenced-queue-core`                                    | Accepted |
| SQ-022 | Add operational readiness surface before product features                         | Accepted |

---

# SQ-001 — Build a standalone reusable source-ordered queue component

## Status

Accepted.

## Context

`wf` requires reliable command dispatch between components. Commands may be produced by workflow state entry/exit behaviour and consumed by external workers. Items for the same workflow instance, source, customer, device, document, or other ordering key may need to be processed sequentially.

A general broker such as Kafka is too heavyweight for the current need. RabbitMQ is viable but adds an additional production system. A database-backed source-ordered queue is a better first fit.

## Decision

Build a standalone component named `sequenced-queue`.

It will provide:

* durable queue items;
* multiple producers;
* multiple consumers;
* per-source strict ordering;
* concurrent processing across different sources;
* worker leases;
* retry/backoff;
* dead-letter handling;
* Java and Python clients;
* REST API;
* optional direct Java/PostgreSQL client.

## Consequences

The queue component can be reused by `wf` and by other projects.

The component must be designed as a real product boundary, not as a private table inside `wf`.

---

# SQ-002 — Use PostgreSQL as the first persistence and queue backend

## Status

Accepted.

## Context

The queue requires durable storage, transactional sequence assignment, row locking, retry state, and administrative repair visibility.

PostgreSQL provides:

* transactional writes;
* row-level locking;
* `FOR UPDATE SKIP LOCKED`;
* JSONB payload storage;
* indexing;
* Testcontainers support;
* operational familiarity.

## Decision

Use PostgreSQL as the first and only production backend for the MVP.

## Consequences

The queue can avoid Kafka/RabbitMQ initially.

The queue is not a general broker replacement.

Database throughput and locking behaviour become part of the queue’s performance envelope.

---

# SQ-003 — Guarantee ordering per `(queueName, sourceId)`

## Status

Accepted.

## Context

The main correctness requirement is that items belonging to the same source must be processed in sequence and never concurrently.

Examples of source IDs:

* workflow instance ID;
* workflow instance ID plus thread ID;
* customer ID;
* device ID;
* account ID;
* document ID;
* agent session ID.

## Decision

The core invariant is:

```text
For each (queueName, sourceId), items are processed in sequenceNo order by at most one worker at a time.
```

Items with different `sourceId` values may be processed concurrently.

## Consequences

Throughput depends on the number of active sources, not merely the number of queue items.

One source with many items processes sequentially.

Many sources with items can process concurrently.

---

# SQ-004 — Use at-least-once delivery, not exactly-once side effects

## Status

Accepted.

## Context

A worker can execute a side effect and crash before recording completion. The item may then be retried. Therefore true exactly-once side effects cannot be guaranteed by the queue alone.

## Decision

The queue guarantees at-least-once delivery.

Handlers must be idempotent.

The queue will provide:

* stable item IDs;
* optional idempotency keys;
* attempt counts;
* retries;
* dead-letter state;
* worker leases.

## Consequences

Documentation must state clearly that exactly-once side effects are not guaranteed.

Consumers must use idempotency where side effects matter.

---

# SQ-005 — Use source-level leasing to preserve per-source ordering

## Status

Accepted.

## Context

If workers claim arbitrary queue item rows directly, two workers could process different items for the same source at the same time.

That would violate the central ordering guarantee.

## Decision

Workers must claim a source lease before claiming an item.

The source, not the item, is the unit of concurrency.

The queue will maintain a `queue_source_state` table with:

* `queue_name`;
* `source_id`;
* `next_sequence_no`;
* `status`;
* `leased_by`;
* `lease_id`;
* `lease_until`.

Only one worker can lease a source at a time.

## Consequences

The implementation is more complex than a simple queue table.

The ordering invariant is explicit and enforceable.

---

# SQ-006 — Use queue item sequence numbers per source

## Status

Accepted.

## Context

The system needs a deterministic ordering for items within a source.

## Decision

Each item receives a `sequence_no` assigned transactionally per `(queueName, sourceId)`.

The queue will enforce:

```text
UNIQUE(queue_name, source_id, sequence_no)
```

The source state row holds `next_sequence_no`.

## Consequences

Producers can enqueue concurrently to the same source without sequence gaps or duplicates.

Sequence assignment must occur inside a transaction while the source state row is locked.

---

# SQ-007 — Use HTTP/REST as the default external client protocol

## Status

Accepted.

## Context

The queue should support Java, Python, and future clients. External clients should not need database credentials or schema knowledge.

## Decision

The default external integration model is:

```text
client → REST API → sequenced-queue-server → PostgreSQL
```

REST clients will be provided for Java and Python.

## Consequences

The queue server owns correctness.

Client libraries remain lightweight wrappers around the API.

REST adds overhead compared with direct SQL, but it provides a clean component boundary.

---

# SQ-008 — Provide Java and Python REST client libraries

## Status

Accepted.

## Context

The project needs ergonomic client libraries for common integration languages.

## Decision

Provide:

```text
sequenced-queue-java-client
sequenced_queue Python package
```

These clients will support:

* enqueue;
* claim;
* complete;
* fail;
* heartbeat;
* worker loop;
* empty queue backoff;
* graceful shutdown.

## Consequences

The REST API must be stable and documented with OpenAPI.

The clients should not duplicate queue locking logic.

---

# SQ-009 — Provide a direct Java → PostgreSQL client for trusted deployments

## Status

Accepted.

## Context

REST is clean, but direct SQL is faster for trusted internal Java deployments. `wf` may want low-overhead Java command workers that claim queue items directly from PostgreSQL.

## Decision

Provide a direct Java/PostgreSQL client alongside the REST clients.

The direct Java client will use:

```text
Java → JDBC/DataSource → PostgreSQL
```

It will not require the queue server to be running.

## Consequences

The direct Java client must preserve the same semantics as the REST server.

The direct client is for trusted internal deployments only.

It increases compatibility and schema-versioning obligations.

---

# SQ-010 — Do not provide a direct Python → PostgreSQL client initially

## Status

Accepted.

## Context

A direct database client must implement the same source lease, head-item, retry, heartbeat, and completion semantics as the server. Duplicating this logic across multiple languages increases risk.

## Decision

Do not provide a direct Python/PostgreSQL client in the MVP.

Python clients use REST.

## Consequences

Python clients get a safer boundary.

High-performance direct database access is initially Java-only.

---

# SQ-011 — REST and direct Java clients must share the same queue semantics

## Status

Accepted.

## Context

A queue item may be enqueued through REST and claimed through the direct Java client, or vice versa.

The two access paths must not diverge.

## Decision

REST server and direct Java client must operate on the same schema, status model, source leasing rules, and head-item rules.

Cross-mode compatibility tests are mandatory.

## Consequences

Schema migrations must preserve compatibility.

Shared core Java repository logic is preferred.

If SQL is duplicated, it must be covered by shared contract tests.

---

# SQ-012 — Dead-lettered head items block their source by default

## Status

Accepted.

## Context

If item `N` fails permanently, processing item `N+1` may violate ordering and side-effect assumptions.

## Decision

A dead-lettered head item blocks the source by default.

The source becomes:

```text
blocked
```

Later items for that source are not processed until an admin action resolves the block.

Admin actions may include:

* retry;
* skip;
* cancel;
* mark resolved.

## Consequences

This is safer for workflow command processing.

Operational tooling is required to repair blocked sources.

---

# SQ-013 — Use worker leases and heartbeat for crash recovery

## Status

Accepted.

## Context

Workers may crash while processing an item. Without leases, the queue could remain stuck forever.

## Decision

Claims create a lease with:

* `lease_id`;
* `leased_by`;
* `lease_until`.

Long-running workers may heartbeat to extend the lease.

A recovery process reclaims expired leases.

## Consequences

Workers must heartbeat during long processing.

Completion and failure calls must validate lease identity.

Expired leases can lead to redelivery, reinforcing the at-least-once model.

---

# SQ-014 — Use idempotency keys for duplicate enqueue protection

## Status

Accepted.

## Context

Producers may retry enqueue calls after network failure or crash. Without idempotency, duplicate queue items may be created.

## Decision

Enqueue accepts an optional `idempotencyKey`.

If an item already exists for `(queueName, idempotencyKey)`, the existing item is returned.

## Consequences

Producers should provide stable idempotency keys when enqueueing business commands.

The database should enforce a uniqueness constraint over queue name and idempotency key.

---

# SQ-015 — Keep the MVP focused

## Status

Accepted.

## Context

There is a risk of turning the component into a full broker or job system too early.

## Decision

The MVP will not include:

* pub/sub;
* fanout;
* priorities;
* recurring jobs;
* broker bridge;
* UI;
* multi-database support;
* sharding;
* batch source draining;
* direct SQL clients in multiple languages.

## Consequences

The first implementation can focus on correctness.

Extensions can be added after the core invariant is proven.

---

# SQ-016 — Use Testcontainers PostgreSQL integration tests for correctness

## Status

Accepted.

## Context

The core behaviour depends on PostgreSQL locking, transactions, and concurrency. Unit tests alone are insufficient.

## Decision

Use Testcontainers PostgreSQL integration tests for:

* concurrent enqueue;
* sequence assignment;
* source lease exclusivity;
* claim/complete;
* retry/backoff;
* dead-letter blocking;
* lease expiry recovery;
* REST/direct compatibility.

## Consequences

The test suite may be slower but will validate real database behaviour.

Concurrency tests are mandatory, not optional.

---

# SQ-017 — Keep direct PostgreSQL client internal/trusted only

## Status

Accepted.

## Context

The direct Java client bypasses the REST server’s API-level security and permission checks.

## Decision

The direct Java/PostgreSQL client is for trusted internal services only.

It should be documented as such.

## Consequences

Direct clients need database credentials.

Deployment must restrict database privileges.

REST remains the preferred external boundary.

---

# SQ-018 — Expose admin repair operations

## Status

Accepted.

## Context

Sources can become blocked by dead-lettered head items. Operators need ways to inspect and repair them.

## Decision

Expose admin operations for:

* listing blocked sources;
* retrying items;
* skipping items;
* cancelling items;
* unblocking sources where valid.

## Consequences

The queue becomes operable.

Admin operations must be transactional and auditable eventually.

---

# SQ-019 — Use short database transactions

## Status

Accepted.

## Context

Holding database transactions during command execution would reduce concurrency and create lock contention.

## Decision

The queue must use short transactions.

Correct pattern:

```text
claim transaction:
  acquire source lease
  mark item processing
  commit

handler execution:
  no queue transaction open

complete/fail transaction:
  validate lease
  update item
  release/block source
  commit
```

## Consequences

Workers must not hold database locks while running user code.

This supports concurrency and avoids long-running transaction hazards.

---

# SQ-020 — Use schema version compatibility checks for direct clients

## Status

Proposed.

## Context

Direct Java clients depend on the database schema directly. If a client version and schema version mismatch, queue semantics could break.

## Decision

Add schema version compatibility checks for direct Java clients.

The client should fail fast if the schema is missing or incompatible.

Possible sources:

* Flyway schema history;
* dedicated `queue_schema_version` table.

## Consequences

This adds safety for direct client deployments.

The exact mechanism remains open.

---

# Open decisions

## OD-001 — Java version

Should the project target Java 21 or Java 25?

Options:

1. Java 21 LTS;
2. Java 25 if aligned with the wider `wf` ecosystem.

Recommendation:

```text
Use Java 21 unless the surrounding project standardises on Java 25.
```

---

## OD-002 — Framework for server

Should the queue server use Spring Boot?

Options:

1. Spring Boot;
2. Micronaut;
3. Quarkus;
4. lightweight Javalin/Jetty.

Recommendation:

```text
Use Spring Boot for familiarity and integration with PostgreSQL, OpenAPI, and Testcontainers.
```

---

## OD-003 — Migration tool

Should database migrations use Flyway or Liquibase?

Options:

1. Flyway;
2. Liquibase.

Recommendation:

```text
Use Flyway unless there is a project-level reason to prefer Liquibase.
```

---

## OD-004 — JSON representation in Java clients

Should Java APIs expose JSON as strings, Jackson `JsonNode`, or maps?

Options:

1. `String`;
2. `JsonNode`;
3. `Map<String,Object>`;
4. overloaded support.

Recommendation:

```text
Support String first; add JsonNode overloads if Jackson is already used.
```

---

## OD-005 — Lease identity fields

Should both `queue_source_state` and `queue_item` include `lease_id`?

Options:

1. source only;
2. item only;
3. both.

Recommendation:

```text
Use lease_id on both source and item for clear validation.
```

---

## OD-006 — Terminal status passability

Which terminal statuses allow later items for the same source to proceed?

Proposed passable statuses:

```text
succeeded
cancelled
skipped
failed
```

Proposed blocking statuses:

```text
pending
retry_wait
processing
dead_lettered
```

Open question:

```text
Should failed be passable or should failed also block the source?
```

Recommendation:

```text
Use dead_lettered to block; failed is passable only if explicitly terminal non-retryable and safe.
```

---

## OD-007 — Backoff policy

What retry backoff should the MVP use?

Options:

1. fixed delay;
2. exponential backoff;
3. client-supplied delay;
4. server-configured policy.

Recommendation:

```text
Use server-configured exponential backoff with optional client maxAttempts.
```

---

## OD-008 — Batch claims

Should MVP support claiming multiple sources or items per call?

Options:

1. no, one item per claim;
2. claim multiple sources;
3. drain multiple items for one source;
4. both.

Recommendation:

```text
MVP should claim one item per source lease. Add batching after correctness is proven.
```

---

## OD-009 — LISTEN/NOTIFY

Should PostgreSQL `LISTEN/NOTIFY` be used to wake workers?

Options:

1. not in MVP;
2. optional nudge only;
3. core mechanism.

Recommendation:

```text
Not in MVP. If added later, use only as a wake-up nudge, never as source of truth.
```

---

## OD-010 — Admin audit

Should admin repair operations be audited in MVP?

Options:

1. yes, full audit table;
2. simple audit log;
3. defer until after core queue correctness.

Recommendation:

```text
Add simple audit if easy; do not block MVP on full audit.
```

---

## OD-011 — Multi-tenancy

Should tenant ID be first-class in the queue schema?

Options:

1. no tenant field in MVP;
2. tenant in headers only;
3. tenant_id column on queue_item;
4. tenant_id on both source and item.

Recommendation:

```text
If this will be used by wf, include tenant_id as nullable column early.
```

---

## OD-012 — Direct client security model

Should direct Java clients use restricted database roles?

Options:

1. document only;
2. provide SQL role scripts;
3. implement row-level security.

Recommendation:

```text
Provide restricted DB role scripts later; do not implement RLS in MVP.
```

---

## OD-013 — Result storage

Should result JSON be retained forever?

Options:

1. retain in `queue_item`;
2. move completed items to history table;
3. configurable retention;
4. external archive.

Recommendation:

```text
Keep result_json initially, then add archive/retention after MVP.
```

---

## OD-014 — Completed item archiving

When should succeeded items be archived or purged?

Options:

1. never in MVP;
2. scheduled archival by age;
3. partition by month;
4. move to history table.

Recommendation:

```text
Do not solve in MVP, but design indexes so table growth can be managed later.
```

---

## OD-015 — Queue name creation

Are queues implicit or explicitly created?

Options:

1. implicit on first enqueue;
2. explicit queue registry;
3. configuration-only.

Recommendation:

```text
Implicit initially; explicit queue registry later if queue-level policy is needed.
```

---

## OD-016 — Queue-level policies

Should queues have configurable policies?

Possible policies:

* default lease seconds;
* max lease seconds;
* default max attempts;
* backoff policy;
* dead-letter behaviour;
* max payload size.

Recommendation:

```text
Global defaults in MVP. Queue-level policies later.
```

---

# Initial implementation sequence

## Phase 1 — Server and schema

* Spring Boot server skeleton;
* PostgreSQL docker-compose;
* Flyway migrations;
* `queue_source_state`;
* `queue_item`;
* enqueue endpoint;
* Testcontainers test for sequence assignment.

## Phase 2 — Claim and complete

* source lease claim;
* head item claim;
* complete endpoint;
* source release;
* concurrency tests.

## Phase 3 — Failure, retry, dead-letter

* fail endpoint;
* retry/backoff;
* dead-letter;
* source blocked;
* admin retry/skip/cancel.

## Phase 4 — Lease heartbeat and recovery

* heartbeat endpoint;
* expired lease recovery;
* lease validation tests.

## Phase 5 — REST Java and Python clients

* producer clients;
* worker loop;
* complete/fail;
* heartbeat;
* examples.

## Phase 6 — Direct Java PostgreSQL client

* DataSource-based client;
* enqueue;
* claim;
* complete;
* fail;
* heartbeat;
* recovery;
* compatibility tests with REST path.

---

# Core invariant to protect

Every implementation and test must protect this invariant:

```text
For each (queueName, sourceId):
  sequenceNo is assigned strictly in enqueue order;
  at most one worker may hold the source lease;
  only the head processable item may be claimed;
  later items must never overtake earlier blocking items.
```

If a proposed optimisation weakens that invariant, reject it.

---

# SQ-020 — Use schema version compatibility checks for direct clients

## Status

Accepted.

## Decision

The direct Java/PostgreSQL client must be able to fail fast when the database schema is missing or incompatible. The client reads the Flyway schema version exposed by `sequenced-queue-core` and supports opt-in validation with `validateSchemaOnBuild(true)`.

The current schema version is `2`.

---

# SQ-021 — Keep production SQL in `sequenced-queue-core`

## Status

Accepted.

## Decision

Production queue SQL belongs in `sequenced-queue-core`. The REST server, Java REST client, Python REST client, and direct Java client must not duplicate queue state-transition SQL or bypass the shared core implementation.

Server code may expose HTTP, security, metrics, and health adapters, but durable queue mutations and inspection queries remain core-owned.

---

# SQ-022 — Add operational readiness surface before product features

## Status

Accepted.

## Decision

After the PostgreSQL correctness checkpoint, the next priority is observability and repairability rather than batching, retention, CLI, UI, or performance features.

Stage 1 adds:

* Micrometer/Actuator metrics;
* queue-specific health details;
* transactional admin audit;
* admin inspection endpoints;
* structured REST error responses;
* documentation of guarantees and operating model.
