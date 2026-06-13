# `sequenced-queue` Product Roadmap

## Product position

`sequenced-queue` should be positioned as:

> A lightweight PostgreSQL-backed durable queue that guarantees sequential processing per source while allowing parallel processing across sources, with REST, Java, Python, and direct Java/PostgreSQL access paths.

Its niche is **not** “better Kafka” or “better RabbitMQ”. Its niche is:

```text
durable source-ordered work dispatch
```

for systems where the ordering key matters more than raw broker throughput.

Best-fit use cases:

```text
workflow instance commands
per-customer integration updates
per-account mutations
per-device command dispatch
document/session/agent task queues
human/agent workflow side effects
```

---

# Roadmap overview

The roadmap has six stages:

```text
0. Correctness foundation
1. Operational readiness
2. Developer experience
3. Production hardening
4. Scaling and performance
5. Productisation and ecosystem
6. Advanced workflow/agent integration
```

The project has substantially passed **Stage 0**.

---

# Stage 0 — Correctness Foundation

## Status

```text
Mostly complete / checkpoint passed
```

## Objective

Prove the core invariant:

```text
For each (queueName, sourceId), queue items are processed in sequence order and never concurrently.
```

## Required capabilities

Already achieved or substantially achieved:

- PostgreSQL schema
- Source state table
- Queue item table
- Per-source sequence numbers
- Source leases
- Item leases
- Enqueue
- Claim
- Complete
- Fail
- Heartbeat
- Admin repair
- REST API
- Java REST client
- Python REST client
- Direct Java/PostgreSQL client
- Shared core implementation
- API key security
- REST/direct compatibility
- Docker-backed PostgreSQL contract tests

## Exit criteria

The project has effectively met these:

```text
PostgresQueueContractTest passes
SequencedQueueDirectClientTest passes
QueueApiIntegrationTest passes
ApiKeySecurityTest passes
RestDirectCompatibilityTest passes
No Testcontainers skips
Overall reactor BUILD SUCCESS
```

## Remaining Stage 0 cleanup

Before moving too far ahead:

- Update README to reflect current module structure.
- Ensure `DECISIONS.md` reflects the shared-core decision.
- Record the PostgreSQL correctness checkpoint.
- Make sure all previously duplicated SQL is removed from production paths.
- Add Maven wrapper if still missing.
- Keep Testcontainers non-optional for contract profiles.

---

# Stage 1 — Operational Readiness

## Objective

Make the queue observable, diagnosable, and repairable.

This is the next most important phase. The core semantics are now credible; the next risk is that an operator cannot see or fix problems.

## Features

### 1. Metrics

Expose metrics for:

```text
queue.items.pending
queue.items.processing
queue.items.retry_wait
queue.items.dead_lettered
queue.sources.idle
queue.sources.leased
queue.sources.blocked
queue.claims.total
queue.claims.empty
queue.completions.total
queue.failures.total
queue.heartbeats.total
queue.heartbeats.failed
queue.lease_expiries.total
queue.admin.retry.total
queue.admin.skip.total
queue.admin.cancel.total
```

Use Micrometer in the server.

Direct Java client can expose local worker metrics if useful.

### 2. Health checks

Add health endpoints for:

```text
database reachable
Flyway schema current
queue tables present
recovery process enabled
```

### 3. Admin audit log

Add an audit table:

```sql
queue_admin_audit
```

Record:

```text
audit_id
timestamp
actor/api_key_id
operation
queue_name
source_id
item_id
previous_status
new_status
reason
metadata_json
```

Audit these operations:

```text
retry
skip
cancel
unblock
force release, if ever added
```

### 4. Dead-letter inspection

Add endpoints:

```http
GET /admin/queues/{queueName}/dead-lettered
GET /admin/queues/{queueName}/blocked-sources
GET /admin/queues/{queueName}/sources/{sourceId}/items
```

### 5. Better error model

Standardise error responses:

```json
{
  "errorCode": "LEASE_LOST",
  "message": "Lease is no longer valid",
  "queueName": "wf.commands",
  "itemId": "...",
  "sourceId": "..."
}
```

### 6. Documentation update

Document:

- delivery guarantees;
- non-guarantees;
- source ordering;
- lease model;
- retry/dead-letter semantics;
- REST vs direct Java mode;
- security model;
- local Docker startup;
- running contract tests.

## Exit criteria

Stage 1 is complete when:

```text
operators can see queue health
operators can inspect blocked/dead-lettered work
admin actions are audited
errors are predictable
README accurately describes real behaviour
```

---

# Stage 2 — Developer Experience

## Objective

Make the product pleasant and safe for developers to use.

## Features

### 1. Stable public API model

Define the canonical public API objects:

```text
EnqueueRequest
EnqueueResponse
ClaimRequest
ClaimResponse
ClaimedItem
CompleteRequest
FailRequest
HeartbeatRequest
ItemResponse
QueueError
```

Ensure REST, Java REST client, Python client, and direct Java client use consistent naming.

### 2. Client examples

Add examples:

```text
examples/java-producer
examples/java-rest-worker
examples/java-direct-worker
examples/python-producer
examples/python-worker
```

Each example should be runnable.

### 3. Worker framework improvements

Worker libraries should support:

```text
handler registration by itemType
automatic heartbeat
lease lost detection
retryable exception type
non-retryable exception type
graceful shutdown
empty queue backoff
structured logging
```

### 4. CLI tool

Add a small CLI:

```bash
sq enqueue ...
sq inspect queue ...
sq inspect source ...
sq list blocked ...
sq retry item ...
sq skip item ...
```

This is valuable for testing and admin work.

### 5. OpenAPI quality

Ensure the OpenAPI document is clean enough to generate clients later.

Add examples for every endpoint.

### 6. Schema compatibility check

Direct Java client should fail fast if schema is incompatible.

Example:

```java
client.validateSchemaOnBuild(true)
```

This checks Flyway version or a dedicated schema version.

## Exit criteria

Stage 2 is complete when a new developer can:

```text
start Postgres
start server
enqueue an item
run a Java worker
run a Python worker
inspect blocked work
understand failure semantics
```

without reading source code.

---

# Stage 3 — Production Hardening

## Objective

Make the queue suitable for real internal production use.

## Features

### 1. API key management

Current API key filtering is a good baseline. Next:

```text
multiple keys
admin keys vs worker keys
key names/identifiers
key rotation
disabled keys
hashed key storage
```

Avoid storing raw API keys.

### 2. Queue-level configuration

Add a `queue_config` table or config file support:

```text
queue_name
default_lease_seconds
max_lease_seconds
default_max_attempts
max_payload_bytes
dead_letter_policy
retry_backoff_policy
retention_policy
```

### 3. Payload size limits

Enforce:

```text
max payload size
max header size
max error message size
```

This prevents accidental abuse.

### 4. Retention and archival

Completed items will grow indefinitely.

Add policies for:

```text
succeeded item retention
cancelled item retention
skipped item retention
dead-letter retention
archive table or deletion
```

Initial simple policy:

```text
delete/archive succeeded items older than N days
retain dead-lettered items until manually resolved or older than M days
```

### 5. Structured logging

Log key lifecycle events:

```text
enqueue
claim
complete
fail
heartbeat failed
lease expired
admin action
```

Do not log full payloads by default.

### 6. Versioned clients

Document compatibility:

```text
server version
schema version
java-rest client version
java-direct client version
python-rest client version
```

### 7. Migration discipline

Define:

```text
no destructive migrations without compatibility window
direct client must validate schema
server starts only with compatible schema
```

## Exit criteria

Stage 3 is complete when the queue can be deployed internally with confidence and monitored/administered without manual SQL.

---

# Stage 4 — Scaling and Performance

## Objective

Understand and improve real throughput under different source distributions.

## Key performance reality

Throughput is limited by active sources:

```text
1 hot source with 100,000 items
  → sequential throughput only

10,000 sources with 1 item each
  → highly parallel
```

This is correct and should be documented.

## Features

### 1. Benchmark harness

Add repeatable benchmarks:

```text
single hot source
many active sources
mixed hot/cold sources
tiny payloads
larger payloads
REST workers
direct Java workers
different worker counts
```

Metrics:

```text
enqueue throughput
claim throughput
complete throughput
end-to-end latency
lock contention
dead-letter/retry overhead
database CPU
database IO
```

### 2. Batch claim

Add optional claim batching:

```text
claim up to N sources
one item per source
```

Do **not** initially drain multiple items per source unless carefully tested.

### 3. Batch complete/fail

Add:

```http
POST /queues/{queueName}/completions
POST /queues/{queueName}/failures
```

Useful for tiny jobs.

### 4. Source draining mode

Optional later:

```text
claim source lease
process up to M sequential items for that source
release source
```

This improves throughput for sources with many small items, but increases lease duration and fairness concerns.

### 5. Partitioning/archive strategy

Consider partitioning `queue_item` by:

```text
created_at
queue_name
```

only once table growth justifies it.

### 6. LISTEN/NOTIFY as wake-up only

Optional:

```text
PostgreSQL LISTEN/NOTIFY to wake idle workers
```

But never use it as source of truth.

Polling/claim remains authoritative.

## Exit criteria

Stage 4 is complete when you can state credible performance envelopes:

```text
direct Java mode: X items/sec under Y conditions
REST mode: X items/sec under Y conditions
single hot source latency: X
many-source throughput: X
```

---

# Stage 5 — Productisation and Ecosystem

## Objective

Turn the component from working code into a reusable product.

## Features

### 1. Packaging

Publish:

```text
Docker image for server
Maven artifacts
Python package
versioned docs
example docker-compose
```

### 2. Admin UI

A minimal UI would be genuinely useful.

Screens:

```text
Queues overview
Blocked sources
Dead-lettered items
Source timeline
Item detail
Admin retry/skip/cancel
Worker activity
Metrics dashboard link
```

This does not need to be elaborate.

### 3. Documentation site

Docs should include:

```text
Getting started
Architecture
REST API
Java REST client
Java direct client
Python client
Operational guide
Failure semantics
Security guide
Performance guide
Migration guide
```

### 4. Compatibility matrix

Example:

```text
Server 0.3.x supports schema 3
Java REST client 0.3.x supports server 0.3+
Java direct client 0.3.x requires schema 3
Python client 0.3.x supports server 0.3+
```

### 5. Examples for real use cases

Add sample integrations:

```text
email sending queue
workflow command queue
per-customer update queue
agent session queue
```

## Exit criteria

Stage 5 is complete when someone outside the original project can install and use it from docs alone.

---

# Stage 6 — Advanced Workflow / Agent Integration

## Objective

Make `sequenced-queue` especially useful for workflow engines and agentic systems.

This should come after the queue itself is solid.

## Features

### 1. Workflow command profile

Define a standard item type:

```text
wf.command
```

Payload shape:

```json
{
  "workflowInstanceId": "...",
  "threadId": "...",
  "stateId": "...",
  "commandName": "...",
  "commandPayload": {}
}
```

### 2. Agent session queue profile

Define a standard pattern:

```text
sourceId = agentSessionId
```

This guarantees:

```text
one agent session processes one step at a time
different sessions process concurrently
```

### 3. Human task integration

Use queue items for:

```text
notification dispatch
escalation commands
external task completion events
```

Not as the task manager itself, but as a durable command substrate.

### 4. Outbox bridge

Add an optional outbox bridge for systems that need broker delivery later:

```text
sequenced-queue → RabbitMQ
sequenced-queue → Kafka
sequenced-queue → webhook
```

The queue remains source of truth.

### 5. Result event integration

For workflow systems:

```text
queue item complete → callback/event to workflow engine
```

Careful: this starts to blur queue and workflow semantics. Keep it optional.

## Exit criteria

Stage 6 is complete when `sequenced-queue` has first-class patterns for workflow and agent systems without becoming a workflow engine itself.

---

# Suggested version roadmap

## v0.1 — Correctness MVP

Status: near/current.

Includes:

```text
PostgreSQL schema
enqueue/claim/complete/fail/heartbeat
source leases
dead-letter
admin repair
REST API
Java REST client
Python REST client
direct Java client
shared core
PostgreSQL contract tests
```

Goal:

```text
prove the core invariant
```

## v0.2 — Operational baseline

Includes:

```text
metrics
health checks
admin audit
better error model
README/docs update
schema compatibility check
CLI basic inspect commands
```

Goal:

```text
make it operable
```

## v0.3 — Developer usability

Includes:

```text
polished Java/Python clients
worker examples
CLI enqueue/claim/admin commands
OpenAPI examples
direct Java schema validation
packaged examples
```

Goal:

```text
make it easy to adopt
```

## v0.4 — Production hardening

Includes:

```text
API key management
queue-level config
payload limits
retention/archive policy
structured logging
migration/version policy
```

Goal:

```text
make it safe for internal production
```

## v0.5 — Performance

Includes:

```text
benchmark harness
batch claim
batch complete/fail
optional LISTEN/NOTIFY wake-up
performance docs
```

Goal:

```text
define credible performance envelope
```

## v1.0 — Stable release

Includes:

```text
stable REST API
stable Java REST client
stable Python REST client
stable direct Java client
stable schema migration policy
admin/ops documentation
security baseline
performance benchmarks
Docker image
published artifacts
```

Goal:

```text
safe to use as a reusable internal/external component
```

---

# Priority order

Do not build advanced features yet.

The next practical priority order should be:

```text
1. Document current correctness checkpoint.
2. Add metrics.
3. Add admin audit.
4. Add schema compatibility check.
5. Update README and operational docs.
6. Add basic CLI.
7. Add load/benchmark harness.
8. Add retention/archive policy.
9. Improve API key management.
10. Add batching.
```

---

# Product risks

## Risk 1 — Becoming a broker clone

Avoid adding pub/sub, topics, fanout, and consumer groups too early.

The product wins by staying focused:

```text
source-ordered durable work dispatch
```

## Risk 2 — Hidden semantic drift

REST server, direct Java client, and future features must continue using the shared core.

Any duplicated SQL/state transition logic is a regression.

## Risk 3 — Insufficient operational visibility

A queue without observability becomes a black box.

Metrics and admin audit are not optional for serious use.

## Risk 4 — Table growth

Completed items will accumulate.

Retention/archiving must be designed before real usage grows.

## Risk 5 — Misleading delivery guarantees

Documentation must continue saying:

```text
at-least-once delivery
not exactly-once side effects
handlers must be idempotent
```

Do not soften this wording.

---

# Final recommendation

Move forward, but keep the roadmap disciplined.

The product should mature in this order:

```text
Correctness → Operability → Developer Experience → Production Hardening → Performance → Ecosystem
```

Do not jump to performance features or integrations before observability, audit, schema compatibility, and retention are addressed.

The immediate next checkpoint should be:

```text
Operational Readiness Checkpoint
```

with metrics, admin audit, schema compatibility, and documentation as the core deliverables.
