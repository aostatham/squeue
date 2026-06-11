# Codex Implementation Brief — Source-Ordered Durable Queue Component

## 1. Project name

Working name:

```text
sequenced-queue
```

Alternative names:

```text
source-ordered-queue
durable-source-queue
ordered-work-queue
```

The implementation should use the neutral name `sequenced-queue` unless changed explicitly.

---

## 2. Objective

Build a standalone, reusable, database-backed queue component.

The queue must support:

* multiple producers;
* multiple consumers/workers;
* Java client library;
* Python client library;
* HTTP API;
* PostgreSQL persistence;
* durable queue items;
* per-source strict ordering;
* concurrent processing across different sources;
* at-least-once delivery;
* worker leases;
* retry/backoff;
* dead-letter handling;
* admin repair operations.

The central invariant is:

```text
For a given (queueName, sourceId), queue items must be processed in sequence order and must not be processed concurrently.
```

Items from different `sourceId` values may be processed concurrently.

---

## 3. Background and purpose

This component is intended to sit between systems that need simple guaranteed delivery but do not want Kafka, RabbitMQ, or a full messaging broker.

Primary use case:

```text
wf workflow engine emits command work items.
External workers process commands.
Commands for the same workflow instance or source must execute in order.
Commands for different sources may execute in parallel.
```

But the component must be reusable outside `wf`.

Other possible use cases:

* per-customer command ordering;
* per-document processing;
* per-device command dispatch;
* per-account external integration updates;
* agent/session task ordering;
* workflow instance command dispatch.

---

## 4. Non-goals

Do not build:

* Kafka replacement;
* RabbitMQ replacement;
* general pub/sub;
* fanout topics;
* global ordering;
* exactly-once side-effect execution;
* UI in the first version;
* distributed sharding in the first version;
* multi-database support in the first version;
* priority queues in the first version;
* recurring jobs in the first version.

The MVP should stay focused on:

```text
durable point-to-point work queue with strict per-source ordering
```

---

## 5. Delivery model

The queue provides:

```text
at-least-once delivery
per-source sequential processing
idempotent enqueue support
worker lease recovery
retry/backoff
dead-letter retention
```

The queue does not guarantee:

```text
exactly-once side effects
global item ordering across sources
zero duplicate delivery after worker crash
```

Consumers must write idempotent handlers.

---

## 6. Architecture

Use a database-backed server component.

Recommended stack:

```text
Java 25 or Java 21
Spring Boot
PostgreSQL
Flyway or Liquibase migrations
OpenAPI
JUnit 5
Testcontainers for PostgreSQL integration tests
```

Deliverables:

```text
sequenced-queue-server
sequenced-queue-java-client
sequenced-queue-python-client
```

The server exposes HTTP APIs. Java and Python libraries wrap those APIs.

The first implementation should not require workers to connect directly to the database. Workers should use the HTTP API. Internal server code uses PostgreSQL row locking.

---

## 7. Core domain concepts

## 7.1 Queue

A named logical queue.

Example:

```text
wf.commands
email.notifications
customer.integrations
```

## 7.2 Source

A source is the ordering key.

All items for the same source must be processed in sequence.

Examples:

```text
workflowInstanceId
workflowInstanceId:threadId
customerId
deviceId
documentId
accountId
```

The queue server does not interpret source IDs. It only enforces ordering per source.

## 7.3 Queue item

A durable item of work.

Each item has:

```text
itemId
queueName
sourceId
sequenceNo
itemType
payload
headers
status
availableAt
attemptCount
maxAttempts
lease information
result/error information
```

## 7.4 Source state

A durable row tracking source-level sequencing and leasing.

Each source has:

```text
queueName
sourceId
nextSequenceNo
status
lease details
```

The source state is the concurrency gate.

Only one worker may lease a source at a time.

---

## 8. Core invariant

The implementation must preserve this invariant:

```text
For each (queueName, sourceId):
  - sequenceNo is strictly increasing.
  - one worker at most may hold the source lease.
  - only the lowest available non-terminal sequence item may be processed.
  - later items must not overtake earlier pending/retry/processing items.
```

Different sources may be processed in parallel.

---

## 9. PostgreSQL schema

Create schema with at least the following tables.

## 9.1 `queue_source_state`

```sql
CREATE TABLE queue_source_state (
    queue_name TEXT NOT NULL,
    source_id TEXT NOT NULL,

    next_sequence_no BIGINT NOT NULL DEFAULT 1,

    status TEXT NOT NULL DEFAULT 'idle',
    -- idle | leased | blocked

    leased_by TEXT NULL,
    lease_until TIMESTAMPTZ NULL,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (queue_name, source_id)
);
```

## 9.2 `queue_item`

```sql
CREATE TABLE queue_item (
    item_id UUID PRIMARY KEY,

    queue_name TEXT NOT NULL,
    source_id TEXT NOT NULL,

    sequence_no BIGINT NOT NULL,

    item_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    headers_json JSONB NOT NULL DEFAULT '{}',

    status TEXT NOT NULL,
    -- pending | processing | succeeded | retry_wait | failed | dead_lettered | cancelled | skipped

    available_at TIMESTAMPTZ NOT NULL,

    claimed_by TEXT NULL,
    claimed_at TIMESTAMPTZ NULL,
    lease_until TIMESTAMPTZ NULL,

    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,

    idempotency_key TEXT NULL,

    last_error_type TEXT NULL,
    last_error_message TEXT NULL,
    result_json JSONB NULL,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    UNIQUE (queue_name, source_id, sequence_no),
    UNIQUE (queue_name, idempotency_key)
);
```

Note: `UNIQUE (queue_name, idempotency_key)` must handle nullable keys carefully. PostgreSQL allows multiple nulls. This is acceptable if idempotency is optional. If idempotency is required, enforce not null.

## 9.3 Indexes

Create indexes suitable for claim and query paths.

```sql
CREATE INDEX idx_queue_item_source_sequence
ON queue_item (queue_name, source_id, sequence_no);

CREATE INDEX idx_queue_item_claimable
ON queue_item (queue_name, status, available_at, created_at);

CREATE INDEX idx_queue_item_source_status
ON queue_item (queue_name, source_id, status, sequence_no);

CREATE INDEX idx_queue_item_dead_letter
ON queue_item (queue_name, status, updated_at)
WHERE status = 'dead_lettered';

CREATE INDEX idx_source_state_claim
ON queue_source_state (queue_name, status, lease_until, updated_at);
```

---

## 10. Status model

## 10.1 Queue item statuses

Use:

```text
pending
processing
succeeded
retry_wait
failed
dead_lettered
cancelled
skipped
```

Meaning:

```text
pending        waiting to be processed
processing     currently leased by a worker
succeeded      completed successfully
retry_wait     failed, retry scheduled
failed         terminal non-retryable failure
dead_lettered  terminal after max retries or poison message
cancelled      cancelled by admin/system
skipped        manually skipped to unblock source
```

## 10.2 Source statuses

Use:

```text
idle
leased
blocked
```

Meaning:

```text
idle       available for leasing
leased     currently leased by a worker
blocked    blocked by dead-lettered head item until admin action
```

---

## 11. Enqueue behaviour

The server must assign sequence numbers per `(queueName, sourceId)`.

Enqueue must be transactional.

Pseudo-flow:

```text
begin transaction
  create source_state row if missing
  lock source_state row
  assign sequenceNo = nextSequenceNo
  increment nextSequenceNo
  insert queue_item
commit
```

If `idempotencyKey` is supplied and an item already exists for `(queueName, idempotencyKey)`, return the existing item instead of creating a duplicate.

Response should include:

```text
itemId
queueName
sourceId
sequenceNo
status
```

---

## 12. Claim behaviour

Workers claim by queue, supported item types, and worker ID.

The worker should claim a source lease first, then the head item for that source.

Important:

```text
Do not claim arbitrary items directly.
The source is the unit of concurrency.
```

Claim flow:

```text
1. Find source with available head item.
2. Lock source_state using FOR UPDATE SKIP LOCKED.
3. Mark source_state leased by worker.
4. Claim the lowest sequence available item for that source.
5. Return lease and item to worker.
```

Claim must only return one item per source in MVP.

Batch-draining may be added later.

---

## 13. Strict per-source ordering

For strict mode:

```text
If sequenceNo 5 is pending/retry_wait/processing/dead_lettered,
sequenceNo 6 must not be processed.
```

Later items only become processable when earlier items are terminal and allowed to be passed:

```text
succeeded
cancelled
skipped
```

Decision for MVP:

```text
dead_lettered blocks the source.
```

The source becomes `blocked` when the head item is `dead_lettered`.

Admin must unblock it by retrying, cancelling, skipping, or marking resolved.

---

## 14. Completion behaviour

Workers complete by `itemId`, `workerId`, and `leaseId` or equivalent lease token.

On success:

```text
item.status = succeeded
item.result_json = supplied result
clear item lease
release source lease
```

The server must verify that:

```text
item is processing
item is claimed by this worker
lease has not expired
source is leased by this worker
```

If verification fails, return an error.

---

## 15. Failure behaviour

Workers may report failure.

Request includes:

```text
retryable
errorType
errorMessage
optional backoff
```

If retryable and attempts remain:

```text
item.status = retry_wait
availableAt = now + backoff
clear item lease
release source lease
```

If non-retryable:

```text
item.status = failed
clear item lease
release source lease
```

If attempts exhausted:

```text
item.status = dead_lettered
source.status = blocked
clear item lease
release source lease
```

---

## 16. Lease and heartbeat behaviour

Workers must hold leases.

Use:

```text
leaseId
workerId
leaseUntil
```

Workers may heartbeat to extend lease.

If a worker dies, lease expires.

A recovery job must scan expired leases:

```text
item.status = processing AND leaseUntil < now
```

Expired processing items should be moved to:

```text
retry_wait
```

or:

```text
dead_lettered
```

depending on attempt count.

The source lease must also be cleared or blocked accordingly.

---

## 17. Retry/backoff

MVP retry strategy:

```text
fixed or exponential backoff
maxAttempts per item
availableAt controls retry time
```

Backoff can be server-defaulted.

Example:

```text
attempt 1: 10 seconds
attempt 2: 30 seconds
attempt 3: 2 minutes
attempt 4: 10 minutes
attempt 5: dead_lettered
```

Allow client to provide `maxAttempts`.

---

## 18. Admin operations

MVP admin operations:

```text
GET queue item
GET queue items by source
GET blocked sources
POST retry dead-lettered item
POST skip item
POST cancel item
POST unblock source
```

Admin operations must be audited later, but full audit can be deferred if not in MVP.

Required behaviour:

```text
skip item:
  item.status = skipped
  source may continue

retry item:
  item.status = retry_wait
  availableAt = now
  source.status = idle

cancel item:
  item.status = cancelled
  source may continue
```

---

## 19. HTTP API

Implement OpenAPI.

## 19.1 Enqueue

```http
POST /queues/{queueName}/items
```

Request:

```json
{
  "sourceId": "workflow-instance-123",
  "itemType": "wf.command",
  "idempotencyKey": "cmd-123",
  "payload": {
    "commandName": "sendEmail",
    "workflowInstanceId": "inst-123",
    "threadId": "_main"
  },
  "headers": {
    "tenantId": "tenant-1",
    "correlationId": "corr-456"
  },
  "availableAt": "2026-06-11T10:00:00Z",
  "maxAttempts": 5
}
```

Response:

```json
{
  "itemId": "item-789",
  "queueName": "wf.commands",
  "sourceId": "workflow-instance-123",
  "sequenceNo": 42,
  "status": "pending"
}
```

## 19.2 Claim

```http
POST /queues/{queueName}/claims
```

Request:

```json
{
  "workerId": "worker-1",
  "supportedItemTypes": ["wf.command", "wf.notification"],
  "leaseSeconds": 60,
  "maxItems": 1
}
```

Response if item available:

```json
{
  "leaseId": "lease-123",
  "queueName": "wf.commands",
  "sourceId": "workflow-instance-123",
  "leaseUntil": "2026-06-11T10:01:00Z",
  "items": [
    {
      "itemId": "item-789",
      "sequenceNo": 42,
      "itemType": "wf.command",
      "payload": {},
      "headers": {}
    }
  ]
}
```

Response if no item available:

```json
{
  "items": []
}
```

## 19.3 Complete

```http
POST /queues/{queueName}/items/{itemId}/complete
```

Request:

```json
{
  "workerId": "worker-1",
  "leaseId": "lease-123",
  "result": {
    "status": "email_sent"
  }
}
```

## 19.4 Fail

```http
POST /queues/{queueName}/items/{itemId}/fail
```

Request:

```json
{
  "workerId": "worker-1",
  "leaseId": "lease-123",
  "retryable": true,
  "errorType": "SMTP_TIMEOUT",
  "errorMessage": "Timed out contacting SMTP"
}
```

## 19.5 Heartbeat

```http
POST /queues/{queueName}/leases/{leaseId}/heartbeat
```

Request:

```json
{
  "workerId": "worker-1",
  "extendBySeconds": 60
}
```

## 19.6 Query source items

```http
GET /queues/{queueName}/sources/{sourceId}/items
```

## 19.7 Query item

```http
GET /queues/{queueName}/items/{itemId}
```

## 19.8 Admin blocked sources

```http
GET /admin/queues/{queueName}/blocked-sources
```

## 19.9 Admin retry

```http
POST /admin/queues/{queueName}/items/{itemId}/retry
```

## 19.10 Admin skip

```http
POST /admin/queues/{queueName}/items/{itemId}/skip
```

## 19.11 Admin cancel

```http
POST /admin/queues/{queueName}/items/{itemId}/cancel
```

---

## 20. Java client library

Create a Java client library.

Package name suggestion:

```text
com.example.sequencedqueue.client
```

Replace `example` later.

Producer example:

```java
SequencedQueueClient client = SequencedQueueClient.builder()
    .baseUrl("http://localhost:8080")
    .apiKey("dev-key")
    .build();

EnqueueResponse response = client.enqueue("wf.commands", EnqueueRequest.builder()
    .sourceId("inst-123")
    .itemType("wf.command")
    .idempotencyKey("cmd-789")
    .payload(Map.of("commandName", "sendEmail"))
    .headers(Map.of("tenantId", "tenant-1"))
    .build());
```

Worker example:

```java
SequencedQueueWorker worker = client.worker("wf.commands")
    .workerId("notification-worker-1")
    .supports("wf.command")
    .leaseSeconds(60)
    .handler("wf.command", item -> {
        executeCommand(item.payload());
        return QueueResult.success(Map.of("sent", true));
    })
    .build();

worker.runForever();
```

The Java client should handle:

* HTTP calls;
* polling;
* heartbeat while processing;
* complete/fail;
* graceful shutdown;
* backoff when queue empty;
* typed exceptions.

---

## 21. Python client library

Create a Python client library.

Package name suggestion:

```text
sequenced_queue
```

Producer example:

```python
client = SequencedQueueClient(
    base_url="http://localhost:8080",
    api_key="dev-key"
)

client.enqueue(
    queue_name="wf.commands",
    source_id="inst-123",
    item_type="wf.command",
    idempotency_key="cmd-789",
    payload={"commandName": "sendEmail"},
    headers={"tenantId": "tenant-1"}
)
```

Worker example:

```python
worker = client.worker(
    queue_name="wf.commands",
    worker_id="notification-worker-1",
    supported_item_types=["wf.command"],
    lease_seconds=60
)

@worker.handler("wf.command")
def handle_command(item):
    execute_command(item.payload)
    return {"sent": True}

worker.run_forever()
```

The Python client should handle:

* polling;
* heartbeat;
* complete/fail;
* retryable exception mapping;
* graceful shutdown;
* exponential empty-poll backoff.

---

## 22. Testing requirements

Use a layered test strategy.

## 22.1 Unit tests

Test:

* enqueue validation;
* status transitions;
* retry calculation;
* idempotency key handling;
* admin actions;
* source blocked/unblocked logic.

## 22.2 Integration tests with PostgreSQL

Use Testcontainers.

Test:

1. Multiple producers enqueue to same source.
2. Sequence numbers are strictly increasing.
3. Multiple workers do not process same source concurrently.
4. Different sources can be processed concurrently.
5. Failed head item blocks later items for same source.
6. Dead-lettered head item blocks source.
7. Admin skip unblocks source.
8. Lease expiry causes retry.
9. Duplicate enqueue idempotency returns existing item.
10. Worker cannot complete item with wrong lease.
11. Worker cannot complete item after lease expiry.
12. Heartbeat extends lease.
13. Retry_wait item becomes claimable only after `availableAt`.

## 22.3 Client tests

Java client:

* enqueue;
* claim;
* complete;
* fail;
* worker loop;
* heartbeat.

Python client:

* enqueue;
* claim;
* complete;
* fail;
* worker loop;
* heartbeat.

---

## 23. Concurrency tests

Add explicit concurrency tests.

Required tests:

```text
100 concurrent enqueues to same source
  → sequenceNo = 1..100 with no gaps/duplicates

20 workers claiming from one source
  → at most one processing item at a time

20 workers claiming from 20 sources
  → parallel claims happen

head item fails
  → later source items are not claimed

lease expires
  → another worker can reclaim safely
```

---

## 24. Observability

MVP should expose basic metrics if convenient.

Useful metrics:

```text
queue.pending.count
queue.processing.count
queue.dead_lettered.count
queue.claim.latency
queue.item.age
queue.worker.claims
queue.worker.failures
queue.source.blocked.count
```

This can be deferred if necessary, but code should not make it hard later.

---

## 25. Security

MVP can use simple API key authentication.

Do not build full user management.

Security model:

```text
API key required
workerId supplied by client
admin endpoints require admin API key
```

Later:

```text
OAuth / OIDC
tenant permissions
queue-level permissions
worker identity
```

---

## 26. Configuration

Server configuration:

```yaml
sequencedQueue:
  defaultLeaseSeconds: 60
  maxLeaseSeconds: 600
  defaultMaxAttempts: 5
  recoveryIntervalSeconds: 30
  maxPayloadBytes: 262144
  emptyPollSleepMillis: 250
```

---

## 27. Development sequence

Implement in small phases.

## Phase 1 — Server skeleton

* Spring Boot project
* PostgreSQL connection
* Flyway migration
* health endpoint
* basic config
* OpenAPI setup

## Phase 2 — Enqueue

* source_state creation
* sequence assignment
* item insert
* idempotency key
* enqueue endpoint
* enqueue integration tests

## Phase 3 — Claim / complete

* claim source lease
* claim head item
* complete item
* release source
* integration tests for per-source exclusivity

## Phase 4 — Fail / retry / dead-letter

* fail endpoint
* retry_wait
* backoff
* max attempts
* blocked source
* admin skip/retry/cancel

## Phase 5 — Lease heartbeat / recovery

* heartbeat endpoint
* expired lease scanner
* reclaim tests

## Phase 6 — Java client

* producer API
* worker API
* heartbeat
* worker loop
* examples

## Phase 7 — Python client

* producer API
* worker API
* heartbeat
* worker loop
* examples

## Phase 8 — Documentation

* README
* API examples
* worker examples
* ordering guarantee explanation
* failure semantics
* deployment guide

---

## 28. Acceptance criteria

The project is acceptable when:

1. PostgreSQL-backed server starts locally.
2. OpenAPI endpoint documentation exists.
3. Producers can enqueue items with source IDs.
4. Sequence numbers are assigned per source.
5. Multiple workers can claim items.
6. Same-source items are never processed concurrently.
7. Same-source items are processed in sequence order.
8. Different-source items can process concurrently.
9. Failed head item blocks later same-source items.
10. Retry/backoff works.
11. Dead-letter blocks source.
12. Admin skip/retry/cancel works.
13. Lease expiry recovery works.
14. Java client can enqueue and run worker loop.
15. Python client can enqueue and run worker loop.
16. Testcontainers integration tests prove concurrency guarantees.

---

## 29. Design warnings

Do not accidentally break the ordering invariant.

Specifically, avoid:

```text
claiming arbitrary pending queue_item rows directly
processing retry_wait item after later item
letting dead_lettered head item be skipped automatically
holding DB transactions open during handler execution
treating exactly-once side effects as guaranteed
using LISTEN/NOTIFY as the source of truth
```

The source lease is the core mechanism.

---

## 30. MVP implementation note

Start with one item per claim.

Do not implement batch-draining until the simple version is correct.

The first reliable version should prefer correctness over throughput.

The throughput improvement path is:

```text
batch claims
source draining
partitioning
read replicas for query APIs
archiving completed items
optional RabbitMQ bridge later
```

---

## 31. Expected final repository structure

Suggested repository layout:

```text
sequenced-queue/
  README.md
  docs/
    architecture.md
    api.md
    ordering-semantics.md
    failure-semantics.md
  server/
    pom.xml
    src/main/java/...
    src/main/resources/db/migration/...
    src/test/java/...
  clients/
    java/
      pom.xml
      src/main/java/...
      src/test/java/...
    python/
      pyproject.toml
      sequenced_queue/
      tests/
  examples/
    java-worker/
    python-worker/
    docker-compose.yml
```

---

## 32. First Codex task

Begin by generating the repository skeleton and the server MVP.

Do not implement everything at once.

First concrete task:

```text
Create the sequenced-queue repository skeleton with:
- Spring Boot server module
- PostgreSQL docker-compose
- Flyway migration for queue_source_state and queue_item
- enqueue endpoint
- basic OpenAPI documentation
- Testcontainers integration test proving sequence numbers increase for one source
```

After this task passes, continue with claim/complete behaviour.

---

## 33. Coding style

Prefer:

* clear domain classes;
* explicit status enums;
* small services;
* repository interfaces;
* transactional service methods;
* integration tests for concurrency;
* no clever abstractions early;
* readable SQL for claim logic;
* OpenAPI-first or OpenAPI-generated docs.

Do not hide core queue semantics behind generic framework magic.

---

## 34. Final product definition

The component is successful if it can be described honestly as:

```text
A lightweight PostgreSQL-backed durable queue that guarantees sequential processing per source while allowing parallel processing across sources, with HTTP, Java, and Python clients.
```
