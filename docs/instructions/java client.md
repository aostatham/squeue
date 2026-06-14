# Codex Design Note — Direct Java PostgreSQL Client for `sequenced-queue`

## 1. Purpose

Add a direct Java → SQL → direct client for `sequenced-queue`.

This client exists alongside the REST clients.

The goal is to provide a high-performance trusted Java client that can enqueue, claim, complete, fail, heartbeat, and administer queue items by talking directly to PostgreSQL.

The direct Java client must preserve exactly the same queue semantics as the server API.

The central invariant remains:

```text
For each (queueName, sourceId), items are processed in sequenceNo order and at most one worker may process that source at a time.
```

Different sources may be processed concurrently.

---

## 2. Why this client exists

The REST client is the default external integration mechanism.

The direct Java direct client exists for trusted internal deployments where:

* workers are Java-based;
* workers are deployed close to PostgreSQL;
* low latency matters;
* high claim/complete throughput matters;
* direct database access is acceptable;
* the deployment wants to avoid HTTP overhead;
* the queue is used as an embedded infrastructure component.

Example use cases:

```text
wf-runtime and Java command workers deployed in the same trusted environment.
Java workers consume command work directly from PostgreSQL.
REST remains available for Python and external clients.
```

---

## 3. Non-goals

The direct Java client must not become a second queue product.

Do not build:

* a separate schema;
* separate semantics;
* direct SQL clients for every language;
* Python direct SQL client in MVP;
* alternate ordering logic;
* alternate status transition rules;
* client-side interpretation of queue semantics that conflicts with the server;
* bypasses around source leasing.

The direct Java client is a trusted optimisation path, not the primary portability layer.

---

## 4. Access model

There will be three supported access modes:

```text
1. REST client
   Java/Python/external workers → HTTP → queue server → PostgreSQL

2. Embedded Java service mode
   Same JVM code → QueueService → PostgreSQL

3. Direct Java direct client
   Java worker → JDBC/DataSource → PostgreSQL
```

The direct Java client should use the same schema and status model as the REST server.

It must not require the queue server to be running.

---

## 5. Package/module structure

Suggested repository layout:

```text
sequenced-queue/
  server/
  core/
  clients/
    java-rest/
    java-direct/
    python-rest/
```

The new module should be:

```text
clients/java-direct
```

Suggested package:

```text
com.sequencedqueue.direct
```

If a shared Java domain module already exists, reuse it:

```text
core/
  QueueItem
  QueueStatus
  EnqueueRequest
  ClaimRequest
  ClaimResponse
  QueueResult
```

The direct client should avoid duplicating DTOs where possible.

---

## 6. Critical design rule

The direct Java direct client must share the same core SQL semantics as the server.

Preferred design:

```text
core SQL/repository logic lives in one reusable Java module
server REST controller calls that module
direct Java client calls that module
```

Avoid this:

```text
server has one SQL implementation
java-direct-client has a copied second SQL implementation
```

If full sharing is difficult, the direct client may initially duplicate SQL, but every duplicated SQL path must be covered by the same integration test contract as the server.

---

## 7. Database connectivity

The client should be constructed from a `javax.sql.DataSource`.

Example:

```java
SequencedQueueDirectClient client =
    SequencedQueueDirectClient.builder()
        .dataSource(dataSource)
        .defaultQueueName("wf.commands")
        .workerId("notification-worker-1")
        .build();
```

The client must not own a global static connection.

The client must not open unpooled connections per operation.

Use the caller-provided `DataSource` so applications can use HikariCP, application-server pools, or Spring-managed pools.

---

## 8. Public Java API

## 8.1 Producer API

```java
EnqueueResponse enqueue(String queueName, EnqueueRequest request);
```

Example:

```java
EnqueueResponse response = client.enqueue("wf.commands",
    EnqueueRequest.builder()
        .sourceId("inst-123")
        .itemType("wf.command")
        .idempotencyKey("cmd-789")
        .payloadJson("""
            {"commandName":"sendEmail","workflowInstanceId":"inst-123"}
            """)
        .headersJson("""
            {"tenantId":"tenant-1"}
            """)
        .maxAttempts(5)
        .build());
```

## 8.2 Worker claim API

```java
Optional<ClaimItem> claimNext(ClaimRequest request);
```

Example:

```java
Optional<ClaimItem> claim = client.claimNext(ClaimRequest.builder()
    .queueName("wf.commands")
    .workerId("worker-1")
    .supportedItemTypes(List.of("wf.command"))
    .leaseSeconds(60)
    .build());
```

## 8.3 Completion API

```java
void complete(CompleteRequest request);
```

Example:

```java
client.complete(CompleteRequest.builder()
    .queueName("wf.commands")
    .itemId(item.itemId())
    .leaseId(item.leaseId())
    .workerId("worker-1")
    .resultJson("""
        {"sent":true}
        """)
    .build());
```

## 8.4 Failure API

```java
void fail(FailRequest request);
```

Example:

```java
client.fail(FailRequest.builder()
    .queueName("wf.commands")
    .itemId(item.itemId())
    .leaseId(item.leaseId())
    .workerId("worker-1")
    .retryable(true)
    .errorType("SMTP_TIMEOUT")
    .errorMessage("Timed out contacting SMTP")
    .build());
```

## 8.5 Heartbeat API

```java
void heartbeat(HeartbeatRequest request);
```

## 8.6 Admin API

```java
void retry(String queueName, UUID itemId);
void skip(String queueName, UUID itemId, String reason);
void cancel(String queueName, UUID itemId, String reason);
List<BlockedSource> listBlockedSources(String queueName, int limit);
```

---

## 9. Worker convenience API

Provide a worker loop wrapper similar to the REST Java worker.

Example:

```java
SequencedQueueDirectWorker worker = client.worker("wf.commands")
    .workerId("notification-worker-1")
    .supports("wf.command")
    .leaseSeconds(60)
    .emptyPollBackoff(Duration.ofMillis(250))
    .handler("wf.command", item -> {
        sendEmail(item.payloadJson());
        return QueueResult.success("""
            {"sent":true}
            """);
    })
    .build();

worker.runForever();
```

The worker wrapper should:

* poll using `claimNext`;
* call handler;
* heartbeat during long processing;
* complete on success;
* fail on exception;
* apply empty-poll backoff;
* stop gracefully;
* avoid holding database transactions while executing handler code.

---

## 10. Transaction discipline

The direct client must use short transactions.

Correct pattern:

```text
claim transaction:
  lock source
  mark source leased
  mark head item processing
  commit

execute handler:
  no database transaction held

complete/fail transaction:
  validate lease
  update item
  release or block source
  commit
```

Do not hold a database transaction while executing user command logic.

This is mandatory.

---

## 11. Enqueue SQL behaviour

Enqueue must be transactional.

Pseudo-flow:

```text
begin
  insert source_state if missing
  select source_state for update
  if idempotencyKey exists:
      return existing item
  assign sequenceNo = nextSequenceNo
  increment nextSequenceNo
  insert queue_item
commit
```

Idempotency rule:

```text
If (queueName, idempotencyKey) already exists, return the existing item.
```

If `idempotencyKey` is null, create a new item.

Do not generate sequence numbers outside the transaction.

---

## 12. Claim SQL behaviour

The client must claim by source lease first.

Do not claim arbitrary queue item rows.

Claim flow:

```text
begin
  find source with claimable head item
  lock source_state with FOR UPDATE SKIP LOCKED
  set source leased_by, lease_until, lease_id
  select lowest sequence claimable item for that source
  mark item processing
commit
```

A source is claimable only if:

```text
source.status = idle
OR source.status = leased and source.lease_until < now()
```

and the source has an available head item.

The head item is the lowest sequence item that is not terminal/passable.

Claim must not allow sequence `N+1` if sequence `N` is pending, retry_wait, processing, or dead_lettered.

---

## 13. Head item rule

Define passable terminal statuses:

```text
succeeded
cancelled
skipped
failed
```

Define blocking statuses:

```text
pending
retry_wait
processing
dead_lettered
```

For a source, the next processable item is the lowest sequence item whose status is `pending` or `retry_wait`, but only if no earlier item has a blocking status.

If the lowest blocking item is `dead_lettered`, source must become `blocked`.

---

## 14. Claim SQL shape

Codex should implement readable SQL. Use native SQL through JDBC.

The claim may be implemented as multiple statements in one transaction for clarity.

Avoid clever SQL if it risks correctness.

Approximate algorithm:

```text
1. Select candidate source rows using SKIP LOCKED.
2. For each candidate source:
   a. Determine head blocking item.
   b. If dead_lettered, mark source blocked and continue.
   c. If pending/retry_wait and availableAt <= now, claim it.
   d. Otherwise release source and continue.
```

This is acceptable for MVP if indexed and tested.

Optimisation can come later.

---

## 15. Lease model

The direct client should use a lease token.

Add or use:

```text
lease_id UUID
leased_by TEXT
lease_until TIMESTAMPTZ
```

on `queue_source_state`, and:

```text
lease_id UUID
claimed_by TEXT
lease_until TIMESTAMPTZ
```

on `queue_item`.

If the current schema lacks `lease_id`, add it.

The worker must present `leaseId` when completing, failing, or heartbeating.

Completion must fail if the lease is expired or mismatched.

---

## 16. Complete behaviour

Completion transaction:

```text
begin
  validate item.status = processing
  validate item.claimedBy = workerId
  validate item.leaseId = leaseId
  validate item.leaseUntil >= now
  validate source.leasedBy = workerId
  validate source.leaseId = leaseId
  set item.status = succeeded
  set result_json
  clear item lease fields
  release source lease
commit
```

If validation fails, throw a typed exception:

```text
LeaseLostException
ItemNotProcessingException
QueueConflictException
```

---

## 17. Failure behaviour

Failure transaction:

If retryable and attempts remain:

```text
item.status = retry_wait
item.availableAt = now + backoff
clear item lease
release source lease
```

If attempts exhausted:

```text
item.status = dead_lettered
source.status = blocked
clear item lease
clear source lease
```

If non-retryable:

```text
item.status = failed
clear item lease
release source lease
```

Backoff should be configurable.

---

## 18. Heartbeat behaviour

Heartbeat transaction:

```text
validate source lease
validate item lease if itemId supplied
extend source.leaseUntil
extend item.leaseUntil
commit
```

Heartbeat should not change item status.

---

## 19. Lease recovery

Provide a recovery method:

```java
int recoverExpiredLeases(String queueName, Instant now, int limit);
```

This can be called by:

* queue server scheduler;
* direct client worker supervisor;
* admin process.

Recovery logic:

```text
Find processing items with leaseUntil < now.
For each:
  if attemptCount >= maxAttempts:
      item.status = dead_lettered
      source.status = blocked
  else:
      item.status = retry_wait
      item.availableAt = now
      source.status = idle
  clear lease fields
```

Do not recover items with valid leases.

---

## 20. Admin repair operations

Implement:

```text
retry dead-lettered item
skip item
cancel item
unblock source
```

Rules:

```text
retry:
  item.status = retry_wait
  item.availableAt = now
  source.status = idle

skip:
  item.status = skipped
  source.status = idle

cancel:
  item.status = cancelled
  source.status = idle

unblock source:
  only valid if no dead_lettered head item remains
```

Admin operations must use transactions.

---

## 21. Isolation level

Use PostgreSQL default `READ COMMITTED`.

Do not use long `REPEATABLE READ` transactions for worker claim loops.

The claim logic relies on row locks and `SKIP LOCKED`, and each claim should be a short transaction.

---

## 22. Error handling

Create typed exceptions:

```text
QueueException
QueueUnavailableException
InvalidQueueRequestException
DuplicateIdempotencyKeyException
LeaseLostException
SourceBlockedException
ItemNotFoundException
ItemNotClaimedException
QueueConflictException
```

Do not expose raw SQL exceptions from public APIs.

Wrap SQL exceptions with useful context:

```text
queueName
sourceId
itemId
workerId
operation
```

Do not include full payloads in exception messages.

---

## 23. JSON handling

The direct client should accept JSON as:

```text
String
JsonNode
Map<String,Object>
```

if convenient, but internally store JSONB.

For MVP, string JSON is acceptable if validated.

Do not require the direct client to depend heavily on a specific JSON framework unless the rest of the Java project already uses one.

If using Jackson, provide `JsonNode` overloads.

---

## 24. Performance requirements

Direct direct client is expected to outperform REST for small items by avoiding:

```text
HTTP request/response overhead
JSON round-trip through server controller
extra network hop
server request handling overhead
```

But correctness is more important than raw speed.

Performance targets for MVP are qualitative:

```text
low-latency claim/complete for trusted Java workers
reasonable throughput with multiple workers and sources
no same-source concurrent processing
no sequence overtaking
```

Add benchmark harness later, not in MVP.

---

## 25. Batching

Do not implement batch source draining initially unless the basic implementation is correct.

MVP:

```text
one source lease
one item
one complete/fail
```

Later extension:

```text
claim up to N sources
claim up to M sequential items per source
batch complete
```

Do not compromise source ordering for batching.

---

## 26. Compatibility with REST server

The direct client and REST server must be compatible.

A queue item enqueued by REST must be claimable by direct Java client.

A queue item enqueued by direct Java client must be claimable by REST worker.

Admin repair by REST must affect direct workers correctly.

Admin repair by direct client must affect REST workers correctly.

Add cross-mode integration tests.

---

## 27. Integration tests

Use Testcontainers PostgreSQL.

Required tests:

### Enqueue

```text
direct client enqueue creates source state
sequence numbers increase per source
idempotency key returns existing item
concurrent direct enqueues to same source produce no gaps/duplicates
```

### Claim ordering

```text
one worker claims head item
later item is not claimed before earlier item
different sources can be claimed concurrently
same source cannot be claimed by two workers
```

### REST/direct compatibility

```text
REST enqueue → direct claim → direct complete
direct enqueue → REST claim → REST complete
direct claim → REST complete should fail without correct lease
REST claim → direct complete should fail without correct lease
```

### Failure

```text
retryable failure moves item to retry_wait
retry_wait not claimable before availableAt
attempt exhaustion moves item to dead_lettered
dead_lettered head blocks source
admin skip unblocks source
```

### Lease

```text
heartbeat extends lease
wrong worker cannot complete
wrong lease cannot complete
expired lease cannot complete
recoverExpiredLeases makes item retryable
```

### Concurrency

```text
50 workers claiming one source never process concurrently
50 workers claiming 50 sources can process concurrently
100 concurrent enqueues to same source sequence 1..100
```

---

## 28. Security warning

The direct Java direct client bypasses the REST server’s API-level security.

Therefore:

```text
Only trusted internal services should use it.
```

The database user used by the direct client should have the minimum required privileges.

Recommended:

```text
separate DB role for direct queue workers
no DDL privileges
DML only on queue tables
no access to unrelated application tables
```

If tenant isolation matters, direct clients must either:

```text
use tenant-scoped credentials
```

or:

```text
enforce tenant filtering in the direct client and database policies
```

Tenant security remains an open decision unless row-level security is introduced.

---

## 29. Documentation requirements

Document clearly:

```text
REST clients are default.
Direct PostgreSQL Java client is trusted/internal.
Direct client is faster but has stronger operational/security coupling.
Direct client shares the same schema and semantics.
Do not mix incompatible versions of direct client and server schema.
```

Add version compatibility notes:

```text
client version X supports schema version Y
```

---

## 30. Versioning

Add schema version awareness.

Direct client should optionally check schema compatibility at startup.

Create a method:

```java
QueueSchemaInfo getSchemaInfo();
```

If Flyway is used, read from Flyway schema history or a dedicated table.

Direct client should fail fast if schema is older than required.

---

## 31. Recommended implementation order

### Phase JP-1 — Module skeleton

Create `clients/java-direct`.

Add:

```text
DataSource-based client
basic configuration
domain DTO reuse
typed exceptions
```

### Phase JP-2 — Enqueue

Implement:

```text
enqueue
idempotency
sequence assignment
integration tests
```

### Phase JP-3 — Claim / complete

Implement:

```text
claimNext
source lease
item lease
complete
basic worker loop
```

### Phase JP-4 — Fail / retry / dead-letter

Implement:

```text
fail
backoff
dead-letter
source blocked
admin skip/retry/cancel
```

### Phase JP-5 — Heartbeat / recovery

Implement:

```text
heartbeat
recoverExpiredLeases
lease tests
```

### Phase JP-6 — Compatibility tests

Implement:

```text
REST enqueue → direct claim
direct enqueue → REST claim
cross-mode lease validation
```

### Phase JP-7 — Documentation and examples

Add:

```text
README section
Java direct worker example
performance notes
security warning
schema compatibility note
```

---

## 32. Codex first task

Start with the direct Java direct client skeleton and enqueue support only.

First task:

```text
Create clients/java-direct module.

Implement:
- SequencedQueueDirectClient
- DataSource-based configuration
- enqueue(queueName, EnqueueRequest)
- idempotency handling
- per-source sequence assignment
- typed exceptions
- Testcontainers PostgreSQL integration test:
  100 concurrent enqueues to same source produce sequenceNo 1..100 with no duplicates/gaps
```

Do not implement claim/complete until enqueue is proven correct.

---

## 33. Acceptance criteria

The direct Java direct client is acceptable when:

1. It uses a caller-provided `DataSource`.
2. It does not require the REST server to run.
3. It uses the same PostgreSQL schema as the queue server.
4. It preserves per-source sequence assignment.
5. It preserves per-source source leasing.
6. It never processes two items for the same source concurrently.
7. It does not process later source items before earlier blocking items.
8. It supports complete/fail/heartbeat/recovery.
9. It passes the shared queue contract tests.
10. It is compatible with REST clients.
11. It has clear documentation saying it is for trusted internal Java deployments.

---

## 34. Final design statement

The direct Java direct client is a trusted high-performance access path for `sequenced-queue`.

It must remain semantically identical to the REST server path.

The source of correctness is not the REST API and not the client library.

The source of correctness is:

```text
the shared queue schema
the source lease protocol
the head-item ordering rule
the transaction boundaries
the integration test contract
```
