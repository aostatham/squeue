# wf Adoption Example — Direct Java/PostgreSQL Queue

This document shows how a Java workflow system, referred to here as `wf`, can adopt `sequenced-queue` using the MVP-supported packages:

```text
sequenced-queue-core
sequenced-queue-java-direct-client
```

The intended use case is:

```text
wf emits durable commands/events.
Separate Java worker processes consume them asynchronously.
Commands/events for the same workflow/source are processed sequentially.
Commands/events for different workflows/sources can process concurrently.
```

This example does **not** require:

```text
REST server
OpenAPI
Docker server packaging
Python client
Java REST client
HTTP API
```

Those components may exist in the repository, but they are outside the MVP support boundary for this direct Java `wf` example.

---

## 1. Architecture

The runtime architecture is:

```text
wf runtime process
  |
  | enqueue command/event
  v
SequencedQueueDirectClient
  |
  | caller-provided DataSource / JDBC
  v
PostgreSQL queue tables


wf worker process 1
  |
  | claim / heartbeat / complete / fail
  v
SequencedQueueDirectClient
  |
  v
PostgreSQL queue tables


wf worker process 2
  |
  | claim / heartbeat / complete / fail
  v
SequencedQueueDirectClient
  |
  v
PostgreSQL queue tables
```

There is no HTTP hop.

There is no queue server process.

PostgreSQL is the durable coordination point.

The queue provides:

```text
durable command/event storage
strict per-source ordering
parallel processing across different sources
asynchronous execution by separate Java worker processes
lease-based crash recovery
retry/dead-letter handling
admin repair for blocked sources
```

---

## 2. MVP-supported packages

### Package 1 — Core with schema

`sequenced-queue-core` owns the schema and core queue semantics.

Example Maven dependency:

```xml
<dependency>
  <groupId>com.sequencedqueue</groupId>
  <artifactId>sequenced-queue-core</artifactId>
  <version>0.1.1</version>
</dependency>
```

Use the actual project `groupId` if different.

The core package provides:

```text
PostgreSQL V1 schema baseline
queue tables and indexes
core SQL/state transitions
source ordering semantics
lease/recovery semantics
retry/dead-letter/admin behaviour
schema metadata
core validation
```

### Package 2 — Direct Java API

`sequenced-queue-java-direct-client` is the primary MVP access path for trusted Java systems.

Example Maven dependency:

```xml
<dependency>
  <groupId>com.sequencedqueue</groupId>
  <artifactId>sequenced-queue-java-direct-client</artifactId>
  <version>0.1.1</version>
</dependency>
```

The direct Java client provides:

```text
DataSource-backed producer API
DataSource-backed worker API
claim / complete / fail / heartbeat
lease recovery
admin repair operations
schema compatibility validation
typed exceptions
worker helper
```

The direct Java client does not require the REST server.

---

## 3. Schema lifecycle

`wf` should not create or reset the queue schema on every run.

The correct lifecycle is:

```text
deployment/setup:
  install the queue schema once

normal wf startup:
  validate the schema exists and is compatible

normal wf runtime:
  enqueue, claim, complete, fail, recover, repair

wf shutdown/restart:
  queue tables and queued data remain in PostgreSQL
```

The direct Java client should validate the schema. It should not create, migrate, drop, or reset the queue tables during normal operation.

---

## 4. Installing the schema

The queue schema is supplied by `sequenced-queue-core`.

For a simple deployment, install the packaged SQL script with `psql`.

Example:

```bash
psql \
  -h localhost \
  -U wf_admin \
  -d wf_db \
  -v ON_ERROR_STOP=1 \
  -f V1__initial_queue_schema.sql
```

Do not put production passwords directly on the command line. Prefer:

```text
~/.pgpass
environment-managed secrets
CI/CD secret variables
deployment secret manager
```

The schema should be installed by a deployment or migration user, not by the normal `wf` runtime user.

A typical split is:

```text
wf_admin:
  owns schema installation and upgrades

wf_app:
  runs wf and queue workers
  reads/writes queue data
  does not run DDL
```

If Flyway is used for deployment, the same V1 schema may be applied as a Flyway migration:

```java
Flyway.configure()
    .dataSource(adminDataSource)
    .locations("classpath:db/migration")
    .load()
    .migrate();
```

This should be treated as deployment/migration code, not as normal direct-client startup behaviour.

---

## 5. Runtime schema validation

At normal startup, `wf` should validate that the schema already exists.

Example direct client construction:

```java
SequencedQueueDirectClient queueClient =
    SequencedQueueDirectClient.builder()
        .dataSource(dataSource)
        .validateSchemaOnBuild(true)
        .build();
```

Expected behaviour:

```text
schema exists: client starts
schema missing: client fails fast
schema incompatible: client fails fast
```

The direct client should not silently create or upgrade schema during ordinary runtime.

---

## 6. DataSource configuration

The direct Java API uses a caller-provided `DataSource`.

A production `wf` deployment should use a pooled `DataSource`, for example HikariCP.

Example:

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/wf_db");
config.setUsername("wf_app");
config.setPassword(System.getenv("WF_DB_PASSWORD"));
config.setMaximumPoolSize(10);

DataSource dataSource = new HikariDataSource(config);
```

Then pass that `DataSource` to the direct queue client:

```java
SequencedQueueDirectClient queueClient =
    SequencedQueueDirectClient.builder()
        .dataSource(dataSource)
        .validateSchemaOnBuild(true)
        .build();
```

The direct client should not create a global static connection, and it should not open an unpooled connection per operation.

---

## 7. Queue design for wf

A `wf` integration needs to choose:

```text
queueName
sourceId
itemType
payload
headers
idempotencyKey
```

### Recommended queue names

For workflow commands:

```text
wf.commands
```

For workflow events:

```text
wf.events
```

For the first MVP integration, start with:

```text
wf.commands
```

Add `wf.events` only if events need a separate queue and worker topology.

---

## 8. Choosing `sourceId`

`sourceId` determines the ordering boundary.

The queue guarantees that items for the same `(queueName, sourceId)` are processed sequentially and not concurrently.

Recommended first choice:

```text
sourceId = workflowInstanceId
```

This gives:

```text
same workflow instance = sequential command processing
different workflow instances = concurrent processing
```

Example:

```text
queueName = wf.commands
sourceId  = wf-123
```

Commands for `wf-123` remain ordered.

Commands for `wf-123` and `wf-456` may process concurrently.

### Alternative sourceId

If `wf` has independent threads inside a workflow instance and those threads may safely run concurrently, use:

```text
sourceId = workflowInstanceId + ":" + threadId
```

Example:

```text
sourceId = wf-123:_main
sourceId = wf-123:approval-branch
```

This allows separate workflow threads to process concurrently while preserving ordering within each thread.

Start with `workflowInstanceId` unless concurrency inside a workflow instance is definitely safe.

---

## 9. Choosing `itemType`

For the first integration, use a broad command type:

```text
itemType = wf.command
```

Then include the actual command name inside the payload:

```json
{
  "commandName": "SendEmail"
}
```

This simplifies worker registration.

Later, if useful, split into more specific item types:

```text
wf.command.SendEmail
wf.command.CallWebhook
wf.command.GenerateDocument
```

---

## 10. Enqueueing a wf command

Example command payload:

```json
{
  "commandName": "SendEmail",
  "workflowInstanceId": "wf-123",
  "threadId": "_main",
  "commandId": "cmd-456",
  "arguments": {
    "template": "welcome",
    "recipient": "user@example.com"
  }
}
```

Example enqueue code:

```java
Map<String, Object> payload = Map.of(
    "commandName", "SendEmail",
    "workflowInstanceId", workflowInstanceId,
    "threadId", "_main",
    "commandId", commandId,
    "arguments", Map.of(
        "template", "welcome",
        "recipient", "user@example.com"
    )
);

EnqueueResponse response = queueClient.enqueue(
    "wf.commands",
    EnqueueRequest.builder()
        .sourceId(workflowInstanceId)
        .itemType("wf.command")
        .idempotencyKey(commandId)
        .payload(payload)
        .headers(Map.of(
            "correlationId", correlationId,
            "tenantId", tenantId
        ))
        .maxAttempts(5)
        .build()
);
```

Use the current public API names from `sequenced-queue-java-direct-client`. If method/class names differ, keep the same conceptual structure and update the example to match the actual API.

Important fields:

```text
queueName       wf.commands
sourceId        workflowInstanceId
itemType        wf.command
idempotencyKey  commandId
payload         command JSON
headers         metadata such as correlationId and tenantId
```

---

## 11. Transaction boundary with wf state

A workflow system usually has its own state tables.

The ideal integration is:

```text
begin transaction
  update wf workflow state
  enqueue queue item
commit
```

This gives atomicity between:

```text
wf state transition
queued command/event
```

There are two possible integration modes.

### Mode A — queue operation owns its transaction

Simpler:

```text
wf updates state
wf commits state
wf enqueues command
```

Risk:

```text
state commits but enqueue fails
```

or:

```text
enqueue succeeds but wf crashes before recording that fact
```

This can be acceptable if `wf` has reconciliation or an outbox table.

### Mode B — caller-managed transaction

Preferred for workflow engines:

```text
begin transaction
  update wf state table
  insert/enqueue queue command
commit
```

If the current direct Java API supports caller-managed transactions, use that for `wf`.

If it does not, keep this as an integration follow-up:

```text
Support caller-managed transaction participation for wf adoption.
```

Do not hold a database transaction open while executing worker handler code.

---

## 12. Worker process

A separate Java worker process consumes queued commands.

Example:

```java
SequencedQueueDirectClient queueClient =
    SequencedQueueDirectClient.builder()
        .dataSource(dataSource)
        .validateSchemaOnBuild(true)
        .build();

SequencedQueueDirectWorker worker =
    queueClient.worker("wf.commands")
        .workerId("wf-command-worker-1")
        .supports("wf.command")
        .leaseSeconds(60)
        .emptyPollBackoff(Duration.ofMillis(250))
        .handler("wf.command", item -> {
            Map<String, Object> payload = item.payload();

            String commandName = (String) payload.get("commandName");

            switch (commandName) {
                case "SendEmail" -> {
                    sendEmail(payload);
                    return QueueResult.success(Map.of("sent", true));
                }
                case "CallWebhook" -> {
                    callWebhook(payload);
                    return QueueResult.success(Map.of("called", true));
                }
                default -> {
                    throw new NonRetryableQueueException(
                        "Unknown command: " + commandName
                    );
                }
            }
        })
        .build();

worker.runForever();
```

The worker helper should handle:

```text
polling
claiming
heartbeat
handler execution
complete on success
fail on exception
lease-lost handling
empty-poll backoff
graceful shutdown
```

User handler code should run outside database transactions.

Claim, heartbeat, complete, and fail operations should use short database transactions.

---

## 13. Scaling workers

Multiple worker processes can run at the same time:

```text
wf-command-worker-1
wf-command-worker-2
wf-command-worker-3
```

The queue should allow:

```text
wf-123 command 1 and wf-456 command 1 to process concurrently
```

But prevent:

```text
wf-123 command 1 and wf-123 command 2 processing concurrently
```

This is the key benefit for workflow command processing.

---

## 14. Retry and failure mapping

Handlers should distinguish retryable and non-retryable failures.

### Retryable failures

Examples:

```text
HTTP 503 from external service
SMTP timeout
temporary database issue
rate limit
network timeout
```

These should be failed as retryable so the item can move to retry/backoff and be attempted again.

### Non-retryable failures

Examples:

```text
unknown commandName
invalid command payload
missing required workflow data
permanent validation failure
```

These should be failed as non-retryable or allowed to dead-letter according to the current direct API semantics.

A practical rule:

```text
unexpected infrastructure problem = retryable
invalid command definition = non-retryable
repeated retryable failure until maxAttempts = dead-lettered
```

A dead-lettered head item blocks the source. For `wf`, this is desirable because command `N + 1` must not overtake failed command `N`.

---

## 15. Dead-letter repair

When a command becomes dead-lettered, `wf` needs an admin repair path.

Typical actions:

```text
retry
skip
cancel
unblock
list blocked sources
```

### Retry

Use when the underlying problem has been fixed:

```text
external service recovered
configuration corrected
bug fixed and deployed
```

### Skip

Use when the command can safely be ignored:

```text
manual correction already completed
notification no longer needed
duplicate command already handled
```

### Cancel

Use when the command should become terminally cancelled:

```text
workflow cancelled
business process no longer valid
```

### Unblock

Use only when the blocking head item has already been resolved.

Do not automatically bypass a dead-lettered head item. That would break workflow ordering.

---

## 16. Lease recovery

If a worker process crashes while processing an item:

```text
item remains in processing
lease expires
recovery makes the item claimable again or moves it toward dead-letter
another worker can continue later
```

`wf` needs a recovery mechanism.

Options:

```text
run recovery periodically inside each worker process
run one dedicated recovery scheduler process
run recovery in the wf runtime process
```

A simple worker-side scheduler is enough for an MVP example:

```java
ScheduledExecutorService recovery =
    Executors.newSingleThreadScheduledExecutor();

recovery.scheduleAtFixedRate(() -> {
    try {
        int recovered = queueClient.recoverExpiredLeases(
            "wf.commands",
            Instant.now(),
            100
        );

        log.info("Recovered {} expired wf command queue leases", recovered);
    } catch (Exception e) {
        log.warn("wf command queue lease recovery failed", e);
    }
}, 30, 30, TimeUnit.SECONDS);
```

Use the actual direct API method signature if it differs.

---

## 17. Idempotent handlers

The queue provides at-least-once delivery.

It does not provide exactly-once side effects.

Therefore `wf` handlers must be idempotent.

Use a stable command ID:

```text
commandId
```

A robust side-effect pattern is:

```text
handler starts
  check wf_command_effects for commandId
  if commandId already completed:
      return success

  perform external side effect

  record commandId completed

  return success
```

This matters for commands such as:

```text
send email
call webhook
charge payment
create ticket
write to another external system
```

The queue prevents same-source concurrency, but crash recovery can still redeliver work.

---

## 18. Optional wf command table

For stronger traceability, `wf` can keep its own command/outbox table.

Example:

```text
wf_command
  command_id
  workflow_instance_id
  thread_id
  command_name
  command_payload_json
  queue_item_id
  status
  created_at
  updated_at
```

Suggested flow:

```text
wf creates command record
wf enqueues queue item with idempotencyKey = command_id
wf stores queue item id
worker processes queue item
worker updates wf_command status
```

This separates `wf` domain history from queue transport state.

---

## 19. Minimal adoption sequence

### Step 1 — Add dependencies

Add:

```text
sequenced-queue-core
sequenced-queue-java-direct-client
```

to the Java modules that enqueue and consume `wf` commands.

### Step 2 — Install schema once

Install the V1 queue schema using deployment SQL or Flyway.

Do this once per environment/database.

Do not recreate the schema on every `wf` run.

### Step 3 — Configure DataSource

Use the normal `wf` PostgreSQL `DataSource` or a dedicated queue `DataSource`.

### Step 4 — Build direct client

Build the client with schema validation enabled:

```java
SequencedQueueDirectClient queueClient =
    SequencedQueueDirectClient.builder()
        .dataSource(dataSource)
        .validateSchemaOnBuild(true)
        .build();
```

### Step 5 — Enqueue one command

Enqueue:

```text
queueName = wf.commands
sourceId = workflowInstanceId
itemType = wf.command
idempotencyKey = commandId
payload = command JSON
```

### Step 6 — Run one worker

Run a separate Java process that supports:

```text
wf.command
```

and completes the item.

### Step 7 — Add failure handling

Map handler failures to retryable and non-retryable outcomes.

### Step 8 — Add lease recovery

Run recovery periodically.

### Step 9 — Add admin repair

Expose internal operations for:

```text
list blocked sources
retry
skip
cancel
unblock
```

### Step 10 — Add ordering/concurrency tests

Prove the workflow ordering model.

---

## 20. Minimal integration test

Create a test with multiple commands for multiple workflows.

Input:

```text
workflow A: A1, A2, A3
workflow B: B1, B2, B3
```

Run multiple workers.

Expected:

```text
A is processed as A1 -> A2 -> A3
B is processed as B1 -> B2 -> B3
A and B may interleave
A items do not process concurrently
B items do not process concurrently
```

Valid processing order:

```text
A1, B1, B2, A2, B3, A3
```

Invalid processing order:

```text
A1, A3, A2
```

Invalid concurrency:

```text
A1 and A2 processing at the same time
```

---

## 21. Suggested wf adapter classes

A practical `wf` integration can wrap the direct queue API behind small domain-specific classes.

Suggested classes:

```text
WfQueueClientFactory
WfCommandQueue
WfCommandWorker
WfCommandDispatcher
WfQueueRecoveryScheduler
WfQueueAdminService
```

Example producer wrapper:

```java
public final class WfCommandQueue {
    private final SequencedQueueDirectClient client;

    public WfCommandQueue(SequencedQueueDirectClient client) {
        this.client = client;
    }

    public EnqueueResponse enqueueCommand(WfCommand command) {
        return client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId(command.workflowInstanceId())
            .itemType("wf.command")
            .idempotencyKey(command.commandId())
            .payload(command.toPayload())
            .headers(command.toHeaders())
            .build());
    }
}
```

Example worker wrapper:

```java
public final class WfCommandWorker {
    private final SequencedQueueDirectClient client;
    private final WfCommandDispatcher dispatcher;
    private final String workerId;

    public WfCommandWorker(
        SequencedQueueDirectClient client,
        WfCommandDispatcher dispatcher,
        String workerId
    ) {
        this.client = client;
        this.dispatcher = dispatcher;
        this.workerId = workerId;
    }

    public void runForever() {
        client.worker("wf.commands")
            .workerId(workerId)
            .supports("wf.command")
            .handler("wf.command", item -> {
                WfCommand command = WfCommand.fromPayload(item.payload());
                dispatcher.dispatch(command);
                return QueueResult.success(Map.of("ok", true));
            })
            .build()
            .runForever();
    }
}
```

---

## 22. Common adoption mistakes

### Mistake 1 — using the wrong sourceId

Too broad:

```text
sourceId = tenantId
```

This serializes too much work.

Too narrow:

```text
sourceId = commandId
```

This allows commands for the same workflow instance to run concurrently.

Recommended first choice:

```text
sourceId = workflowInstanceId
```

### Mistake 2 — non-idempotent handlers

Handlers may be retried after a crash.

External side effects must be idempotent.

### Mistake 3 — no heartbeat for long-running handlers

If handlers can run longer than the lease, heartbeat must be enabled and tested.

### Mistake 4 — no recovery process

Expired leases need recovery.

### Mistake 5 — no admin repair path

Dead-lettered head items block workflow progress until repaired.

### Mistake 6 — runtime schema creation

Do not create or reset queue schema on every `wf` run.

Schema installation is a deployment step.

Normal startup should only validate schema compatibility.

---

## 23. Summary

For `wf`, the direct Java adoption model is:

```text
install schema once
keep queue tables and data persistent
configure a pooled DataSource
build the direct Java client
validate schema on startup
enqueue commands with sourceId = workflowInstanceId
run separate Java worker processes
heartbeat long-running work
recover expired leases
make handlers idempotent
repair dead-lettered head items explicitly
```

This gives `wf` the original required behaviour:

```text
commands/events can be fired durably
workers consume asynchronously in separate Java processes
same workflow/source remains ordered
different workflows/sources run concurrently
PostgreSQL stores queue state across wf restarts
```
