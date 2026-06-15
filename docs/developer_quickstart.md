# Developer Quickstart

This guide shows the Stage 2 developer path: start PostgreSQL, run the server, enqueue work, run Java and Python workers, and inspect operational state.

For delivery guarantees and non-guarantees, see [Semantics](semantics.md). For a compact examples index, see [Examples README](../examples/README.md).

Security in this quickstart uses the current static/configured API-key model. It is not OAuth/OIDC or production-grade identity management.

## Bootstrap from a Clean Checkout

Compile and install the local Maven modules once so example modules can resolve the local client artifacts:

```sh
./mvnw -DskipTests install
```

Install the Python client dependencies:

```sh
python -m pip install -e sequenced-queue-python-client
```

## Start PostgreSQL

```sh
docker compose up -d postgres
```

The default server configuration expects:

```text
DATABASE_URL=jdbc:postgresql://localhost:5432/sequenced_queue
DATABASE_USERNAME=sequenced_queue
DATABASE_PASSWORD=sequenced_queue
```

## Start the Server

```sh
./mvnw -pl sequenced-queue-server spring-boot:run
```

Default local keys:

```text
SQ_API_KEY=dev-key
SQ_ADMIN_API_KEY=dev-admin-key
```

## Enqueue an Item

Using the Java REST producer example:

```sh
./mvnw -pl examples/java-producer exec:java
```

Using the Python REST producer example:

```sh
python examples/python-producer/producer.py
```

Both examples enqueue item type `example.command` into queue `wf.commands` for source `example-source` unless environment variables override the defaults.

## Run a Java REST Worker

```sh
SQ_RUN_ONCE=true ./mvnw -pl examples/java-rest-worker exec:java
```

The Java REST worker uses `SequencedQueueWorker`, claims one head item at a time, heartbeats halfway through the lease, completes successful handler results, and fails handler errors according to `QueueResult`. Set `SQ_RUN_ONCE=true` for a deterministic quickstart run, or omit it to poll until stopped.

## Run a Python REST Worker

```sh
SQ_RUN_ONCE=true python examples/python-worker/worker.py
```

The Python worker uses `SequencedQueueWorker`, starts a heartbeat thread for claimed work, completes successful handlers, treats `RetryableQueueError` as retryable, and treats other exceptions as non-retryable. Set `SQ_RUN_ONCE=true` for a deterministic quickstart run, or omit it to poll until stopped.

## Run a Java Direct Worker

The direct Java worker bypasses the REST server and talks to PostgreSQL through `sequenced-queue-core`.

```sh
SQ_RUN_ONCE=true ./mvnw -pl examples/java-direct-worker exec:java
```

The direct worker requires the core Flyway schema to already be applied by the server or by your migration process. It enables `validateSchemaOnBuild(true)`, so startup fails fast if the schema is missing or incompatible. It uses `SequencedQueueDirectWorker` for automatic heartbeat, lease-lost detection, complete/fail suppression after lease loss, graceful shutdown, and empty-queue backoff.

## Run Tests

Run the Java reactor tests:

```sh
./mvnw test
```

Run the required Docker-backed PostgreSQL contract suite:

```sh
./mvnw verify -Ppostgres-contract
```

Run the developer-facing examples/OpenAPI/client contract suite:

```sh
./mvnw verify -Pdeveloper-contract
```

Run both suites:

```sh
./mvnw verify -Pfull-contract
```

Run Python client tests:

```sh
cd sequenced-queue-python-client
python -m pytest
```

## Inspect Dead-Letter and Admin State

Use the admin API key:

```sh
curl -H "Authorization: Bearer dev-admin-key" \
  "http://localhost:8080/admin/queues/wf.commands/dead-lettered"

curl -H "Authorization: Bearer dev-admin-key" \
  "http://localhost:8080/admin/queues/wf.commands/blocked-sources"

curl -H "Authorization: Bearer dev-admin-key" \
  "http://localhost:8080/admin/queues/wf.commands/audit"
```

Admin repair examples:

```sh
curl -X POST \
  -H "Authorization: Bearer dev-admin-key" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator retry"}' \
  "http://localhost:8080/admin/queues/wf.commands/items/{itemId}/retry"

curl -X POST \
  -H "Authorization: Bearer dev-admin-key" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator skip"}' \
  "http://localhost:8080/admin/queues/wf.commands/items/{itemId}/skip"
```

Manual retention purge is admin-only and deletes only old passable terminal rows. `limit` defaults to `1000` and is capped by `sequenced-queue.max-retention-purge-batch-size`. Dry-run first:

```sh
curl -X POST \
  -H "Authorization: Bearer dev-admin-key" \
  -H "Content-Type: application/json" \
  -d '{"olderThan":"2026-01-01T00:00:00Z","statuses":["succeeded","cancelled","skipped","failed"],"dryRun":true,"reason":"quickstart preview","limit":1000}' \
  "http://localhost:8080/admin/queues/wf.commands/retention/purge"
```

See [Semantics](semantics.md) for passable terminal status rules and audit behavior.

## Java REST Client Usage

```java
SequencedQueueClient client = SequencedQueueClient.builder()
    .baseUrl("http://localhost:8080")
    .apiKey("dev-key")
    .build();

client.enqueue("wf.commands", SequencedQueueClient.EnqueueRequest.of(
    "customer-123",
    "example.command",
    Map.of("action", "sync")
));
```

The REST client is a thin HTTP wrapper. It does not know PostgreSQL and does not implement queue state transitions.

## Java Direct Client Usage

```java
PGSimpleDataSource dataSource = new PGSimpleDataSource();
dataSource.setURL("jdbc:postgresql://localhost:5432/sequenced_queue");
dataSource.setUser("sequenced_queue");
dataSource.setPassword("sequenced_queue");

SequencedQueueDirectClient client = SequencedQueueDirectClient.builder()
    .dataSource(dataSource)
    .validateSchemaOnBuild(true)
    .build();
```

The direct client delegates to `sequenced-queue-core`. For the current release, it requires schema baseline `V1`. If `validateSchemaOnBuild(true)` is enabled and the current Flyway schema baseline is not `V1`, the builder throws `QueueUnavailableException` and the client is not created.

Oversized direct-client fields are rejected by core as `QueueFieldTooLargeException`, with `fieldName`, `maxBytes`, and `actualBytes` available without exposing the oversized content.

See [Versioning](versioning.md) for schema compatibility policy and [Security](security.md) for least-privilege database role guidance.

For long-running direct workers, prefer the helper:

```java
SequencedQueueDirectWorker worker = client.worker("wf.commands")
    .workerId("direct-worker-1")
    .leaseSeconds(30)
    .handler("example.command", item ->
        DirectQueueResult.success(Map.of("handledBy", "direct-worker-1")))
    .build();

worker.runOnce();
worker.runForever();
worker.stop();
```

## Python REST Client Usage

```python
client = SequencedQueueClient("http://localhost:8080", api_key="dev-key")
client.enqueue(
    "wf.commands",
    source_id="customer-123",
    item_type="example.command",
    payload={"action": "sync"},
)
```

The Python client is also REST-only. It does not access PostgreSQL directly. If the server returns `FIELD_TOO_LARGE`, the client preserves the structured response details on the raised exception.

## Worker Lifecycle

Workers repeatedly:

1. Claim one available head item for one source.
2. Start heartbeat for the source/item lease.
3. Run the registered handler.
4. Complete on success or fail on error.
5. Stop heartbeat after the handler finishes.

Empty claims use backoff so idle workers do not spin aggressively.

REST and direct Java workers expose `runOnce()` for tests, examples, and deterministic operational checks. `runOnce()` returns `true` after handling one claimed item and `false` when no item is available.

## Heartbeat and Lease-Lost Behaviour

Workers heartbeat approximately halfway through the configured lease duration. If heartbeat fails, the worker marks the lease as lost and does not complete or fail the item after the handler returns. This prevents stale workers from overwriting recovery or another worker's later claim.

Expired leases are recovered by the server recovery task. Recovery moves items to `retry_wait` or `dead_lettered` according to attempt count.

## Retryable vs Non-Retryable Errors

Java REST workers use `QueueResult`:

```java
return QueueResult.success(Map.of("ok", true));
return QueueResult.retryableFailure("TEMPORARY_ERROR", "try later");
return QueueResult.failure("VALIDATION_ERROR", "bad input");
```

Python workers use exceptions:

```python
raise RetryableQueueError("try later")
raise ValueError("bad input")
```

Retryable failures become `retry_wait` until `maxAttempts` is exhausted. Exhausted retryable failures become `dead_lettered` and block the source. Non-retryable failures become `failed` and do not block later source items.
