# Developer Quickstart

This guide shows the Stage 2 developer path: start PostgreSQL, run the server, enqueue work, run Java and Python workers, and inspect operational state.

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
PYTHONPATH=sequenced-queue-python-client python examples/python-producer/producer.py
```

Both examples enqueue item type `example.command` into queue `wf.commands` for source `example-source` unless environment variables override the defaults.

## Run a Java REST Worker

```sh
./mvnw -pl examples/java-rest-worker exec:java
```

The Java REST worker uses `SequencedQueueWorker`, claims one head item at a time, heartbeats halfway through the lease, completes successful handler results, and fails handler errors according to `QueueResult`.

## Run a Python REST Worker

```sh
PYTHONPATH=sequenced-queue-python-client python examples/python-worker/worker.py
```

The Python worker uses `SequencedQueueWorker`, starts a heartbeat thread for claimed work, completes successful handlers, treats `RetryableQueueError` as retryable, and treats other exceptions as non-retryable.

## Run a Java Direct Worker

The direct Java worker bypasses the REST server and talks to PostgreSQL through `sequenced-queue-core`.

```sh
./mvnw -pl examples/java-direct-worker exec:java
```

The direct worker requires the core Flyway schema to already be applied by the server or by your migration process. It enables `validateSchemaOnBuild(true)`, so startup fails fast if the schema is missing or incompatible.

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

The direct client delegates to `sequenced-queue-core`. It requires schema version `2`. If `validateSchemaOnBuild(true)` is enabled and the current Flyway schema version is not `2`, the builder throws `QueueUnavailableException` and the client is not created.

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

The Python client is also REST-only. It does not access PostgreSQL directly.

## Worker Lifecycle

Workers repeatedly:

1. Claim one available head item for one source.
2. Start heartbeat for the source/item lease.
3. Run the registered handler.
4. Complete on success or fail on error.
5. Stop heartbeat after the handler finishes.

Empty claims use backoff so idle workers do not spin aggressively.

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
