# sequenced-queue

`sequenced-queue` is a PostgreSQL-backed durable work queue with strict per-source ordering.

Invariant:

```text
For a given (queueName, sourceId), queue items are processed in sequence order and are not processed concurrently.
```

Items for different `sourceId` values may be processed concurrently.

## Modules

- `sequenced-queue-server`: Spring Boot HTTP server, PostgreSQL persistence, Flyway schema, leases, retries, dead-letter handling, and admin repair endpoints.
- `sequenced-queue-java-client`: Java HTTP client and polling worker helper.
- `clients/java-postgres`: trusted/internal Java PostgreSQL client for direct DataSource-based enqueue access.
- `sequenced-queue-python-client`: Python HTTP client and polling worker helper.
- `docs/openapi.yaml`: MVP HTTP API description.

## Local Run

Start PostgreSQL:

```sh
docker compose up -d postgres
```

Run the server:

```sh
mvn -pl sequenced-queue-server spring-boot:run
```

Run Java tests:

```sh
mvn test
```

Run Python client tests:

```sh
cd sequenced-queue-python-client
python -m pytest
```

## Delivery Semantics

- At-least-once delivery.
- Idempotent enqueue when `idempotencyKey` is supplied.
- Worker leases with heartbeat.
- Expired lease recovery moves processing work to `retry_wait` or `dead_lettered`.
- Dead-lettered head items block their source until admin retry, skip, or cancel.

Consumers must use idempotent handlers because worker crashes can cause duplicate delivery.

## Direct Java PostgreSQL Client

The direct Java PostgreSQL client is for trusted internal Java deployments that can safely talk to PostgreSQL without going through the REST server. It uses the same schema and enqueue semantics as the server path, but bypasses API-layer security and should use a least-privilege database role.

Current support: `enqueue(queueName, EnqueueRequest)`, idempotency handling, per-source sequence assignment, and schema version lookup. This client version supports schema migration `V1`.
