# sequenced-queue

`sequenced-queue` is a PostgreSQL-backed durable work queue with strict per-source ordering.

Invariant:

```text
For a given (queueName, sourceId), queue items are processed in sequence order and are not processed concurrently.
```

Items for different `sourceId` values may be processed concurrently.

## Modules

- `sequenced-queue-core`: shared queue implementation, Flyway schema migration, plain JDBC PostgreSQL repository, transaction abstraction, leases, retries, dead-letter handling, and admin repair semantics.
- `sequenced-queue-server`: Spring Boot HTTP adapter and OpenAPI surface over `sequenced-queue-core`.
- `sequenced-queue-java-client`: Java HTTP client and polling worker helper.
- `clients/java-direct`: trusted/internal Java PostgreSQL adapter that uses the same `sequenced-queue-core` implementation through a caller-provided `DataSource`.
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

Run the required PostgreSQL contract suite with Docker-backed Testcontainers:

```sh
mvn verify -Ppostgres-contract
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

## Direct Java Client

The direct Java client is for trusted internal Java deployments that can safely talk to PostgreSQL without going through the REST server. It delegates to `sequenced-queue-core`, so it shares the same PostgreSQL SQL implementation and queue semantics as the REST server path. It bypasses API-layer security and should use a least-privilege database role.

The database must already have the `sequenced-queue-core` Flyway migration applied. This client version supports schema migration `V1`; trusted deployments can call `getSchemaInfo()` or enable `validateSchemaOnBuild(true)` on the direct client builder to fail fast on missing or incompatible schema.

Current support: enqueue, claim, complete, fail, heartbeat, expired-lease recovery, blocked-source inspection, admin retry/skip/cancel/unblock, idempotency handling, per-source sequence assignment, and schema version lookup.
