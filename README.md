# sequenced-queue

`sequenced-queue` is a lightweight PostgreSQL-backed durable work queue with strict per-source ordering and REST, Java, Python, and trusted direct Java/PostgreSQL access paths.

The core product is the Core Runtime.
The Full Distribution adds optional clients/examples/docs.
Both use the same core semantics.

Invariant:

```text
For a given (queueName, sourceId), queue items are processed in sequence order and are not processed concurrently.
```

Items for different `sourceId` values may be processed concurrently.

The queue guarantees durable at-least-once delivery and strict per-source processing order.

It does not guarantee exactly-once side effects. Handlers must be idempotent.

Project status:

```text
Stage 0 - Correctness Foundation: passed
Stage 1 - Operational Readiness Baseline: passed
Stage 2 - Developer Experience Baseline: passed
Current focus - v0.1.0-rc1 release readiness
```

## Modules

- `sequenced-queue-core`: shared queue implementation, Flyway schema migration, plain JDBC PostgreSQL repository, transaction abstraction, leases, retries, dead-letter handling, and admin repair semantics.
- `sequenced-queue-worker-core`: internal Java worker loop shared by the Java REST and trusted direct Java worker helpers. It has no PostgreSQL or REST transport implementation.
- `sequenced-queue-server`: Spring Boot HTTP adapter, API key filter, Actuator metrics/health, and OpenAPI surface over `sequenced-queue-core`.
- `sequenced-queue-java-client`: Java HTTP client and polling worker helper.
- `clients/java-direct`: trusted/internal Java PostgreSQL adapter that uses the same `sequenced-queue-core` implementation through a caller-provided `DataSource`.
- `sequenced-queue-python-client`: Python HTTP client and polling worker helper.
- `docs/openapi.yaml`: MVP HTTP API description.
- `examples`: runnable Java and Python producer/worker examples.

## Documentation

- [Developer Quickstart](docs/developer_quickstart.md)
- [Changelog](CHANGELOG.md)
- [Release Checklist](RELEASE_CHECKLIST.md)
- [Canonical Semantics](docs/semantics.md)
- [Simplification Strategy](docs/simplification_strategy.md)
- [Known Issues and Simplification Follow-ups](docs/ISSUES.md)
- [Security and Database Privileges](docs/security.md)
- [Versioning and Schema Compatibility](docs/versioning.md)
- [OpenAPI Contract](docs/openapi.yaml)
- [Examples README](examples/README.md)
- [Product Roadmap](docs/sequenced_queue_product_roadmap.md)
- [Decision Log](docs/DECISIONS.md)

## Local Run

Start PostgreSQL:

```sh
docker compose up -d postgres
```

Run the server:

```sh
./mvnw -pl sequenced-queue-server spring-boot:run
```

Run Java tests:

```sh
./mvnw test
```

Run the required PostgreSQL contract suite with Docker-backed Testcontainers:

```sh
./mvnw verify -Ppostgres-contract
```

Run developer-facing examples/OpenAPI/client contracts:

```sh
./mvnw verify -Pdeveloper-contract
```

Run both contract sets:

```sh
./mvnw verify -Pfull-contract
```

Run Python client tests:

```sh
cd sequenced-queue-python-client
python -m pytest
```

For a runnable producer/worker walkthrough, see [Developer Quickstart](docs/developer_quickstart.md).

## Docker Image

Build the server image from the repository root:

```sh
docker build -f sequenced-queue-server/Dockerfile -t sequenced-queue-server:0.1.0-rc1 .
```

Run it against an existing PostgreSQL database:

```sh
docker run --rm -p 8080:8080 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/sequenced_queue \
  -e SPRING_DATASOURCE_USERNAME=sequenced_queue \
  -e SPRING_DATASOURCE_PASSWORD=sequenced_queue \
  -e SEQUENCED_QUEUE_API_KEY=replace-worker-key \
  -e SEQUENCED_QUEUE_ADMIN_API_KEY=replace-admin-key \
  sequenced-queue-server:0.1.0-rc1
```

The server also supports the local development aliases `DATABASE_URL`, `DATABASE_USERNAME`, and `DATABASE_PASSWORD` through `application.yml`; the `SPRING_DATASOURCE_*` names are the standard Spring Boot overrides.

## Delivery Semantics

- At-least-once delivery.
- No exactly-once side effects.
- No global ordering across different sources.
- No broker/pub-sub/fanout semantics.
- Idempotent enqueue when `idempotencyKey` is supplied.
- Worker leases with heartbeat.
- Dead-lettered head items block their source until admin repair.

Consumers must use idempotent handlers because worker crashes can cause duplicate delivery.

The canonical semantics document is [docs/semantics.md](docs/semantics.md).

## Lease and Retry Model

Claiming work leases one source and its current head item. A worker must complete, fail, or heartbeat with the matching `workerId` and `leaseId`.

Retryable failures become `retry_wait` until attempts are exhausted. Exhausted retryable failures become `dead_lettered` and block the source. Non-retryable failures become `failed`; `failed`, `skipped`, `cancelled`, and `succeeded` are passable terminal states.

Successful admin repair operations are written to `queue_admin_audit`. See [docs/semantics.md](docs/semantics.md) for canonical retry, lease, recovery, and admin repair detail.

Manual retention purge is available through the admin API. It deletes only old passable terminal rows (`succeeded`, `cancelled`, `skipped`, `failed`) and never purges `pending`, `processing`, `retry_wait`, or `dead_lettered`. Purge requests are bounded by `limit`, defaulting to `1000` and capped by `sequenced-queue.max-retention-purge-batch-size`.

## REST API and Security

Worker endpoints live under `/queues/{queueName}` and require `Authorization: Bearer <sequenced-queue.api-key>`. The admin API key can also call worker endpoints.

Admin endpoints live under `/admin/queues/{queueName}` and require `Authorization: Bearer <sequenced-queue.admin-api-key>`. The server fails startup if the worker and admin API keys are equal. This is a simple static/configured API key model; it is not OAuth/OIDC, full API-key lifecycle management, key rotation, hashed key storage, or production-grade identity management.

Queue configuration is global-only. There is no `queue_config` table or queue-level database policy model.

Global queue settings:

```text
sequenced-queue.default-lease-seconds=60
sequenced-queue.max-lease-seconds=600
sequenced-queue.default-max-attempts=5
sequenced-queue.max-payload-bytes=262144
sequenced-queue.max-headers-bytes=65536
sequenced-queue.max-error-message-bytes=8192
sequenced-queue.max-admin-reason-bytes=2048
sequenced-queue.max-retention-purge-batch-size=10000
```

These limits are enforced in `sequenced-queue-core`, not only by REST controllers.

REST errors use a stable shape:

```json
{
  "errorCode": "LEASE_LOST",
  "message": "lease is not held by worker",
  "details": {}
}
```

Known error codes include `VALIDATION_ERROR`, `ITEM_NOT_FOUND`, `SOURCE_NOT_FOUND`, `SOURCE_BLOCKED`, `LEASE_LOST`, `LEASE_EXPIRED`, `ITEM_NOT_PROCESSING`, `IDEMPOTENCY_CONFLICT`, `UNAUTHORIZED`, `FORBIDDEN`, `QUEUE_CONFLICT`, and `INTERNAL_ERROR`.

## Operational Endpoints

Spring Boot Actuator exposes:

```text
GET /actuator/health
GET /actuator/metrics
GET /actuator/metrics/{metricName}
```

The queue health component checks database reachability, Flyway schema state, required queue tables, the admin audit table, and whether lease recovery is enabled.

Metrics include:

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
queue.admin.unblock.total
```

Admin inspection endpoints:

```text
GET /admin/queues/{queueName}/dead-lettered
GET /admin/queues/{queueName}/blocked-sources
GET /admin/queues/{queueName}/sources/{sourceId}/items
GET /admin/queues/{queueName}/items/{itemId}
GET /admin/queues/{queueName}/audit
```

List endpoints support `limit` and `offset`.

## Direct Java Client

The direct Java client is for trusted internal Java deployments that can safely talk to PostgreSQL without going through the REST server. It delegates to `sequenced-queue-core`, so it shares the same PostgreSQL SQL implementation and queue semantics as the REST server path. It bypasses API-layer security and should use a least-privilege database role.

The database must already have the `sequenced-queue-core` Flyway baseline applied. For the current pre-release build, the database schema baseline is `V1`; trusted deployments can call `getSchemaInfo()` or enable `validateSchemaOnBuild(true)` on the direct client builder to fail fast on missing or incompatible schema.

Current support: enqueue, claim, complete, fail, heartbeat, expired-lease recovery, blocked-source inspection, admin retry/skip/cancel/unblock, manual retention purge, idempotency handling, per-source sequence assignment, and schema version lookup.

## Python REST Client

The Python package in `sequenced-queue-python-client` is an HTTP client and worker helper. It uses the REST API and does not access PostgreSQL directly.

Run its tests with:

```sh
cd sequenced-queue-python-client
python -m pytest
```
