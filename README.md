# sequenced-queue

`sequenced-queue` is a lightweight PostgreSQL-backed durable work queue with strict per-source ordering.

The central invariant is:

```text
For a given (queueName, sourceId), queue items are processed in sequence order and are not processed concurrently.
```

Items for different `sourceId` values may be processed concurrently.

The queue provides durable **at-least-once** delivery and strict per-source processing order. It does **not** provide exactly-once side effects. Consumers must use idempotent handlers.

## Current release status

```text
Stage 0 - Correctness Foundation: passed
Stage 1 - Operational Readiness Baseline: passed
Stage 2 - Developer Experience Baseline: passed

v0.1.0 - initial MVP release
v0.1.1 - corrected MVP package-boundary release
```

The current recommended baseline is:

```text
v0.1.1
```

## Distribution model

The repository contains both the MVP-supported direct Java distribution and broader post-MVP product surfaces.

### MVP-supported packages

```text
Package 1: sequenced-queue-core
Package 2: sequenced-queue-java-direct-client
Support dependency: sequenced-queue-worker-core
```

The MVP support boundary is **core schema + direct Java/PostgreSQL API**.

This is the intended path for trusted internal Java deployments, including `wf`-style workflow command/event processing.

### Post-MVP / broader product surfaces

The following components remain in the repository and are kept buildable/testable, but they are outside the MVP Package 1/Package 2 support boundary:

```text
sequenced-queue-server
sequenced-queue-java-client
sequenced-queue-python-client
docs/openapi.yaml
Docker server packaging
REST examples
Python examples
```

All supported access paths must continue to delegate queue semantics to `sequenced-queue-core`.

---

## Modules

- `sequenced-queue-core`: core schema and queue semantics package. It contains the PostgreSQL `V1` schema baseline, production SQL/state transitions, transaction abstraction, source leases, item leases, retries, dead-letter handling, admin repair semantics, schema metadata, validation, and PostgreSQL contract tests.
- `sequenced-queue-worker-core`: shared Java worker loop used by worker helpers. It has no PostgreSQL or REST transport implementation. It is a support dependency for the MVP direct Java API.
- `clients/java-direct`: trusted/internal direct Java/PostgreSQL adapter. Its Maven artifact is `sequenced-queue-java-direct-client`. It uses a caller-provided `DataSource`, delegates to `sequenced-queue-core`, and does not require the REST server.
- `sequenced-queue-server`: Spring Boot HTTP adapter over `sequenced-queue-core`, with API key filter, Actuator metrics/health, and OpenAPI surface. This is a post-MVP/full-distribution surface.
- `sequenced-queue-java-client`: Java HTTP client and polling worker helper for the REST API. This is a post-MVP/full-distribution surface.
- `sequenced-queue-python-client`: Python HTTP client and polling worker helper for the REST API. This is a post-MVP/full-distribution surface.
- `docs/openapi.yaml`: HTTP API description for the REST server surface.
- `examples`: runnable examples for Java/Python/direct usage, depending on the example.

---

## Documentation

- [Developer Quickstart](docs/developer_quickstart.md)
- [Core Schema Package](docs/core-schema.md)
- [Distributions](docs/distributions.md)
- [Versioning and Schema Compatibility](docs/versioning.md)
- [Canonical Semantics](docs/semantics.md)
- [Security and Database Privileges](docs/security.md)
- [Changelog](CHANGELOG.md)
- [Release Checklist](RELEASE_CHECKLIST.md)
- [Known Issues and Simplification Follow-ups](docs/ISSUES.md)
- [Simplification Strategy](docs/simplification_strategy.md)
- [OpenAPI Contract](docs/openapi.yaml)
- [Examples README](examples/README.md)
- [Product Roadmap](docs/sequenced_queue_product_roadmap.md)
- [Decision Log](docs/DECISIONS.md)

---

## MVP Support Boundary

### Package 1 — Core with schema

`sequenced-queue-core` is the source of queue correctness for the MVP.

It owns:

```text
PostgreSQL V1 schema baseline
production queue SQL
queue domain model
source ordering
source leases
item leases
status transitions
retry/dead-letter behaviour
admin repair behaviour
manual retention purge semantics
global validation and size limits
stable core error codes
FIELD_TOO_LARGE validation
schema metadata
PostgreSQL contract tests
```

It is the only package that owns production queue state-transition SQL.

### Package 2 — Direct Java API

The trusted direct Java/PostgreSQL API in `clients/java-direct` is the primary MVP access path for internal Java deployments.

Its Maven artifact is:

```text
sequenced-queue-java-direct-client
```

It:

```text
delegates to sequenced-queue-core
uses a caller-provided DataSource
does not require the REST server
bypasses REST API-key security
should use a least-privilege database role
can validate schema compatibility at startup
supports direct Java producer, worker, recovery, and admin operations
```

`sequenced-queue-worker-core` is part of the MVP dependency graph as a support dependency, but MVP users normally interact with `sequenced-queue-java-direct-client` rather than using worker-core directly.

### Post-MVP product surfaces

The REST server, OpenAPI document, Docker server packaging, Java REST client, Python REST client, REST examples, Python examples, and operational HTTP docs remain in the repository, but they are outside the MVP Package 1/Package 2 support boundary.

---

## Schema lifecycle

The queue schema is persistent infrastructure. Applications should not create, drop, or reset it on every run.

Correct lifecycle:

```text
deployment/setup:
  install the V1 schema once

normal application startup:
  validate that the schema exists and is compatible

normal runtime:
  enqueue, claim, complete, fail, heartbeat, recover, repair

application shutdown/restart:
  queue tables and queued data remain in PostgreSQL
```

The direct Java client should validate schema compatibility. It should not create, migrate, drop, or reset queue tables during normal operation.

For the current release, the schema baseline is:

```text
V1
```

There is no current `V2`/`V3`/`V4` migration chain.

### Installing the schema with `psql`

A simple deployment can install the packaged SQL schema with `psql`:

```sh
psql \
  -h localhost \
  -U wf_admin \
  -d wf_db \
  -v ON_ERROR_STOP=1 \
  -f V1__initial_queue_schema.sql
```

Do not put production passwords directly on the command line. Prefer `~/.pgpass`, environment-managed secrets, CI/CD secrets, or a deployment secret manager.

A typical role split is:

```text
wf_admin:
  installs or upgrades schema

wf_app:
  runs wf/runtime/workers
  reads and writes queue data
  does not run DDL
```

### Installing the schema with Flyway

If Flyway is used for deployment, apply the core migration from the packaged classpath location:

```java
Flyway.configure()
    .dataSource(adminDataSource)
    .locations("classpath:db/migration")
    .load()
    .migrate();
```

This should be deployment/migration code, not ordinary direct-client startup behaviour.

---

## Direct Java quick start

Use a pooled `DataSource`, such as HikariCP or an application-managed container pool.

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/wf_db");
config.setUsername("wf_app");
config.setPassword(System.getenv("WF_DB_PASSWORD"));
config.setMaximumPoolSize(10);

DataSource dataSource = new HikariDataSource(config);
```

Build the direct client with schema validation enabled:

```java
SequencedQueueDirectClient client = SequencedQueueDirectClient.builder()
    .dataSource(dataSource)
    .validateSchemaOnBuild(true)
    .build();
```

Expected startup behaviour:

```text
schema exists and is compatible: client starts
schema missing: client fails fast
schema incompatible: client fails fast
```

The direct client does not create a production database pool and does not require the REST server.

---

## wf-style command/event usage

For workflow-style command/event work, use queue names such as:

```text
wf.commands
wf.events
```

Recommended first choice:

```text
queueName = wf.commands
sourceId  = workflowInstanceId
itemType  = wf.command
```

This gives:

```text
same workflow instance = sequential processing
different workflow instances = concurrent processing
```

If independent workflow threads may safely run concurrently, use:

```text
sourceId = workflowInstanceId:threadId
```

Example enqueue:

```java
client.enqueue("wf.commands", EnqueueRequest.builder()
    .sourceId(workflowInstanceId)
    .itemType("wf.command")
    .idempotencyKey(commandId)
    .payloadJson("""
        {"commandName":"SendEmail","workflowInstanceId":"%s","threadId":"_main"}
        """.formatted(workflowInstanceId))
    .headersJson("""
        {"correlationId":"%s"}
        """.formatted(correlationId))
    .maxAttempts(5)
    .build());
```

Example worker:

```java
SequencedQueueDirectWorker worker = client.worker("wf.commands")
    .workerId("wf-command-worker-1")
    .handler("wf.command", item -> {
        // Execute the wf command idempotently.
        return DirectQueueResult.success(Map.of("ok", true));
    })
    .build();

worker.runForever();
```

The direct worker helper uses short core transactions for claim and complete/fail. User handler code runs outside database transactions.

Handlers must be idempotent because delivery is at-least-once. For external side effects, use a stable command id or effect id to detect duplicate processing after worker crash/recovery.

---

## Local development and verification

Start PostgreSQL:

```sh
docker compose up -d postgres
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

---

## REST server local run

The REST server is a post-MVP/full-distribution surface.

Run the server locally:

```sh
./mvnw -pl sequenced-queue-server spring-boot:run
```

Worker endpoints live under `/queues/{queueName}` and require:

```text
Authorization: Bearer <sequenced-queue.api-key>
```

Admin endpoints live under `/admin/queues/{queueName}` and require:

```text
Authorization: Bearer <sequenced-queue.admin-api-key>
```

The admin API key can also call worker endpoints. The server fails startup if the worker and admin API keys are equal.

This is a simple static/configured API key model. It is not OAuth/OIDC, full API-key lifecycle management, key rotation, hashed key storage, or production-grade identity management.

---

## Docker server image

The Docker server image is a post-MVP/full-distribution surface.

Build the server image from the repository root:

```sh
docker build -t sequenced-queue-server:0.1.1 -f sequenced-queue-server/Dockerfile .
```

Run it against an existing PostgreSQL database:

```sh
docker run --rm -p 8080:8080 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/sequenced_queue \
  -e SPRING_DATASOURCE_USERNAME=sequenced_queue \
  -e SPRING_DATASOURCE_PASSWORD=sequenced_queue \
  -e SEQUENCED_QUEUE_API_KEY=replace-worker-key \
  -e SEQUENCED_QUEUE_ADMIN_API_KEY=replace-admin-key \
  sequenced-queue-server:0.1.1
```

The server also supports the local development aliases `DATABASE_URL`, `DATABASE_USERNAME`, and `DATABASE_PASSWORD` through `application.yml`; the `SPRING_DATASOURCE_*` names are the standard Spring Boot overrides.

---

## Delivery semantics

- At-least-once delivery.
- No exactly-once side effects.
- No global ordering across different sources.
- No broker/pub-sub/fanout semantics.
- Idempotent enqueue when `idempotencyKey` is supplied.
- Worker leases with heartbeat.
- Dead-lettered head items block their source until admin repair.

Consumers must use idempotent handlers because worker crashes can cause duplicate delivery.

The canonical semantics document is [docs/semantics.md](docs/semantics.md).

---

## Lease and retry model

Claiming work leases one source and its current head item. A worker must complete, fail, or heartbeat with the matching `workerId` and `leaseId`.

Retryable failures become `retry_wait` until attempts are exhausted. Exhausted retryable failures become `dead_lettered` and block the source.

Non-retryable failures become `failed`. `failed`, `skipped`, `cancelled`, and `succeeded` are passable terminal states.

Successful admin repair operations are written to `queue_admin_audit`. See [docs/semantics.md](docs/semantics.md) for canonical retry, lease, recovery, and admin repair details.

Manual retention purge deletes only old passable terminal rows:

```text
succeeded
cancelled
skipped
failed
```

It never purges:

```text
pending
processing
retry_wait
dead_lettered
```

Purge requests are bounded by `limit`, defaulting to `1000` and capped by `sequenced-queue.max-retention-purge-batch-size`.

---

## Global queue settings

Queue configuration is global-only. There is no `queue_config` table or queue-level database policy model.

```text
sequenced-queue.default-lease-seconds=60
sequenced-queue.max-lease-seconds=600
sequenced-queue.default-max-attempts=5
sequenced-queue.max-payload-bytes=262144
sequenced-queue.max-headers-bytes=32768
sequenced-queue.max-result-bytes=262144
sequenced-queue.max-error-type-bytes=256
sequenced-queue.max-error-message-bytes=8192
sequenced-queue.max-admin-reason-bytes=2048
sequenced-queue.max-admin-metadata-bytes=32768
sequenced-queue.max-retention-purge-batch-size=10000
```

These limits are enforced in `sequenced-queue-core`, not only by REST controllers.

Oversized content is not echoed in REST error responses, direct Java exceptions, or application logs.

Oversized fields use `FIELD_TOO_LARGE` and include safe byte counts.

REST example:

```json
{
  "errorCode": "FIELD_TOO_LARGE",
  "message": "payload exceeds configured size limit",
  "fieldName": "payload",
  "maxBytes": 262144,
  "actualBytes": 300112,
  "details": {}
}
```

Known error codes include:

```text
VALIDATION_ERROR
FIELD_TOO_LARGE
ITEM_NOT_FOUND
SOURCE_NOT_FOUND
SOURCE_BLOCKED
LEASE_LOST
LEASE_EXPIRED
ITEM_NOT_PROCESSING
IDEMPOTENCY_CONFLICT
UNAUTHORIZED
FORBIDDEN
QUEUE_CONFLICT
INTERNAL_ERROR
```

---

## Operational HTTP endpoints

Operational HTTP endpoints are part of the REST server/full-distribution surface.

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

---

## Direct Java client

The direct Java client is for trusted internal Java deployments that can safely talk to PostgreSQL without going through the REST server.

It delegates to `sequenced-queue-core`, so it shares the same PostgreSQL SQL implementation and queue semantics as the REST server path. It bypasses API-layer security and should use a least-privilege database role.

The database must already have the `sequenced-queue-core` schema baseline installed. For the current release, that baseline is `V1`.

Trusted deployments can call `getSchemaInfo()` or enable `validateSchemaOnBuild(true)` on the direct client builder to fail fast on missing or incompatible schema.

Current support:

```text
enqueue
claim
complete
fail
heartbeat
expired-lease recovery
blocked-source inspection
admin retry
admin skip
admin cancel
admin unblock
manual retention purge
idempotency handling
per-source sequence assignment
schema version lookup
```

Oversized direct-client fields are rejected as `QueueFieldTooLargeException`, which exposes `fieldName`, `maxBytes`, and `actualBytes` without exposing the oversized content.

The direct worker helper uses short core transactions for claim and complete/fail. User handler code runs outside database transactions.

---

## Python REST client

The Python package in `sequenced-queue-python-client` is an HTTP client and worker helper. It uses the REST API and does not access PostgreSQL directly.

If the server returns `FIELD_TOO_LARGE`, the Python client preserves the structured error details on `QueueClientError`.

Run its tests with:

```sh
cd sequenced-queue-python-client
python -m pytest
```

---

## Non-goals for this release

`sequenced-queue` is not:

```text
a Kafka replacement
a RabbitMQ replacement
a general pub/sub broker
a fanout/event bus
a global-ordering queue
an exactly-once side-effect executor
a workflow engine
```

The intended niche is durable source-ordered work dispatch.
