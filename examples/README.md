# sequenced-queue Examples

These examples are intentionally small and runnable from the repository root.

## Java REST Producer

```sh
./mvnw -pl examples/java-producer exec:java
```

Enqueues one `example.command` item through the REST API.

## Java REST Worker

```sh
./mvnw -pl examples/java-rest-worker exec:java
```

Runs a polling REST worker with automatic heartbeat and lease-lost handling.

## Java Direct Worker

```sh
./mvnw -pl examples/java-direct-worker exec:java
```

Runs a trusted direct PostgreSQL worker using the shared core implementation. The core Flyway schema must already be applied, and the example validates schema compatibility on startup.

## Python REST Producer

```sh
PYTHONPATH=sequenced-queue-python-client python examples/python-producer/producer.py
```

Enqueues one `example.command` item through the REST API.

## Python REST Worker

```sh
PYTHONPATH=sequenced-queue-python-client python examples/python-worker/worker.py
```

Runs a polling REST worker with automatic heartbeat and lease-lost handling.

## Common Environment Variables

```text
SQ_BASE_URL=http://localhost:8080
SQ_API_KEY=dev-key
SQ_QUEUE=wf.commands
SQ_SOURCE_ID=example-source
SQ_ITEM_TYPE=example.command
SQ_WORKER_ID=example-worker
SQ_LEASE_SECONDS=30
DATABASE_URL=jdbc:postgresql://localhost:5432/sequenced_queue
DATABASE_USERNAME=sequenced_queue
DATABASE_PASSWORD=sequenced_queue
```
