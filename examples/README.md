# sequenced-queue Examples

These examples are intentionally small and runnable from the repository root. They are developer examples, not production deployment templates.

For delivery guarantees and non-guarantees, see [Semantics](../docs/semantics.md).

From a clean checkout, build the local Java artifacts and install the Python client dependencies first:

```sh
./mvnw -DskipTests install
python -m pip install -e sequenced-queue-python-client
```

## Java REST Producer

```sh
./mvnw -pl examples/java-producer exec:java
```

Enqueues one `example.command` item through the REST API.

REST examples use the running server and `SQ_API_KEY`.

## Java REST Worker

```sh
SQ_RUN_ONCE=true ./mvnw -pl examples/java-rest-worker exec:java
```

Completes one item through the REST API. Omit `SQ_RUN_ONCE=true` to run as a polling worker with automatic heartbeat and lease-lost handling.

## Java Direct Worker

```sh
SQ_RUN_ONCE=true ./mvnw -pl examples/java-direct-worker exec:java
```

Completes one item through the trusted direct PostgreSQL client. Omit `SQ_RUN_ONCE=true` to run as a polling worker. The Java direct worker is trusted/internal, requires direct PostgreSQL access, and bypasses REST API-key security. The example uses `SequencedQueueDirectWorker`, which provides automatic heartbeat, lease-lost detection, complete/fail suppression after lease loss, graceful shutdown, and empty-queue backoff.

## Python REST Producer

```sh
python examples/python-producer/producer.py
```

Enqueues one `example.command` item through the REST API.

Python examples use the running server and `SQ_API_KEY`.

## Python REST Worker

```sh
SQ_RUN_ONCE=true python examples/python-worker/worker.py
```

Completes one item through the REST API. Omit `SQ_RUN_ONCE=true` to run as a polling worker with automatic heartbeat and lease-lost handling.

## Common Environment Variables

```text
SQ_BASE_URL=http://localhost:8080
SQ_API_KEY=dev-key
SQ_QUEUE=wf.commands
SQ_SOURCE_ID=example-source
SQ_ITEM_TYPE=example.command
SQ_WORKER_ID=example-worker
SQ_LEASE_SECONDS=30
SQ_RUN_ONCE=true
DATABASE_URL=jdbc:postgresql://localhost:5432/sequenced_queue
DATABASE_USERNAME=sequenced_queue
DATABASE_PASSWORD=sequenced_queue
```

## Test Coverage

Java examples compile as Maven reactor modules and have smoke tests for the producer/worker paths. Python examples compile through `sequenced-queue-python-client/tests/test_examples.py`.
