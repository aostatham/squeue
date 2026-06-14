# sequenced-queue distributions

## Core Runtime

Includes:
- sequenced-queue-core
- sequenced-queue-worker-core
- sequenced-queue-server
- sequenced-queue-java-client
- clients/java-direct
- Java examples
- PostgreSQL contract tests

Required for:
- strict per-source ordered work dispatch
- Java REST workers
- trusted direct Java/PostgreSQL workers

`sequenced-queue-worker-core` is an internal Java worker-loop helper. It contains no production SQL, no REST transport, and no database access.

## Full Distribution

Includes Core Runtime plus:
- sequenced-queue-python-client
- Python examples
- extended docs
- operational/release docs

Deferred:
- CLI
- UI
- batching
- LISTEN/NOTIFY
- broker bridges
- queue_config table
- archive tables
