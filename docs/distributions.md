# sequenced-queue distributions

## MVP-Supported Packages

### Package 1 - Core With Schema

Includes:
- sequenced-queue-core
- PostgreSQL `V1` Flyway schema baseline
- production queue SQL
- core queue domain model
- transactional queue semantics
- schema metadata and compatibility model
- global validation, limits, and stable error codes
- PostgreSQL contract tests

Required for:
- strict per-source ordered work dispatch
- source and item lease correctness
- retry/dead-letter/admin repair semantics
- retention purge semantics
- direct Java schema compatibility validation

### Package 2 - Direct Java API

Includes:
- clients/java-direct
- trusted Java/PostgreSQL client API
- direct worker helper
- dependency on `sequenced-queue-core`
- dependency on `sequenced-queue-worker-core`

Required for:
- trusted direct Java/PostgreSQL deployments
- caller-provided `DataSource` integration
- direct Java access to the core-backed queue semantics

`sequenced-queue-worker-core` is an internal/shared support artifact used by the direct Java API worker helper. It is part of the MVP dependency graph, but it is not the primary user-facing package. MVP users normally interact with `clients/java-direct`.

## Repository Product Surfaces

Includes:
- sequenced-queue-server
- sequenced-queue-java-client
- Java examples
- PostgreSQL contract tests

These remain in the repository, but REST server packaging, OpenAPI, Docker server image, Java REST client, Python REST client, and examples are outside the MVP Package 1/Package 2 support boundary.

`sequenced-queue-worker-core` contains no production SQL, no REST transport, and no database access.

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
