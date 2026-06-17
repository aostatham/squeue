# Versioning and Schema Compatibility

The current MVP release is `0.1.0`.

For the current MVP release, the database schema baseline is `V1`.

## MVP Support Boundary

Package 1 is `sequenced-queue-core`. It is the core-with-schema package and owns the PostgreSQL `V1` schema baseline, production queue SQL, queue semantics, schema metadata, validation, and contract-tested behavior.

Package 2 is the trusted direct Java/PostgreSQL API in `clients/java-direct`. It delegates to `sequenced-queue-core` and is the primary MVP access path for trusted internal Java/wf deployments.

`sequenced-queue-worker-core` is a shared support artifact used by the direct Java worker helper. It is part of the MVP dependency graph, but MVP users normally interact with the direct Java API rather than using worker-core directly.

The REST server, OpenAPI document, Docker server packaging, Java REST client, Python REST client, and examples remain in the repository but are post-MVP product surfaces outside the Package 1/Package 2 support boundary.

The REST server and trusted direct Java client read Flyway schema history through `sequenced-queue-core`.

The direct Java client validates that the database schema is compatible with the `V1` baseline. When `validateSchemaOnBuild(true)` is enabled, the direct client builder fails fast if the current schema baseline is missing or incompatible.

## Artifacts

Maven artifacts currently use groupId `com.jdansoft` and version `0.1.0`.

| Module path | Maven artifactId | Notes |
| --- | --- | --- |
| `sequenced-queue-core` | `sequenced-queue-core` | Shared queue implementation, production SQL, Flyway migrations, and schema compatibility checks. |
| `sequenced-queue-worker-core` | `sequenced-queue-worker-core` | Internal Java worker loop shared by REST and direct Java worker helpers; contains no SQL or transport implementation. |
| `sequenced-queue-server` | `sequenced-queue-server` | Spring Boot REST API and operational adapter. |
| `sequenced-queue-java-client` | `sequenced-queue-java-client` | Java REST client and worker helper. |
| `clients/java-direct` | `sequenced-queue-java-direct-client` | Trusted direct Java/PostgreSQL client backed by `sequenced-queue-core`. |
| `examples/java-producer` | `sequenced-queue-example-java-producer` | Runnable Java REST producer example. |
| `examples/java-rest-worker` | `sequenced-queue-example-java-rest-worker` | Runnable Java REST worker example. |
| `examples/java-direct-worker` | `sequenced-queue-example-java-direct-worker` | Runnable trusted direct Java/PostgreSQL worker example. |

The Python package lives in `sequenced-queue-python-client`, uses package name `sequenced-queue`, and uses PEP 440 version `0.1.0` for this release.

The Maven groupId, license, SCM URL, project URL, developer, and organization metadata should be finalized before any public artifact publication. They are intentionally not filled with placeholder public URLs.

The Docker image tag used in local documentation is `sequenced-queue-server:0.1.0`.

## Schema Compatibility

The project currently has a single pre-release schema baseline: `V1`. Pre-release development migrations have been collapsed into that baseline because there are no live production databases that require in-place upgrades.

Post-go-live schema migrations will be introduced only after a released schema must be upgraded in place. The future migration policy is tracked in [ISSUES](ISSUES.md).

Schema compatibility is about queue storage semantics, not API-key authentication. The direct Java client bypasses REST API-key security and should use a least-privilege database role.
