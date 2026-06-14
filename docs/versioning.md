# Versioning and Schema Compatibility

The intended release candidate is `0.1.0-rc1`.

For the current pre-release build, the database schema baseline is `V1`.

The REST server and trusted direct Java client read Flyway schema history through `sequenced-queue-core`.

The direct Java client validates that the database schema is compatible with the pre-release `V1` baseline. When `validateSchemaOnBuild(true)` is enabled, the direct client builder fails fast if the current schema baseline is missing or incompatible.

## Artifacts

Maven artifacts currently use groupId `com.example` and version `0.1.0-rc1`.

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

The Python package lives in `sequenced-queue-python-client`, uses package name `sequenced-queue`, and uses PEP 440 version `0.1.0rc1` for this release candidate.

The Maven groupId, license, SCM URL, project URL, developer, and organization metadata should be finalized before any public artifact publication. They are intentionally not filled with placeholder public URLs.

The Docker image tag used in local documentation is `sequenced-queue-server:0.1.0-rc1`.

## Schema Compatibility

The project currently has a single pre-release schema baseline: `V1`. Pre-release development migrations have been collapsed into that baseline because there are no live production databases that require in-place upgrades.

Post-go-live schema migrations will be introduced only after a released schema must be upgraded in place. The future migration policy is tracked in [ISSUES](ISSUES.md).

Schema compatibility is about queue storage semantics, not API-key authentication. The direct Java client bypasses REST API-key security and should use a least-privilege database role.
