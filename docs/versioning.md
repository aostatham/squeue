# Versioning and Schema Compatibility

The current queue schema version is `4`.

The REST server and trusted direct Java client read Flyway schema history through `sequenced-queue-core`.

The direct Java client requires schema version `4`. When `validateSchemaOnBuild(true)` is enabled, the direct client builder fails fast if the current schema version is missing or incompatible.

Migration policy:

- migrations should be additive and non-destructive by default
- destructive migrations require a documented migration plan
- compatibility windows must be explicit before a destructive or incompatible migration is introduced
- generated or published artifacts are not yet considered v1.0 stable

Schema compatibility is about queue storage semantics, not API-key authentication. The direct Java client bypasses REST API-key security and should use a least-privilege database role.
