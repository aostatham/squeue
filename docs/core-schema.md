# Core Schema Package

`sequenced-queue-core` is Package 1 of the MVP support boundary: the core-with-schema package.

It owns the PostgreSQL queue schema, production SQL, queue state transitions, source ordering, source leases, item leases, retry/dead-letter behavior, admin repair behavior, retention purge semantics, schema metadata, core validation, global limits, and stable core error codes.

## Packaged Schema

The core artifact packages the Flyway schema baseline at:

```text
db/migration/V1__initial_queue_schema.sql
```

The current MVP schema baseline is `V1`. There is no current `V2`, `V3`, or `V4` migration chain. Post-go-live schema migrations are future work and will be introduced only when released deployments need in-place upgrades.

The `V1` baseline contains the current queue storage model:

- `queue_source_state`
- `queue_item`
- `queue_admin_audit`
- source and item lease fields
- status constraints
- idempotency and claim indexes
- dead-letter, lease-expiry, source-state, admin-audit, and retention-purge indexes

## Installing Schema

Applications using the trusted direct Java/PostgreSQL client should apply the core schema before using the direct client. The direct Java client can validate schema compatibility, but it does not run migrations automatically.

The intended installation route is Flyway with the core artifact on the application classpath:

```java
Flyway.configure()
    .dataSource(dataSource)
    .locations("classpath:db/migration")
    .load()
    .migrate();
```

After migration, direct Java deployments can call `getSchemaInfo()` or enable `validateSchemaOnBuild(true)` on the direct client builder to fail fast when the schema is missing or incompatible.

## Semantics

Core guarantees strict per-source ordering and source exclusivity: for a given `(queueName, sourceId)`, only the current head item can be processed, and the same source is not processed concurrently.

Items for different sources can be processed concurrently. Delivery is durable and at least once, not exactly-once side effects. Handlers must be idempotent.

A `dead_lettered` head item blocks its source. Admin repair is required to continue after a blocking dead-letter.

Oversized fields are rejected in core with `FIELD_TOO_LARGE` and safe `fieldName`, `maxBytes`, and `actualBytes` details.
