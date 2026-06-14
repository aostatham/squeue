# Security and Database Privileges

This document describes the current security model and recommended PostgreSQL privileges.

## API Keys

The REST server uses static configured API keys:

- worker key for `/queues/**`
- admin key for `/admin/**`

The admin key may also call worker endpoints. The server rejects startup when the worker and admin keys are equal.

This is not OAuth/OIDC, dynamic API-key lifecycle management, hashed key storage, or identity-provider integration.

## REST Server Database Access

The REST server owns schema migration and queue operation execution. It needs database access for:

- Flyway migrations
- `queue_source_state`
- `queue_item`
- `queue_admin_audit`
- `flyway_schema_history`

In simple deployments, the server role can own the queue schema and run Flyway migrations.

## Direct Java Client Database Access

The direct Java client bypasses REST API-key security and talks to PostgreSQL through `sequenced-queue-core`. Use it only in trusted/internal deployments.

A direct worker role should:

- not have DDL privileges
- not own the schema
- not have access to unrelated application tables
- have only the DML/select permissions required for queue operation
- have `SELECT` on `flyway_schema_history` if schema validation is enabled

Tenant isolation is not fully enforced by this project. If tenant isolation is required, add deployment-level controls or wait for explicit tenant permission work.

## Example Restricted Direct Worker Role

Adjust database, schema, and role names for your environment.

```sql
CREATE ROLE sequenced_queue_direct_worker LOGIN PASSWORD 'change-me';

GRANT USAGE ON SCHEMA public TO sequenced_queue_direct_worker;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE queue_item, queue_source_state
TO sequenced_queue_direct_worker;

GRANT SELECT, INSERT
ON TABLE queue_admin_audit
TO sequenced_queue_direct_worker;

GRANT SELECT
ON TABLE flyway_schema_history
TO sequenced_queue_direct_worker;
```

Do not grant DDL privileges such as:

```sql
-- Do not grant these to direct worker roles.
-- GRANT CREATE ON SCHEMA public TO sequenced_queue_direct_worker;
-- ALTER TABLE queue_item OWNER TO sequenced_queue_direct_worker;
```

Run Flyway migrations with the REST server role or a separate migration role, not with a direct worker role.
