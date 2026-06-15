# sequenced-queue Semantics

This is the canonical semantics document for `sequenced-queue`.

## Product Purpose

`sequenced-queue` is a lightweight PostgreSQL-backed durable work queue for source-ordered work dispatch. It is intended for commands, side effects, integration jobs, and other work where the ordering key matters.

It is not a general broker, pub/sub system, stream processor, or fanout mechanism.

## Core Invariant

For each `(queueName, sourceId)`, queue items are processed in sequence order and are not processed concurrently.

Items for different `sourceId` values may be processed concurrently. There is no global ordering across different sources.

## Source Ordering

Each source has a monotonically increasing `sequence_no`. Enqueue assigns sequence numbers transactionally per `(queueName, sourceId)`.

Workers can only claim the current eligible head item for a source. Later items for that source are not claimable until earlier non-passable items are completed, skipped, cancelled, failed, retried, or repaired according to their state.

## Source Leases

A claim leases one source to one worker. The source lease prevents two workers from processing different items for the same source concurrently.

The source lease has:

- `leased_by`
- `lease_id`
- `lease_until`

The worker must present the matching lease identity when heartbeating, completing, or failing the item.

## Item Leases

The claimed item is also marked `processing` with the same lease identity. This ties the source lease and item lease together for completion, failure, heartbeat, and recovery.

## Head-Item Rule

Operations that affect source progression are constrained by the source head item. Admin retry, skip, and cancel operate on the blocking head item. Unblock succeeds only when no blocking head item remains.

## Status Semantics

Passable terminal statuses:

- `succeeded`
- `cancelled`
- `skipped`
- `failed`

Blocking or non-passable statuses:

- `pending`
- `processing`
- `retry_wait`
- `dead_lettered`

`failed` is a terminal passable non-retryable failure state. It records that a worker rejected the item without retrying, and later source items may proceed.

`dead_lettered` is a terminal blocking failure state. It records exhausted retry attempts or unrecoverable lease expiry according to retry policy, and it blocks the source until admin repair.

## Delivery Guarantee

The queue provides durable at-least-once delivery.

Workers and handlers must be idempotent. A worker can perform a side effect and crash before completion is recorded, allowing the item to be delivered again after lease recovery.

## Non-Guarantees

Do not assume:

- exactly-once side effects
- global ordering across different sources
- pub/sub semantics
- fanout
- broker consumer groups
- stream replay
- priority queues

## Idempotency Keys

Producers may provide an `idempotencyKey`. The queue enforces uniqueness per `(queueName, idempotencyKey)` when the key is present and returns the existing item for duplicate enqueue attempts.

Idempotency keys prevent duplicate enqueue records. They do not make worker side effects exactly-once.

## Retry Behaviour

Retryable worker failures become `retry_wait` until the item is available again. When attempts are exhausted, retryable failures become `dead_lettered` and block the source.

Non-retryable worker failures become `failed`. `failed` is passable and does not block later source items.

## Heartbeat and Lease Expiry

Workers heartbeat during processing to extend leases. If heartbeat fails, client worker helpers mark the lease as lost and suppress complete/fail calls after the handler returns.

Expired lease recovery can move `processing` work back to `retry_wait` or to `dead_lettered` according to attempt count and retry policy. Recovery must preserve the source-ordering invariant.

## Dead-Letter Blocking

A `dead_lettered` head item blocks its source. Later source items are not claimable until an admin action repairs or bypasses the blocking item.

Admin repair operations are:

- `retry`: move a dead-lettered head item to `retry_wait`
- `skip`: mark a pending, retry-wait, or dead-lettered head item `skipped`
- `cancel`: mark a pending, retry-wait, or dead-lettered head item `cancelled`
- `unblock`: release a blocked source only after no blocking head item remains

Successful admin repair operations are audited.

## Admin Audit

Admin audit is written only for successful admin mutations:

- `retry`
- `skip`
- `cancel`
- `unblock`
- retention `purge` when `dryRun` is false

Normal worker and producer operations do not write admin audit rows. This includes enqueue, claim, complete, fail, and heartbeat.

## Manual Retention Purge

Retention is manual only. There is no scheduler, archive table, or automatic retention policy.

`POST /admin/queues/{queueName}/retention/purge` deletes old passable terminal items by `updated_at`. `limit` defaults to `1000` and cannot exceed the global `maxRetentionPurgeBatchSize`.

Eligible statuses:

- `succeeded`
- `cancelled`
- `skipped`
- `failed`

Ineligible statuses:

- `pending`
- `processing`
- `retry_wait`
- `dead_lettered`

`dryRun: true` counts the same bounded eligibility set without deleting rows and does not write admin audit. Actual purge writes an admin audit row.

## Configuration Model

Queue configuration is global-only. There is no `queue_config` table and no queue-level database policy model.

Implemented global settings include default lease seconds, maximum lease seconds, default max attempts, request field limits, recovery enablement, and recovery cadence. Per-queue configuration is deferred.

API key management is config-only. The server supports a WORKER key for `/queues/**` and an ADMIN key for `/admin/**`; the ADMIN key can also call worker endpoints. Keys are not stored in the queue database, and OAuth/OIDC and full key lifecycle management are out of scope.

Current global request limits:

- `defaultLeaseSeconds`
- `maxLeaseSeconds`
- `defaultMaxAttempts`
- `maxPayloadBytes`
- `maxHeadersBytes`
- `maxResultBytes`
- `maxErrorTypeBytes`
- `maxErrorMessageBytes`
- `maxAdminReasonBytes`
- `maxAdminMetadataBytes`

Byte-size limits are measured as UTF-8 bytes after the same compact JSON or string serialization that core stores. Oversized fields fail in `sequenced-queue-core` with stable error code `FIELD_TOO_LARGE` and safe `fieldName`, `maxBytes`, and `actualBytes` details.

## Structured Logging Policy

Queue/server logs use safe event fields only:

- `queueName`
- `sourceId`
- `itemId`
- `sequenceNo`
- `itemType`
- `workerId`
- `operation`
- `status`
- `errorCode`
- `fieldName`
- `maxBytes`
- `actualBytes`
- retention `matched` and `deleted` counts

Logs must not include payload JSON, headers JSON, result JSON, error message content, admin reason content, admin metadata content, API key values, idempotency key values, raw SQL details, or full stack traces for expected validation/conflict paths.

## REST and Direct Java Access

The REST server delegates queue operations to `sequenced-queue-core`.

The trusted direct Java client also delegates to `sequenced-queue-core` and uses the same production SQL and semantics. It bypasses REST API-key security and is only for trusted/internal Java deployments with direct PostgreSQL access.

The direct Java client requires the core Flyway baseline to be applied. For the current pre-release build, the required schema baseline is `V1`, and `validateSchemaOnBuild(true)` fails fast when the schema is missing or incompatible.
