# Stage 3 Design A - Payload, Header, Result, and Error Size Limits

## Status

Accepted and implemented for the Stage 3A size-limit baseline.

This design targets Stage 3 production hardening after the `v0.1.0-rc2` correction path. It preserves the current pre-release schema baseline `V1`.

## Context

`sequenced-queue` stores queue payloads, headers, completion results, failure details, and admin audit data in PostgreSQL. These fields can grow independently of queue semantics and can create operational risk if clients accidentally or deliberately submit oversized values.

The product already has a release-candidate baseline:

- Stage 0 correctness foundation is complete.
- Stage 1 operational readiness is complete.
- Stage 2 developer experience baseline is complete.
- Simplification S1 is complete.
- The pre-release Flyway schema baseline is `V1`.

Stage 3 should harden the product for real internal production use without changing the queue protocol.

## Goals

- Prevent oversized payload, header, result, error, reason, and metadata values from entering supported queue paths.
- Keep REST and direct Java/PostgreSQL behavior semantically consistent.
- Make shared core validation authoritative.
- Return safe structured errors that do not echo oversized content.
- Preserve the current queue semantics and schema baseline.
- Keep limits globally configured for this stage.

## Non-Goals

- No `queue_config` table.
- No per-queue, tenant-specific, or endpoint-specific limits.
- No database-backed API key lifecycle.
- No OAuth/OIDC.
- No external blob storage, compression, encryption, streaming upload, or chunked payload design.
- No archive-table change.
- No schema migration beyond the current pre-release `V1` baseline.
- No change to source leases, head-item ordering, claim, complete, fail, heartbeat, recovery, admin repair, or retention purge semantics.

## Current Behaviour

Current code already has partial global size-limit behavior:

- `QueueSettings` contains `maxPayloadBytes`, `maxHeadersBytes`, `maxErrorMessageBytes`, and `maxAdminReasonBytes`.
- The Spring server maps those settings from `sequenced-queue.*` properties.
- `DefaultQueueService` serializes enqueue payload and headers through the persistence JSON path and enforces byte limits before insert.
- Failure `errorMessage` and admin reason strings are checked in core.
- The direct Java client delegates through core, so supported direct Java operations inherit the same core checks where those checks exist.

Current gaps:

- Completion `result_json` is not explicitly size-limited.
- Failure `last_error_type` is not explicitly size-limited.
- Admin audit `metadata_json` is not explicitly size-limited.
- Oversized-field errors currently use generic validation semantics rather than a stable `FIELD_TOO_LARGE` error code with `fieldName`, `maxBytes`, and `actualBytes`.
- Existing defaults set headers to `65536` bytes; this design recommends reviewing that default and lowering it to `32768` bytes unless compatibility concerns argue otherwise.

## Proposed Limits

Use global defaults, configurable per deployment:

| Field | Default | Applies to |
| --- | ---: | --- |
| `payload_json` | `262144` bytes, 256 KiB | Enqueue payload |
| `headers_json` | `32768` bytes, 32 KiB | Enqueue headers |
| `result_json` | `262144` bytes, 256 KiB | Completion result |
| `last_error_type` | `256` bytes | Failure error type |
| `last_error_message` | `8192` bytes, 8 KiB | Failure error message |
| Admin reason | `2048` bytes, 2 KiB | Admin retry, skip, cancel, unblock, retention purge |
| Admin `metadata_json` | `32768` bytes, 32 KiB | Internal admin audit metadata, where present |

These are configuration defaults, not permanent product constants. They are intentionally conservative:

- 256 KiB allows normal command payloads and handler results without making the queue a blob store.
- 32 KiB for headers is enough for operational metadata while discouraging payload-like data in headers.
- 8 KiB for error messages keeps diagnostics useful without storing stack traces or large handler output.
- 256 bytes for error type is enough for stable symbolic names.
- 2 KiB for admin reason supports human audit context without allowing large free-form content.
- 32 KiB for audit metadata supports structured internal metadata such as counts and parameters.

## Configuration Model

Recommended Spring server properties:

```yaml
sequenced-queue:
  max-payload-bytes: 262144
  max-headers-bytes: 32768
  max-result-bytes: 262144
  max-error-type-bytes: 256
  max-error-message-bytes: 8192
  max-admin-reason-bytes: 2048
  max-admin-metadata-bytes: 32768
```

Recommended core model:

```java
QueueLimits(
    int maxPayloadBytes,
    int maxHeadersBytes,
    int maxResultBytes,
    int maxErrorTypeBytes,
    int maxErrorMessageBytes,
    int maxAdminReasonBytes,
    int maxAdminMetadataBytes
)
```

`QueueSettings` can either embed `QueueLimits` or continue to expose flattened values. The important design requirement is that REST and direct Java both map configuration into the same core model.

Validation rules:

- Missing values use defaults.
- Zero or negative values fail startup or direct-client builder creation.
- Minimum allowed value for each limit is `1`.
- No hard maximum is required initially, but implementation may add a defensive upper bound if operationally needed.
- Server startup must fail fast on invalid configured values.
- Direct Java client builder/config creation must fail fast on invalid configured values.

The configuration applies to:

- REST server path.
- Java REST client only when optional local limits are configured.
- Python REST client only when optional local limits are configured.
- Direct Java client path through mandatory core validation.
- Server-side internal admin operations that write audit fields.

## Enforcement Points

Authoritative enforcement must live in shared core code.

Recommended enforcement:

- Enqueue: validate serialized `payload_json` and `headers_json` at the core service boundary before repository insert.
- Complete: validate serialized `result_json` at the core service boundary before repository update.
- Fail: validate `errorType` and `errorMessage` at the core service boundary before repository update.
- Admin repair: validate reason and audit metadata at the core service boundary before audit insert.
- Retention purge: validate reason and audit metadata at the core service boundary before audit insert.

REST DTO validation may reject obvious malformed requests early, but it must not be the source of truth for size limits.

Repository/database boundary checks are useful as defensive assertions but should not be the primary error surface.

## Size Measurement Rule

The canonical rule is:

Measure the UTF-8 byte length of the exact JSON or string representation that will be stored.

JSON values:

- Serialize using the same `ObjectMapper` and persistence path used by core before writing to PostgreSQL.
- Measure `serializedJson.getBytes(StandardCharsets.UTF_8).length`.
- Use compact JSON from the persistence serializer, not pretty-printed JSON.

Raw JSON strings:

- Validate as JSON where validation already exists.
- Measure the UTF-8 byte length of the string that will be stored after validation and normalization.

Plain text values:

- Measure `value.getBytes(StandardCharsets.UTF_8).length`.

Null and empty values:

- Null JSON maps default to `{}` where existing queue semantics already do that.
- Empty JSON objects count as the bytes of `{}`.
- Null optional plain text values pass size validation.
- Empty strings pass size validation.

Unicode:

- Limits are byte limits, not character limits.
- Multibyte characters count by their UTF-8 encoded size.

## REST API Behaviour

Oversized fields should return:

- HTTP status: `400 Bad Request`.
- Error code: `FIELD_TOO_LARGE`.
- No echoed oversized content.

Example:

```json
{
  "errorCode": "FIELD_TOO_LARGE",
  "message": "payload exceeds configured size limit",
  "queueName": "wf.commands",
  "fieldName": "payload",
  "maxBytes": 262144,
  "actualBytes": 300112
}
```

REST response fields:

- `errorCode`: stable code, always `FIELD_TOO_LARGE` for this class of validation error.
- `message`: short safe message with no field content.
- `queueName`: present where available.
- `sourceId`: present only when already known and safe.
- `itemId`: present only when already known and safe.
- `fieldName`: stable field identifier.
- `maxBytes`: configured maximum.
- `actualBytes`: measured size.

## Direct Java Client Behaviour

The direct Java client remains mandatory for trusted internal direct PostgreSQL access and must remain a thin supported path over core behavior.

Recommended exception:

```text
QueueFieldTooLargeException extends InvalidQueueRequestException
```

The exception should expose:

- `fieldName`
- `maxBytes`
- `actualBytes`
- `queueName`, where available
- `sourceId` and `itemId`, where available

The exception must not include field content.

Direct Java should not infer oversized-field behavior from message strings. It should map the stable core error code `FIELD_TOO_LARGE` and structured error context into `QueueFieldTooLargeException`.

## Java REST Client Behaviour

The Java REST client may support optional local `QueueLimits` configuration for early validation. This is a convenience only.

The server remains authoritative because:

- The client may not know the server's configured limits.
- Server-side limits can change without a client release.
- Other HTTP clients may call the REST API directly.

When the server returns `FIELD_TOO_LARGE`, the Java REST client should map it to a typed client exception with the structured fields.

## Python REST Client Behaviour

The Python REST client may add optional local limit validation later, but it should not be required for this design.

Required behavior:

- Preserve server `FIELD_TOO_LARGE` response details in the Python client exception.
- Do not echo oversized content in exception messages.
- Do not fetch server limits automatically in this stage.

## Error Model

Use one stable core error code:

```text
FIELD_TOO_LARGE
```

Do not create separate error codes such as `PAYLOAD_TOO_LARGE`, `HEADERS_TOO_LARGE`, or `RESULT_TOO_LARGE`. Field-specific detail belongs in structured context:

- `fieldName`
- `maxBytes`
- `actualBytes`

Rationale:

- One code is easier for clients to handle.
- Field-specific data is still available.
- Future fields can reuse the same code without expanding the public error-code surface.

Recommended field names:

- `payload`
- `headers`
- `result`
- `errorType`
- `errorMessage`
- `adminReason`
- `adminMetadata`

## Logging and Security Considerations

Oversized content must not be logged by default.

Allowed log fields:

- event name
- operation
- queue name
- source ID
- item ID
- field name
- max bytes
- actual bytes
- stable error code

Disallowed log fields:

- payload JSON
- headers JSON
- result JSON
- error message content
- admin reason content
- admin metadata content
- API keys
- idempotency keys

REST errors and client exceptions must not echo oversized field content. This avoids turning validation failures into data exfiltration or log-amplification paths.

## Database and Schema Impact

No schema change is required for Stage 3A.

The pre-release schema baseline remains `V1`.

Database-level JSONB byte-size constraints are not recommended for this stage because:

- They are awkward to express consistently for JSONB values.
- Application-level validation gives clearer structured errors.
- REST and direct Java share core validation.
- The supported direct database path is the direct Java client, not arbitrary SQL writes.

Residual risk:

- A rogue SQL client that bypasses `sequenced-queue` code can still insert oversized values.

Mitigation:

- Direct SQL writes outside `sequenced-queue` are unsupported.
- Least-privilege database role guidance should continue to discourage broad write access.
- Future database constraints can be reconsidered if operational incidents justify them.

## Test Plan

Core tests:

- Payload exactly at limit passes.
- Payload one byte over limit fails.
- Headers exactly at limit passes.
- Headers one byte over limit fails.
- Result exactly at limit passes.
- Result one byte over limit fails.
- Error type exactly at limit passes.
- Error type one byte over limit fails.
- Error message exactly at limit passes.
- Error message one byte over limit fails.
- Admin reason exactly at limit passes.
- Admin reason one byte over limit fails.
- Admin metadata exactly at limit passes.
- Admin metadata one byte over limit fails.
- UTF-8 multibyte size is counted as bytes, not characters.
- Null JSON maps follow existing default handling.
- Empty JSON objects are measured as `{}`.

REST API tests:

- Oversized enqueue payload returns HTTP 400.
- Oversized enqueue headers returns HTTP 400.
- Oversized completion result returns HTTP 400.
- Oversized failure error type returns HTTP 400.
- Oversized failure error message returns HTTP 400.
- Oversized admin reason returns HTTP 400.
- Error response does not echo oversized content.
- Structured error includes `fieldName`, `maxBytes`, and `actualBytes`.

Direct Java client tests:

- Oversized direct enqueue payload throws `QueueFieldTooLargeException`.
- Oversized direct enqueue headers throws `QueueFieldTooLargeException`.
- Oversized direct completion result throws `QueueFieldTooLargeException`.
- Oversized direct failure error type throws `QueueFieldTooLargeException`.
- Oversized direct failure error message throws `QueueFieldTooLargeException`.
- Oversized direct admin reason throws `QueueFieldTooLargeException`.
- Exception does not include oversized content.

Compatibility tests:

- REST and direct Java enforce the same default limits.
- REST and direct Java produce equivalent `FIELD_TOO_LARGE` semantics.
- Existing queue ordering contract tests still pass.
- Existing source lease, heartbeat, recovery, admin repair, and retention purge tests still pass.
- No schema migration is added.
- `getSchemaInfo()` still reports baseline `V1`.

Configuration tests:

- Defaults are applied when properties are missing.
- Zero limits fail startup/builder creation.
- Negative limits fail startup/builder creation.
- Direct Java invalid limit configuration fails before client use.

## OpenAPI Impact

Implementation should update `docs/openapi.yaml` to:

- Document default limits in endpoint descriptions or schema descriptions.
- Add a `FIELD_TOO_LARGE` error example.
- Include `fieldName`, `maxBytes`, and `actualBytes` in the standard error schema.
- Avoid hardcoding configurable limits as JSON Schema `maxLength` values unless they become true fixed product constants.

Use prose and examples for configurable defaults rather than strict OpenAPI constraints.

## Documentation Impact

Implementation should update:

- `README.md`
- `docs/semantics.md`
- `docs/security.md`
- `docs/developer_quickstart.md`
- `docs/versioning.md`
- `docs/openapi.yaml`
- Direct Java client documentation
- Python client documentation
- `examples/README.md`, only if examples demonstrate limits

Documentation should say:

- Limits are global configuration values.
- Authoritative validation is in core.
- REST and direct Java paths share semantics.
- Oversized content is not echoed in errors or logs.
- Schema baseline remains `V1`.

## Risks and Tradeoffs

Application-level validation vs database constraints:

- Application validation gives better errors and shared REST/direct semantics.
- Database constraints would defend against rogue SQL but are harder to express and evolve.

Byte-size limits vs character-count limits:

- Byte-size limits match storage and network cost.
- Users may find byte limits less intuitive for Unicode-heavy content.

REST clients not knowing server-configured limits:

- Server remains authoritative.
- Optional local validation may drift unless configured explicitly.

Direct Java equivalent configuration:

- Direct Java must map to the same core `QueueLimits`.
- Divergent defaults would create subtle REST/direct incompatibility.

Large payload users:

- Some users may need external blob storage later.
- This design intentionally keeps the queue from becoming a large-object store.

Logging and error safety:

- Including measured sizes is useful.
- Including content is unsafe and must be avoided.

JSON serialization stability:

- Tests must measure the same compact JSON representation used for persistence.
- Pretty-printed or alternate object serialization can create unstable tests.

## Future Work

- Per-queue limits via a future `queue_config` design.
- Admin endpoint to inspect effective limits.
- REST client discovery of server limits.
- External blob payload pattern.
- Payload compression policy.
- Database `CHECK` constraints if later justified.
- Tenant-specific limits.
- Post-go-live migration policy for schema changes.
- Optional OpenAPI extensions for documenting deployment-specific limits.

## Recommendation

Approve Stage 3A implementation with these constraints:

- Add or complete a shared core `QueueLimits` model.
- Add shared core validation utilities for JSON and string byte-size limits.
- Enforce limits at the core service boundary for enqueue, complete, fail, and admin audit operations.
- Add `maxResultBytes`, `maxErrorTypeBytes`, and `maxAdminMetadataBytes` to the current limit set.
- Use stable core error code `FIELD_TOO_LARGE`.
- Return HTTP 400 with safe structured error details through REST.
- Map direct Java failures to `QueueFieldTooLargeException`.
- Keep REST client and Python client validation optional and convenience-only.
- Do not change queue semantics.
- Do not add a schema migration.
- Keep the pre-release schema baseline at `V1`.
