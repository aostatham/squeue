CREATE TABLE queue_source_state (
    queue_name TEXT NOT NULL,
    source_id TEXT NOT NULL,

    next_sequence_no BIGINT NOT NULL DEFAULT 1,

    status TEXT NOT NULL DEFAULT 'idle',
    leased_by TEXT NULL,
    lease_id UUID NULL,
    lease_until TIMESTAMPTZ NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (queue_name, source_id),

    CONSTRAINT chk_queue_source_state_status
        CHECK (status IN ('idle', 'leased', 'blocked')),

    CONSTRAINT chk_queue_source_state_next_sequence_positive
        CHECK (next_sequence_no >= 1)
);

CREATE TABLE queue_item (
    item_id UUID PRIMARY KEY,

    queue_name TEXT NOT NULL,
    source_id TEXT NOT NULL,
    sequence_no BIGINT NOT NULL,

    item_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    headers_json JSONB NOT NULL DEFAULT '{}',

    status TEXT NOT NULL,

    available_at TIMESTAMPTZ NOT NULL,

    claimed_by TEXT NULL,
    lease_id UUID NULL,
    claimed_at TIMESTAMPTZ NULL,
    lease_until TIMESTAMPTZ NULL,

    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,

    idempotency_key TEXT NULL,

    last_error_type TEXT NULL,
    last_error_message TEXT NULL,
    result_json JSONB NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (queue_name, source_id, sequence_no),

    CONSTRAINT chk_queue_item_status
        CHECK (status IN (
            'pending',
            'processing',
            'succeeded',
            'retry_wait',
            'failed',
            'dead_lettered',
            'cancelled',
            'skipped'
        )),

    CONSTRAINT chk_queue_item_sequence_positive
        CHECK (sequence_no >= 1),

    CONSTRAINT chk_queue_item_attempt_count_non_negative
        CHECK (attempt_count >= 0),

    CONSTRAINT chk_queue_item_max_attempts_positive
        CHECK (max_attempts >= 1),

    CONSTRAINT chk_queue_item_attempt_count_lte_max
        CHECK (attempt_count <= max_attempts),

    CONSTRAINT chk_queue_item_payload_json_object
        CHECK (jsonb_typeof(payload_json) = 'object'),

    CONSTRAINT chk_queue_item_headers_json_object
        CHECK (jsonb_typeof(headers_json) = 'object')
);

CREATE UNIQUE INDEX ux_queue_item_idempotency_key
ON queue_item (queue_name, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_queue_item_source_sequence
ON queue_item (queue_name, source_id, sequence_no);

CREATE INDEX idx_queue_item_claimable
ON queue_item (queue_name, status, available_at, created_at);

CREATE INDEX idx_queue_item_source_status
ON queue_item (queue_name, source_id, status, sequence_no);

CREATE INDEX idx_queue_item_dead_letter
ON queue_item (queue_name, status, updated_at)
WHERE status = 'dead_lettered';

CREATE INDEX idx_queue_item_expired_processing
ON queue_item (lease_until)
WHERE status = 'processing';

CREATE INDEX idx_source_state_claim
ON queue_source_state (queue_name, status, lease_until, updated_at);

CREATE INDEX idx_source_state_expired_leases
ON queue_source_state (lease_until)
WHERE status = 'leased';

CREATE TABLE queue_admin_audit (
    audit_id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    actor_id TEXT NULL,
    operation TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    source_id TEXT NULL,
    item_id UUID NULL,
    previous_status TEXT NULL,
    new_status TEXT NULL,
    reason TEXT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}',

    CONSTRAINT chk_queue_admin_audit_operation
        CHECK (operation IN ('retry', 'skip', 'cancel', 'unblock', 'retention_purge')),

    CONSTRAINT chk_queue_admin_audit_metadata_json_object
        CHECK (jsonb_typeof(metadata_json) = 'object')
);

CREATE INDEX idx_queue_admin_audit_queue_time
ON queue_admin_audit (queue_name, occurred_at DESC);

CREATE INDEX idx_queue_admin_audit_item
ON queue_admin_audit (item_id)
WHERE item_id IS NOT NULL;

CREATE INDEX idx_queue_admin_audit_source
ON queue_admin_audit (queue_name, source_id, occurred_at DESC)
WHERE source_id IS NOT NULL;

CREATE INDEX idx_queue_item_retention
ON queue_item (queue_name, status, updated_at, item_id)
WHERE status IN ('succeeded', 'cancelled', 'skipped', 'failed');
