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
        CHECK (operation IN ('retry', 'skip', 'cancel', 'unblock')),

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
