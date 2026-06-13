ALTER TABLE queue_admin_audit
DROP CONSTRAINT chk_queue_admin_audit_operation;

ALTER TABLE queue_admin_audit
ADD CONSTRAINT chk_queue_admin_audit_operation
    CHECK (operation IN ('retry', 'skip', 'cancel', 'unblock', 'retention_purge'));
