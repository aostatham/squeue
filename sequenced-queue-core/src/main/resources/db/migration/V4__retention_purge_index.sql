CREATE INDEX idx_queue_item_retention
ON queue_item (queue_name, status, updated_at, item_id)
WHERE status IN ('succeeded', 'cancelled', 'skipped', 'failed');
