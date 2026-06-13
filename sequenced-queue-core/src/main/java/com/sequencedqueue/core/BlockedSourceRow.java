package com.sequencedqueue.core;

import java.time.OffsetDateTime;
import java.util.UUID;

public record BlockedSourceRow(String queueName, String sourceId, SourceStatus status, String leasedBy, OffsetDateTime leaseUntil, UUID headItemId, ItemStatus headItemStatus, OffsetDateTime updatedAt) {
}
