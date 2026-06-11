package com.sequencedqueue.core;

import java.time.OffsetDateTime;
import java.util.UUID;

public record SourceStateRow(String queueName, String sourceId, long nextSequenceNo, SourceStatus status, String leasedBy, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
}
