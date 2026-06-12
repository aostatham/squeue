package com.sequencedqueue.direct;

import java.time.OffsetDateTime;
import java.util.UUID;

public record SourceResponse(String queueName, String sourceId, long nextSequenceNo, String status, String leasedBy, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime updatedAt) {
}
