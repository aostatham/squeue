package com.example.sequencedqueue.server.store;

import java.time.OffsetDateTime;
import java.util.UUID;

import com.example.sequencedqueue.server.core.SourceStatus;

public record SourceStateRow(
    String queueName,
    String sourceId,
    long nextSequenceNo,
    SourceStatus status,
    String leasedBy,
    UUID leaseId,
    OffsetDateTime leaseUntil,
    OffsetDateTime createdAt,
    OffsetDateTime updatedAt
) {
}
