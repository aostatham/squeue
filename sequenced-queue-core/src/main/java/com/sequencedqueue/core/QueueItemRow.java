package com.sequencedqueue.core;

import java.time.OffsetDateTime;
import java.util.UUID;

public record QueueItemRow(UUID itemId, String queueName, String sourceId, long sequenceNo, String itemType, String payloadJson, String headersJson, ItemStatus status, OffsetDateTime availableAt, String claimedBy, UUID leaseId, OffsetDateTime leaseUntil, int attemptCount, int maxAttempts, String idempotencyKey, String lastErrorType, String lastErrorMessage, String resultJson, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
}
