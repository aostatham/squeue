package com.sequencedqueue.direct;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record ItemResponse(UUID itemId, String queueName, String sourceId, long sequenceNo, String itemType, Map<String, Object> payload, Map<String, Object> headers, String status, OffsetDateTime availableAt, String claimedBy, UUID leaseId, OffsetDateTime leaseUntil, int attemptCount, int maxAttempts, String idempotencyKey, String lastErrorType, String lastErrorMessage, Map<String, Object> result, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
}
