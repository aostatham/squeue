package com.sequencedqueue.core;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class QueueDtos {
    private QueueDtos() {
    }

    public record EnqueueRequest(String sourceId, String itemType, String idempotencyKey, Map<String, Object> payload, Map<String, Object> headers, OffsetDateTime availableAt, Integer maxAttempts) {
    }

    public record EnqueueResponse(UUID itemId, String queueName, String sourceId, long sequenceNo, String status) {
    }

    public record ClaimRequest(String workerId, List<String> supportedItemTypes, Integer leaseSeconds, Integer maxItems) {
    }

    public record ClaimItem(UUID itemId, long sequenceNo, String itemType, Map<String, Object> payload, Map<String, Object> headers) {
    }

    public record ClaimResponse(UUID leaseId, String queueName, String sourceId, OffsetDateTime leaseUntil, List<ClaimItem> items) {
        public static ClaimResponse empty() {
            return new ClaimResponse(null, null, null, null, List.of());
        }
    }

    public record CompleteRequest(String workerId, UUID leaseId, Map<String, Object> result) {
    }

    public record FailRequest(String workerId, UUID leaseId, boolean retryable, String errorType, String errorMessage, Integer backoffSeconds) {
    }

    public record HeartbeatRequest(String workerId, Integer extendBySeconds) {
    }

    public record ItemResponse(UUID itemId, String queueName, String sourceId, long sequenceNo, String itemType, Map<String, Object> payload, Map<String, Object> headers, String status, OffsetDateTime availableAt, String claimedBy, UUID leaseId, OffsetDateTime leaseUntil, int attemptCount, int maxAttempts, String idempotencyKey, String lastErrorType, String lastErrorMessage, Map<String, Object> result, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
    }

    public record SourceResponse(String queueName, String sourceId, long nextSequenceNo, String status, String leasedBy, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime updatedAt) {
    }

    public record BlockedSourceResponse(String queueName, String sourceId, String status, String leasedBy, OffsetDateTime leaseUntil, UUID headItemId, String headItemStatus, OffsetDateTime updatedAt) {
    }

    public record AdminAuditResponse(UUID auditId, OffsetDateTime occurredAt, String actorId, String operation, String queueName, String sourceId, UUID itemId, String previousStatus, String newStatus, String reason, Map<String, Object> metadata) {
    }

    public record RetentionPurgeRequest(OffsetDateTime olderThan, List<String> statuses, Boolean dryRun, String reason, Integer limit) {
        public RetentionPurgeRequest(OffsetDateTime olderThan, List<String> statuses, Boolean dryRun, String reason) {
            this(olderThan, statuses, dryRun, reason, null);
        }
    }

    public record RetentionPurgeResponse(String queueName, boolean dryRun, long matched, long deleted) {
    }

    public record QueueMetricsSnapshot(long pendingItems, long processingItems, long retryWaitItems, long deadLetteredItems, long idleSources, long leasedSources, long blockedSources) {
    }
}
