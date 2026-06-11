package com.sequencedqueue.core;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface QueueRepository {
    Optional<QueueItemRow> findByIdempotencyKey(String queueName, String idempotencyKey);

    void ensureSource(String queueName, String sourceId, OffsetDateTime now);

    SourceStateRow lockSource(String queueName, String sourceId);

    QueueItemRow insertItem(UUID itemId, String queueName, String sourceId, long sequenceNo, String itemType, String payloadJson, String headersJson, OffsetDateTime availableAt, int maxAttempts, String idempotencyKey, OffsetDateTime now);

    void incrementNextSequence(String queueName, String sourceId, OffsetDateTime now);

    Optional<QueueItemRow> claimHeadItem(String queueName, List<String> itemTypes, String workerId, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime now);

    Optional<QueueItemRow> findItem(String queueName, UUID itemId);

    QueueItemRow lockItem(String queueName, UUID itemId);

    List<QueueItemRow> listSourceItems(String queueName, String sourceId);

    QueueItemRow complete(UUID itemId, String resultJson, OffsetDateTime now);

    QueueItemRow fail(UUID itemId, ItemStatus status, OffsetDateTime availableAt, String errorType, String errorMessage, OffsetDateTime now);

    void releaseSource(String queueName, String sourceId, OffsetDateTime now);

    void blockSource(String queueName, String sourceId, OffsetDateTime now);

    int releaseSourceIfLeaseMatches(String queueName, String sourceId, UUID leaseId, String leasedBy, OffsetDateTime now);

    int blockSourceIfLeaseMatches(String queueName, String sourceId, UUID leaseId, String leasedBy, OffsetDateTime now);

    int heartbeat(String queueName, UUID leaseId, String workerId, OffsetDateTime leaseUntil, OffsetDateTime now);

    List<SourceStateRow> blockedSources(String queueName);

    SourceStateRow findSource(String queueName, String sourceId);

    QueueItemRow adminStatus(UUID itemId, ItemStatus status, OffsetDateTime availableAt, OffsetDateTime now);

    Optional<QueueItemRow> skipDeadLetteredHead(String queueName, String sourceId, OffsetDateTime now);

    Optional<QueueItemRow> findHeadBlockingItem(String queueName, String sourceId);

    int blockDeadLetteredHeadSources(String queueName, OffsetDateTime now);

    List<QueueItemRow> expiredProcessing(OffsetDateTime now, int limit);

    QueueSchemaInfo getSchemaInfo();
}
