package com.sequencedqueue.core;

import static com.sequencedqueue.core.QueueDtos.*;

import java.util.List;
import java.util.UUID;

public interface QueueOperations {
    EnqueueResponse enqueue(String queueName, EnqueueRequest request);

    ClaimResponse claim(String queueName, ClaimRequest request);

    ItemResponse complete(String queueName, UUID itemId, CompleteRequest request);

    ItemResponse fail(String queueName, UUID itemId, FailRequest request);

    void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request);

    ItemResponse getItem(String queueName, UUID itemId);

    List<ItemResponse> getSourceItems(String queueName, String sourceId);

    List<ItemResponse> deadLetteredItems(String queueName, int limit, int offset);

    List<SourceResponse> blockedSources(String queueName);

    List<BlockedSourceResponse> inspectBlockedSources(String queueName, int limit, int offset);

    SourceResponse unblockSource(String queueName, String sourceId);

    SourceResponse unblockSource(String queueName, String sourceId, String actorId, String reason);

    ItemResponse retry(String queueName, UUID itemId);

    ItemResponse retry(String queueName, UUID itemId, String actorId, String reason);

    ItemResponse skip(String queueName, UUID itemId);

    ItemResponse skip(String queueName, UUID itemId, String actorId, String reason);

    ItemResponse cancel(String queueName, UUID itemId);

    ItemResponse cancel(String queueName, UUID itemId, String actorId, String reason);

    List<AdminAuditResponse> adminAudit(String queueName, int limit, int offset);

    int recoverExpiredLeases();

    QueueMetricsSnapshot metricsSnapshot();

    QueueSchemaInfo getSchemaInfo();
}
