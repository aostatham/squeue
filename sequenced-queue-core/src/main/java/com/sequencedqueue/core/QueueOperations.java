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

    List<SourceResponse> blockedSources(String queueName);

    SourceResponse unblockSource(String queueName, String sourceId);

    ItemResponse retry(String queueName, UUID itemId);

    ItemResponse skip(String queueName, UUID itemId);

    ItemResponse cancel(String queueName, UUID itemId);

    void recoverExpiredLeases();

    QueueSchemaInfo getSchemaInfo();
}
