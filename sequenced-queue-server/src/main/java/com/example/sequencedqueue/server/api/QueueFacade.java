package com.example.sequencedqueue.server.api;

import static com.sequencedqueue.core.QueueDtos.*;

import java.util.List;
import java.util.UUID;

import com.sequencedqueue.core.QueueOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class QueueFacade {
    private final QueueOperations queueService;

    public QueueFacade(QueueOperations queueService) {
        this.queueService = queueService;
    }

    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        return queueService.enqueue(queueName, request);
    }

    public ClaimResponse claim(String queueName, ClaimRequest request) {
        return queueService.claim(queueName, request);
    }

    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        return queueService.complete(queueName, itemId, request);
    }

    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        return queueService.fail(queueName, itemId, request);
    }

    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        queueService.heartbeat(queueName, leaseId, request);
    }

    public ItemResponse getItem(String queueName, UUID itemId) {
        return queueService.getItem(queueName, itemId);
    }

    public List<ItemResponse> getSourceItems(String queueName, String sourceId) {
        return queueService.getSourceItems(queueName, sourceId);
    }

    public List<SourceResponse> blockedSources(String queueName) {
        return queueService.blockedSources(queueName);
    }

    public SourceResponse unblockSource(String queueName, String sourceId) {
        return queueService.unblockSource(queueName, sourceId);
    }

    public ItemResponse retry(String queueName, UUID itemId) {
        return queueService.retry(queueName, itemId);
    }

    public ItemResponse skip(String queueName, UUID itemId) {
        return queueService.skip(queueName, itemId);
    }

    public ItemResponse cancel(String queueName, UUID itemId) {
        return queueService.cancel(queueName, itemId);
    }

    @Scheduled(fixedDelayString = "${sequenced-queue.recovery-delay-ms:5000}")
    public void recoverExpiredLeases() {
        queueService.recoverExpiredLeases();
    }
}
