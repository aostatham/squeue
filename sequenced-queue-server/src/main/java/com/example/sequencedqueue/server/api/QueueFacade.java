package com.example.sequencedqueue.server.api;

import static com.sequencedqueue.core.QueueDtos.*;

import java.util.List;
import java.util.UUID;

import com.sequencedqueue.core.QueueOperations;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class QueueFacade {
    private final QueueOperations queueService;
    private final QueueOperationalMetrics metrics;
    private final boolean recoveryEnabled;

    public QueueFacade(QueueOperations queueService, QueueOperationalMetrics metrics, @Value("${sequenced-queue.recovery-enabled:true}") boolean recoveryEnabled) {
        this.queueService = queueService;
        this.metrics = metrics;
        this.recoveryEnabled = recoveryEnabled;
    }

    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        return queueService.enqueue(queueName, request);
    }

    public ClaimResponse claim(String queueName, ClaimRequest request) {
        ClaimResponse response = queueService.claim(queueName, request);
        metrics.claim(response.items().isEmpty());
        return response;
    }

    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        ItemResponse response = queueService.complete(queueName, itemId, request);
        metrics.completion();
        return response;
    }

    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        ItemResponse response = queueService.fail(queueName, itemId, request);
        metrics.failure();
        return response;
    }

    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        try {
            queueService.heartbeat(queueName, leaseId, request);
            metrics.heartbeat();
        } catch (RuntimeException e) {
            metrics.heartbeatFailed();
            throw e;
        }
    }

    public ItemResponse getItem(String queueName, UUID itemId) {
        return queueService.getItem(queueName, itemId);
    }

    public List<ItemResponse> getSourceItems(String queueName, String sourceId) {
        return queueService.getSourceItems(queueName, sourceId);
    }

    public List<ItemResponse> deadLetteredItems(String queueName, int limit, int offset) {
        return queueService.deadLetteredItems(queueName, limit, offset);
    }

    public List<SourceResponse> blockedSources(String queueName) {
        return queueService.blockedSources(queueName);
    }

    public List<BlockedSourceResponse> inspectBlockedSources(String queueName, int limit, int offset) {
        return queueService.inspectBlockedSources(queueName, limit, offset);
    }

    public SourceResponse unblockSource(String queueName, String sourceId, String actorId, String reason) {
        SourceResponse response = queueService.unblockSource(queueName, sourceId, actorId, reason);
        metrics.adminUnblock();
        return response;
    }

    public ItemResponse retry(String queueName, UUID itemId, String actorId, String reason) {
        ItemResponse response = queueService.retry(queueName, itemId, actorId, reason);
        metrics.adminRetry();
        return response;
    }

    public ItemResponse skip(String queueName, UUID itemId, String actorId, String reason) {
        ItemResponse response = queueService.skip(queueName, itemId, actorId, reason);
        metrics.adminSkip();
        return response;
    }

    public ItemResponse cancel(String queueName, UUID itemId, String actorId, String reason) {
        ItemResponse response = queueService.cancel(queueName, itemId, actorId, reason);
        metrics.adminCancel();
        return response;
    }

    public List<AdminAuditResponse> adminAudit(String queueName, int limit, int offset) {
        return queueService.adminAudit(queueName, limit, offset);
    }

    @Scheduled(fixedDelayString = "${sequenced-queue.recovery-delay-ms:5000}")
    public void recoverExpiredLeases() {
        if (!recoveryEnabled) {
            return;
        }
        metrics.leaseExpiries(queueService.recoverExpiredLeases());
    }
}
