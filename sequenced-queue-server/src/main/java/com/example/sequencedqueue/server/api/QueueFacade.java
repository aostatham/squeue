package com.example.sequencedqueue.server.api;

import static com.example.sequencedqueue.server.api.ApiDtos.*;

import java.util.List;
import java.util.UUID;

import com.example.sequencedqueue.server.core.QueueService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class QueueFacade {
    private final QueueService queueService;

    public QueueFacade(QueueService queueService) {
        this.queueService = queueService;
    }

    @Transactional
    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        return queueService.enqueue(queueName, request);
    }

    @Transactional
    public ClaimResponse claim(String queueName, ClaimRequest request) {
        return queueService.claim(queueName, request);
    }

    @Transactional
    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        return queueService.complete(queueName, itemId, request);
    }

    @Transactional
    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        return queueService.fail(queueName, itemId, request);
    }

    @Transactional
    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        queueService.heartbeat(queueName, leaseId, request);
    }

    @Transactional(readOnly = true)
    public ItemResponse getItem(String queueName, UUID itemId) {
        return queueService.getItem(queueName, itemId);
    }

    @Transactional(readOnly = true)
    public List<ItemResponse> getSourceItems(String queueName, String sourceId) {
        return queueService.getSourceItems(queueName, sourceId);
    }

    @Transactional(readOnly = true)
    public List<SourceResponse> blockedSources(String queueName) {
        return queueService.blockedSources(queueName);
    }

    @Transactional
    public SourceResponse unblockSource(String queueName, String sourceId) {
        return queueService.unblockSource(queueName, sourceId);
    }

    @Transactional
    public ItemResponse retry(String queueName, UUID itemId) {
        return queueService.retry(queueName, itemId);
    }

    @Transactional
    public ItemResponse skip(String queueName, UUID itemId) {
        return queueService.skip(queueName, itemId);
    }

    @Transactional
    public ItemResponse cancel(String queueName, UUID itemId) {
        return queueService.cancel(queueName, itemId);
    }

    @Scheduled(fixedDelayString = "${sequenced-queue.recovery-delay-ms:5000}")
    @Transactional
    public void recoverExpiredLeases() {
        queueService.recoverExpiredLeases();
    }
}
