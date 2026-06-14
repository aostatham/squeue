package com.example.sequencedqueue.server.api;

import static com.sequencedqueue.core.QueueDtos.*;

import java.util.List;
import java.util.UUID;

import com.sequencedqueue.core.QueueOperations;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class QueueFacade {
    private static final Logger LOG = LoggerFactory.getLogger(QueueFacade.class);

    private final QueueOperations queueService;
    private final QueueOperationalMetrics metrics;
    private final boolean recoveryEnabled;

    public QueueFacade(QueueOperations queueService, QueueOperationalMetrics metrics, @Value("${sequenced-queue.recovery-enabled:true}") boolean recoveryEnabled) {
        this.queueService = queueService;
        this.metrics = metrics;
        this.recoveryEnabled = recoveryEnabled;
    }

    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        EnqueueResponse response = queueService.enqueue(queueName, request);
        LOG.info("event=queue.enqueue queueName={} sourceId={} itemId={} sequenceNo={} status={}",
            response.queueName(), response.sourceId(), response.itemId(), response.sequenceNo(), response.status());
        return response;
    }

    public ClaimResponse claim(String queueName, ClaimRequest request) {
        ClaimResponse response = queueService.claim(queueName, request);
        metrics.claim(response.items().isEmpty());
        if (response.items().isEmpty()) {
            LOG.debug("event=queue.claim.empty queueName={} workerId={}", queueName, request == null ? null : request.workerId());
        } else {
            ClaimItem item = response.items().getFirst();
            LOG.info("event=queue.claim queueName={} sourceId={} itemId={} sequenceNo={} itemType={} workerId={}",
                response.queueName(), response.sourceId(), item.itemId(), item.sequenceNo(), item.itemType(), request.workerId());
        }
        return response;
    }

    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        ItemResponse response = queueService.complete(queueName, itemId, request);
        metrics.completion();
        LOG.info("event=queue.complete queueName={} sourceId={} itemId={} sequenceNo={} itemType={} workerId={} status={}",
            response.queueName(), response.sourceId(), response.itemId(), response.sequenceNo(), response.itemType(), request.workerId(), response.status());
        return response;
    }

    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        ItemResponse response = queueService.fail(queueName, itemId, request);
        metrics.failure();
        LOG.info("event=queue.fail queueName={} sourceId={} itemId={} sequenceNo={} itemType={} workerId={} status={}",
            response.queueName(), response.sourceId(), response.itemId(), response.sequenceNo(), response.itemType(), request.workerId(), response.status());
        return response;
    }

    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        try {
            queueService.heartbeat(queueName, leaseId, request);
            metrics.heartbeat();
        } catch (RuntimeException e) {
            metrics.heartbeatFailed();
            LOG.warn("event=queue.heartbeat.failed queueName={} workerId={} errorCode={}",
                queueName, request == null ? null : request.workerId(), e.getClass().getSimpleName());
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
        LOG.info("event=queue.admin.unblock queueName={} sourceId={} operation=unblock status={}",
            response.queueName(), response.sourceId(), response.status());
        return response;
    }

    public ItemResponse retry(String queueName, UUID itemId, String actorId, String reason) {
        ItemResponse response = queueService.retry(queueName, itemId, actorId, reason);
        metrics.adminRetry();
        LOG.info("event=queue.admin.retry queueName={} sourceId={} itemId={} sequenceNo={} itemType={} operation=retry status={}",
            response.queueName(), response.sourceId(), response.itemId(), response.sequenceNo(), response.itemType(), response.status());
        return response;
    }

    public ItemResponse skip(String queueName, UUID itemId, String actorId, String reason) {
        ItemResponse response = queueService.skip(queueName, itemId, actorId, reason);
        metrics.adminSkip();
        LOG.info("event=queue.admin.skip queueName={} sourceId={} itemId={} sequenceNo={} itemType={} operation=skip status={}",
            response.queueName(), response.sourceId(), response.itemId(), response.sequenceNo(), response.itemType(), response.status());
        return response;
    }

    public ItemResponse cancel(String queueName, UUID itemId, String actorId, String reason) {
        ItemResponse response = queueService.cancel(queueName, itemId, actorId, reason);
        metrics.adminCancel();
        LOG.info("event=queue.admin.cancel queueName={} sourceId={} itemId={} sequenceNo={} itemType={} operation=cancel status={}",
            response.queueName(), response.sourceId(), response.itemId(), response.sequenceNo(), response.itemType(), response.status());
        return response;
    }

    public com.sequencedqueue.core.QueueDtos.RetentionPurgeResponse purgeRetention(String queueName, com.sequencedqueue.core.QueueDtos.RetentionPurgeRequest request, String actorId) {
        com.sequencedqueue.core.QueueDtos.RetentionPurgeResponse response = queueService.purgeRetention(queueName, request, actorId);
        LOG.info("event=queue.retention.purge queueName={} operation=retention_purge result={} matched={} deleted={}",
            response.queueName(), response.dryRun() ? "dry_run" : "deleted", response.matched(), response.deleted());
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
        try {
            int recovered = queueService.recoverExpiredLeases();
            metrics.leaseExpiries(recovered);
            if (recovered > 0) {
                LOG.warn("event=queue.lease.expired operation=recoverExpiredLeases result=recovered matched={}", recovered);
            }
        } catch (RuntimeException e) {
            LOG.warn("event=queue.lease.recovery.failed operation=recoverExpiredLeases errorCode={}", e.getClass().getSimpleName());
        }
    }
}
