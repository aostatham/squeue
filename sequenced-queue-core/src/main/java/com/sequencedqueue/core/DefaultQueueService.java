package com.sequencedqueue.core;

import static com.sequencedqueue.core.QueueDtos.*;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultQueueService implements QueueOperations {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final QueueRepository repository;
    private final TransactionRunner transactions;
    private final RetryPolicy retryPolicy;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final int defaultLeaseSeconds;
    private final int defaultMaxAttempts;

    public DefaultQueueService(QueueRepository repository, TransactionRunner transactions, RetryPolicy retryPolicy, ObjectMapper objectMapper, Clock clock, int defaultLeaseSeconds, int defaultMaxAttempts) {
        this.repository = repository;
        this.transactions = transactions;
        this.retryPolicy = retryPolicy;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.defaultLeaseSeconds = defaultLeaseSeconds;
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    @Override
    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        try {
            return transactions.inTransaction(() -> enqueueInTransaction(queueName, request));
        } catch (DuplicateIdempotencyKeyException e) {
            if (request == null || request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
                throw e;
            }
            return transactions.inTransaction(() -> repository.findByIdempotencyKey(queueName, request.idempotencyKey())
                .map(this::toEnqueueResponse)
                .orElseThrow(() -> e));
        }
    }

    @Override
    public ClaimResponse claim(String queueName, ClaimRequest request) {
        return transactions.inTransaction(() -> claimInTransaction(queueName, request));
    }

    @Override
    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        return transactions.inTransaction(() -> {
            QueueItemRow item = verifyLease(queueName, itemId, request.workerId(), request.leaseId());
            QueueItemRow completed = repository.complete(item.itemId(), writeJson(defaultMap(request.result())), now());
            repository.releaseSource(item.queueName(), item.sourceId(), now());
            return toItemResponse(completed);
        });
    }

    @Override
    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        return transactions.inTransaction(() -> failInTransaction(queueName, itemId, request));
    }

    @Override
    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        transactions.inTransaction(() -> {
            requireText(request.workerId(), "workerId");
            int extendBy = request.extendBySeconds() == null ? defaultLeaseSeconds : request.extendBySeconds();
            int updated = repository.heartbeat(queueName, leaseId, request.workerId(), now().plusSeconds(extendBy), now());
            if (updated == 0) {
                throw new QueueException(QueueException.CONFLICT, "lease is not active for worker");
            }
        });
    }

    @Override
    public ItemResponse getItem(String queueName, UUID itemId) {
        return transactions.inTransaction(() -> repository.findItem(queueName, itemId)
            .map(this::toItemResponse)
            .orElseThrow(() -> new QueueException(QueueException.NOT_FOUND, "item not found")));
    }

    @Override
    public List<ItemResponse> getSourceItems(String queueName, String sourceId) {
        return transactions.inTransaction(() -> repository.listSourceItems(queueName, sourceId).stream().map(this::toItemResponse).toList());
    }

    @Override
    public List<SourceResponse> blockedSources(String queueName) {
        return transactions.inTransaction(() -> repository.blockedSources(queueName).stream().map(this::toSourceResponse).toList());
    }

    @Override
    public SourceResponse unblockSource(String queueName, String sourceId) {
        return transactions.inTransaction(() -> {
            OffsetDateTime now = now();
            SourceStateRow source = repository.lockSource(queueName, sourceId);
            if (source.status() != SourceStatus.blocked) {
                throw new QueueException(QueueException.CONFLICT, "source is not blocked");
            }
            Optional<QueueItemRow> head = repository.findHeadBlockingItem(queueName, sourceId);
            if (head.isPresent() && head.get().status() == ItemStatus.dead_lettered) {
                throw new QueueException(QueueException.CONFLICT, "dead-lettered head remains; use retry, skip, or cancel on the item");
            }
            repository.releaseSource(queueName, sourceId, now);
            return toSourceResponse(repository.findSource(queueName, sourceId));
        });
    }

    @Override
    public ItemResponse retry(String queueName, UUID itemId) {
        return transactions.inTransaction(() -> adminRepair(queueName, itemId, ItemStatus.retry_wait, List.of(ItemStatus.dead_lettered)));
    }

    @Override
    public ItemResponse skip(String queueName, UUID itemId) {
        return transactions.inTransaction(() -> adminRepair(queueName, itemId, ItemStatus.skipped, List.of(ItemStatus.pending, ItemStatus.retry_wait, ItemStatus.dead_lettered)));
    }

    @Override
    public ItemResponse cancel(String queueName, UUID itemId) {
        return transactions.inTransaction(() -> adminRepair(queueName, itemId, ItemStatus.cancelled, List.of(ItemStatus.pending, ItemStatus.retry_wait, ItemStatus.dead_lettered)));
    }

    @Override
    public void recoverExpiredLeases() {
        transactions.inTransaction(() -> {
            OffsetDateTime now = now();
            for (QueueItemRow candidate : repository.expiredProcessing(now, 100)) {
                recoverExpiredItem(candidate, now);
            }
        });
    }

    @Override
    public QueueSchemaInfo getSchemaInfo() {
        return transactions.inTransaction(repository::getSchemaInfo);
    }

    private EnqueueResponse enqueueInTransaction(String queueName, EnqueueRequest request) {
        requireText(queueName, "queueName");
        requireText(request.sourceId(), "sourceId");
        requireText(request.itemType(), "itemType");

        var existing = repository.findByIdempotencyKey(queueName, request.idempotencyKey());
        if (existing.isPresent()) {
            return toEnqueueResponse(existing.get());
        }

        OffsetDateTime now = now();
        repository.ensureSource(queueName, request.sourceId(), now);
        SourceStateRow source = repository.lockSource(queueName, request.sourceId());
        existing = repository.findByIdempotencyKey(queueName, request.idempotencyKey());
        if (existing.isPresent()) {
            return toEnqueueResponse(existing.get());
        }
        long sequenceNo = source.nextSequenceNo();
        repository.incrementNextSequence(queueName, request.sourceId(), now);

        QueueItemRow item = repository.insertItem(UUID.randomUUID(), queueName, request.sourceId(), sequenceNo, request.itemType(), writeJson(defaultMap(request.payload())), writeJson(defaultMap(request.headers())), request.availableAt() == null ? now : request.availableAt(), request.maxAttempts() == null ? defaultMaxAttempts : request.maxAttempts(), blankToNull(request.idempotencyKey()), now);
        return toEnqueueResponse(item);
    }

    private ClaimResponse claimInTransaction(String queueName, ClaimRequest request) {
        requireText(queueName, "queueName");
        requireText(request.workerId(), "workerId");
        if (request.supportedItemTypes() == null || request.supportedItemTypes().isEmpty()) {
            throw new QueueException(QueueException.BAD_REQUEST, "supportedItemTypes is required");
        }
        if (request.maxItems() != null && request.maxItems() > 1) {
            throw new QueueException(QueueException.BAD_REQUEST, "MVP supports maxItems <= 1");
        }

        repository.blockDeadLetteredHeadSources(queueName, now());
        UUID leaseId = UUID.randomUUID();
        int leaseSeconds = request.leaseSeconds() == null ? defaultLeaseSeconds : request.leaseSeconds();
        OffsetDateTime leaseUntil = now().plusSeconds(leaseSeconds);
        return repository.claimHeadItem(queueName, request.supportedItemTypes(), request.workerId(), leaseId, leaseUntil, now())
            .map(item -> new ClaimResponse(leaseId, item.queueName(), item.sourceId(), leaseUntil, List.of(new ClaimItem(item.itemId(), item.sequenceNo(), item.itemType(), readJson(item.payloadJson()), readJson(item.headersJson())))))
            .orElseGet(ClaimResponse::empty);
    }

    private ItemResponse failInTransaction(String queueName, UUID itemId, FailRequest request) {
        QueueItemRow item = verifyLease(queueName, itemId, request.workerId(), request.leaseId());
        OffsetDateTime now = now();
        boolean exhausted = item.attemptCount() >= item.maxAttempts();
        ItemStatus status;
        OffsetDateTime availableAt = now;

        if (!request.retryable()) {
            status = ItemStatus.failed;
        } else if (exhausted) {
            status = ItemStatus.dead_lettered;
        } else {
            status = ItemStatus.retry_wait;
            Duration backoff = request.backoffSeconds() == null ? retryPolicy.nextBackoff(item.attemptCount()) : Duration.ofSeconds(request.backoffSeconds());
            availableAt = now.plus(backoff);
        }

        QueueItemRow failed = repository.fail(item.itemId(), status, availableAt, request.errorType(), request.errorMessage(), now);
        if (status == ItemStatus.dead_lettered) {
            repository.blockSource(item.queueName(), item.sourceId(), now);
        } else {
            repository.releaseSource(item.queueName(), item.sourceId(), now);
        }
        return toItemResponse(failed);
    }

    private QueueItemRow verifyLease(String queueName, UUID itemId, String workerId, UUID leaseId) {
        requireText(workerId, "workerId");
        if (leaseId == null) {
            throw new QueueException(QueueException.BAD_REQUEST, "leaseId is required");
        }
        QueueItemRow candidate = repository.findItem(queueName, itemId)
            .orElseThrow(() -> new QueueException(QueueException.NOT_FOUND, "item not found"));
        SourceStateRow source = repository.lockSource(candidate.queueName(), candidate.sourceId());
        QueueItemRow item = repository.lockItem(queueName, itemId);
        if (!item.sourceId().equals(candidate.sourceId())) {
            throw new QueueException(QueueException.CONFLICT, "item source changed while locking");
        }
        if (item.status() != ItemStatus.processing) {
            throw new QueueException(QueueException.CONFLICT, "item is not processing");
        }
        if (!workerId.equals(item.claimedBy()) || !leaseId.equals(item.leaseId())) {
            throw new QueueException(QueueException.CONFLICT, "lease is not held by worker");
        }
        if (item.leaseUntil() == null || !item.leaseUntil().isAfter(now())) {
            throw new QueueException(QueueException.CONFLICT, "lease has expired");
        }
        if (source.status() != SourceStatus.leased || !workerId.equals(source.leasedBy()) || !leaseId.equals(source.leaseId())) {
            throw new QueueException(QueueException.CONFLICT, "source lease is not held by worker");
        }
        return item;
    }

    private ItemResponse adminRepair(String queueName, UUID itemId, ItemStatus targetStatus, List<ItemStatus> allowedStatuses) {
        QueueItemRow candidate = repository.findItem(queueName, itemId)
            .orElseThrow(() -> new QueueException(QueueException.NOT_FOUND, "item not found"));
        SourceStateRow source = repository.lockSource(candidate.queueName(), candidate.sourceId());
        QueueItemRow item = repository.lockItem(queueName, itemId);
        ensureAdminRepairAllowed(item, candidate, source, allowedStatuses);
        OffsetDateTime now = now();
        QueueItemRow updated = repository.adminStatus(item.itemId(), targetStatus, now, now);
        repository.releaseSource(item.queueName(), item.sourceId(), now);
        return toItemResponse(updated);
    }

    private void ensureAdminRepairAllowed(QueueItemRow item, QueueItemRow candidate, SourceStateRow source, List<ItemStatus> allowedStatuses) {
        if (!item.sourceId().equals(candidate.sourceId())) {
            throw new QueueException(QueueException.CONFLICT, "item source changed while locking");
        }
        if (!allowedStatuses.contains(item.status())) {
            throw new QueueException(QueueException.CONFLICT, "admin repair is not allowed for item status " + item.status());
        }
        if (source.status() == SourceStatus.leased) {
            throw new QueueException(QueueException.CONFLICT, "admin repair cannot modify an actively leased source");
        }
        QueueItemRow head = repository.findHeadBlockingItem(item.queueName(), item.sourceId())
            .orElseThrow(() -> new QueueException(QueueException.CONFLICT, "source has no blocking head item"));
        if (!head.itemId().equals(item.itemId())) {
            throw new QueueException(QueueException.CONFLICT, "admin repair item is not the source head item");
        }
    }

    private void recoverExpiredItem(QueueItemRow candidate, OffsetDateTime now) {
        SourceStateRow source = repository.lockSource(candidate.queueName(), candidate.sourceId());
        QueueItemRow item = repository.lockItem(candidate.queueName(), candidate.itemId());
        if (item.status() != ItemStatus.processing || item.leaseUntil() == null || !item.leaseUntil().isBefore(now)) {
            return;
        }
        boolean sourceMatchesItemLease = source.status() == SourceStatus.leased
            && item.leaseId() != null
            && item.leaseId().equals(source.leaseId())
            && item.claimedBy() != null
            && item.claimedBy().equals(source.leasedBy())
            && source.leaseUntil() != null
            && source.leaseUntil().isBefore(now);
        boolean sourceIsSafelyIdle = source.status() == SourceStatus.idle
            && source.leaseId() == null
            && source.leasedBy() == null
            && source.leaseUntil() == null;
        if (!sourceMatchesItemLease && !sourceIsSafelyIdle) {
            return;
        }

        boolean exhausted = item.attemptCount() >= item.maxAttempts();
        if (exhausted) {
            repository.fail(item.itemId(), ItemStatus.dead_lettered, now, "LEASE_EXPIRED", "Worker lease expired", now);
            repository.blockSource(item.queueName(), item.sourceId(), now);
        } else {
            repository.fail(item.itemId(), ItemStatus.retry_wait, now.plus(retryPolicy.nextBackoff(item.attemptCount())), "LEASE_EXPIRED", "Worker lease expired", now);
            repository.releaseSource(item.queueName(), item.sourceId(), now);
        }
    }

    private EnqueueResponse toEnqueueResponse(QueueItemRow item) {
        return new EnqueueResponse(item.itemId(), item.queueName(), item.sourceId(), item.sequenceNo(), item.status().name());
    }

    private ItemResponse toItemResponse(QueueItemRow item) {
        return new ItemResponse(item.itemId(), item.queueName(), item.sourceId(), item.sequenceNo(), item.itemType(), readJson(item.payloadJson()), readJson(item.headersJson()), item.status().name(), item.availableAt(), item.claimedBy(), item.leaseId(), item.leaseUntil(), item.attemptCount(), item.maxAttempts(), item.idempotencyKey(), item.lastErrorType(), item.lastErrorMessage(), readJson(item.resultJson()), item.createdAt(), item.updatedAt());
    }

    private SourceResponse toSourceResponse(SourceStateRow source) {
        return new SourceResponse(source.queueName(), source.sourceId(), source.nextSequenceNo(), source.status().name(), source.leasedBy(), source.leaseId(), source.leaseUntil(), source.updatedAt());
    }

    private OffsetDateTime now() {
        return OffsetDateTime.now(clock);
    }

    private Map<String, Object> defaultMap(Map<String, Object> value) {
        return value == null ? Map.of() : value;
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new QueueException(QueueException.BAD_REQUEST, field + " is required");
        }
    }

    private String writeJson(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (Exception e) {
            throw new QueueException(QueueException.BAD_REQUEST, "invalid json payload");
        }
    }

    private Map<String, Object> readJson(String value) {
        if (value == null || value.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(value, MAP_TYPE);
        } catch (Exception e) {
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "stored json could not be read");
        }
    }
}
