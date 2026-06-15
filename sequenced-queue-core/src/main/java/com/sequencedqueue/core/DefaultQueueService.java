package com.sequencedqueue.core;

import static com.sequencedqueue.core.QueueDtos.*;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Default core implementation that owns queue validation, sequencing, locking, and state transitions.
 */
public class DefaultQueueService implements QueueOperations {
    /** Jackson type token for persisted JSON objects. */
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    /** Terminal statuses that are safe for manual retention purge. */
    private static final EnumSet<ItemStatus> RETENTION_PURGE_STATUSES = EnumSet.of(
        ItemStatus.succeeded,
        ItemStatus.cancelled,
        ItemStatus.skipped,
        ItemStatus.failed
    );

    /** Repository used for PostgreSQL state changes. */
    private final QueueRepository repository;
    /** Transaction boundary for each queue operation. */
    private final TransactionRunner transactions;
    /** Retry delay policy for retryable failures and lease recovery. */
    private final RetryPolicy retryPolicy;
    /** JSON mapper used for persistence serialization and deserialization. */
    private final ObjectMapper objectMapper;
    /** Clock used to make time-dependent behavior testable. */
    private final Clock clock;
    /** Global queue settings and limits. */
    private final QueueSettings settings;

    /**
     * Creates a service with legacy lease and attempt settings plus other defaults.
     */
    public DefaultQueueService(QueueRepository repository, TransactionRunner transactions, RetryPolicy retryPolicy, ObjectMapper objectMapper, Clock clock, int defaultLeaseSeconds, int defaultMaxAttempts) {
        this(repository, transactions, retryPolicy, objectMapper, clock, new QueueSettings(
            defaultLeaseSeconds,
            QueueSettings.defaults().maxLeaseSeconds(),
            defaultMaxAttempts,
            QueueSettings.defaults().maxPayloadBytes(),
            QueueSettings.defaults().maxHeadersBytes(),
            QueueSettings.defaults().maxErrorMessageBytes(),
            QueueSettings.defaults().maxAdminReasonBytes(),
            QueueSettings.defaults().maxRetentionPurgeBatchSize()
        ));
    }

    /**
     * Creates a service with explicit dependencies and global settings.
     */
    public DefaultQueueService(QueueRepository repository, TransactionRunner transactions, RetryPolicy retryPolicy, ObjectMapper objectMapper, Clock clock, QueueSettings settings) {
        this.repository = repository;
        this.transactions = transactions;
        this.retryPolicy = retryPolicy;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.settings = settings;
    }

    /**
     * Enqueues a request and resolves concurrent idempotency conflicts to the existing item.
     */
    @Override
    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        requireRequest(request);
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

    /**
     * Claims work inside one transaction.
     */
    @Override
    public ClaimResponse claim(String queueName, ClaimRequest request) {
        requireRequest(request);
        return transactions.inTransaction(() -> claimInTransaction(queueName, request));
    }

    /**
     * Completes a leased item after validating both item and source lease ownership.
     */
    @Override
    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        requireRequest(request);
        String resultJson = writeJson(defaultMap(request.result()));
        requireMaxBytes(resultJson, settings.maxResultBytes(), "result", queueName, itemId);
        return transactions.inTransaction(() -> {
            QueueItemRow item = verifyLease(queueName, itemId, request.workerId(), request.leaseId());
            QueueItemRow completed = repository.complete(item.itemId(), resultJson, now());
            repository.releaseSource(item.queueName(), item.sourceId(), now());
            return toItemResponse(completed);
        });
    }

    /**
     * Fails a leased item and either releases or blocks the source according to the resulting status.
     */
    @Override
    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        requireRequest(request);
        return transactions.inTransaction(() -> failInTransaction(queueName, itemId, request));
    }

    /**
     * Extends a lease only when the worker still owns it.
     */
    @Override
    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        requireRequest(request);
        transactions.inTransaction(() -> {
            requireText(request.workerId(), "workerId");
            int extendBy = request.extendBySeconds() == null ? settings.defaultLeaseSeconds() : request.extendBySeconds();
            if (extendBy <= 0) {
                throw new QueueException(QueueException.BAD_REQUEST, "extendBySeconds must be > 0");
            }
            int updated = repository.heartbeat(queueName, leaseId, request.workerId(), now().plusSeconds(extendBy), now());
            if (updated == 0) {
                throw new QueueException(QueueErrorCode.LEASE_LOST, QueueException.CONFLICT, "lease is not active for worker").withQueueName(queueName);
            }
        });
    }

    /**
     * Loads a single item by ID.
     */
    @Override
    public ItemResponse getItem(String queueName, UUID itemId) {
        return transactions.inTransaction(() -> repository.findItem(queueName, itemId)
            .map(this::toItemResponse)
            .orElseThrow(() -> new QueueException(QueueErrorCode.ITEM_NOT_FOUND, QueueException.NOT_FOUND, "item not found").withContext(queueName, null, itemId)));
    }

    /**
     * Lists items for one source.
     */
    @Override
    public List<ItemResponse> getSourceItems(String queueName, String sourceId) {
        return transactions.inTransaction(() -> repository.listSourceItems(queueName, sourceId).stream().map(this::toItemResponse).toList());
    }

    /**
     * Lists dead-lettered items with normalized pagination.
     */
    @Override
    public List<ItemResponse> deadLetteredItems(String queueName, int limit, int offset) {
        return transactions.inTransaction(() -> repository.listDeadLettered(queueName, normalizeLimit(limit), normalizeOffset(offset)).stream().map(this::toItemResponse).toList());
    }

    /**
     * Lists blocked sources.
     */
    @Override
    public List<SourceResponse> blockedSources(String queueName) {
        return transactions.inTransaction(() -> repository.blockedSources(queueName).stream().map(this::toSourceResponse).toList());
    }

    /**
     * Lists blocked sources with head-item context.
     */
    @Override
    public List<BlockedSourceResponse> inspectBlockedSources(String queueName, int limit, int offset) {
        return transactions.inTransaction(() -> repository.inspectBlockedSources(queueName, normalizeLimit(limit), normalizeOffset(offset)).stream().map(this::toBlockedSourceResponse).toList());
    }

    /**
     * Unblocks a source without audit actor details.
     */
    @Override
    public SourceResponse unblockSource(String queueName, String sourceId) {
        return unblockSource(queueName, sourceId, null, null);
    }

    /**
     * Unblocks a source only when no blocking head item remains, then records audit.
     */
    @Override
    public SourceResponse unblockSource(String queueName, String sourceId, String actorId, String reason) {
        validateAdminReason(reason, queueName);
        return transactions.inTransaction(() -> {
            OffsetDateTime now = now();
            SourceStateRow source = repository.lockSource(queueName, sourceId);
            if (source.status() != SourceStatus.blocked) {
                throw new QueueException(QueueErrorCode.SOURCE_BLOCKED, QueueException.CONFLICT, "source is not blocked").withContext(queueName, sourceId, null);
            }
            Optional<QueueItemRow> head = repository.findHeadBlockingItem(queueName, sourceId);
            if (head.isPresent()) {
                throw new QueueException(QueueErrorCode.SOURCE_BLOCKED, QueueException.CONFLICT, "blocking head remains; use retry, skip, or cancel on the item").withContext(queueName, sourceId, head.get().itemId());
            }
            repository.releaseSource(queueName, sourceId, now);
            SourceStateRow released = repository.findSource(queueName, sourceId);
            insertAudit(actorId, "unblock", queueName, sourceId, null, source.status().name(), released.status().name(), reason, Map.of());
            return toSourceResponse(released);
        });
    }

    /**
     * Retries a dead-lettered head item without audit actor details.
     */
    @Override
    public ItemResponse retry(String queueName, UUID itemId) {
        return retry(queueName, itemId, null, null);
    }

    /**
     * Retries a dead-lettered head item and records audit.
     */
    @Override
    public ItemResponse retry(String queueName, UUID itemId, String actorId, String reason) {
        return transactions.inTransaction(() -> adminRepair(queueName, itemId, ItemStatus.retry_wait, List.of(ItemStatus.dead_lettered), "retry", actorId, reason));
    }

    /**
     * Skips an allowed head item without audit actor details.
     */
    @Override
    public ItemResponse skip(String queueName, UUID itemId) {
        return skip(queueName, itemId, null, null);
    }

    /**
     * Skips an allowed head item and records audit.
     */
    @Override
    public ItemResponse skip(String queueName, UUID itemId, String actorId, String reason) {
        return transactions.inTransaction(() -> adminRepair(queueName, itemId, ItemStatus.skipped, List.of(ItemStatus.pending, ItemStatus.retry_wait, ItemStatus.dead_lettered), "skip", actorId, reason));
    }

    /**
     * Cancels an allowed head item without audit actor details.
     */
    @Override
    public ItemResponse cancel(String queueName, UUID itemId) {
        return cancel(queueName, itemId, null, null);
    }

    /**
     * Cancels an allowed head item and records audit.
     */
    @Override
    public ItemResponse cancel(String queueName, UUID itemId, String actorId, String reason) {
        return transactions.inTransaction(() -> adminRepair(queueName, itemId, ItemStatus.cancelled, List.of(ItemStatus.pending, ItemStatus.retry_wait, ItemStatus.dead_lettered), "cancel", actorId, reason));
    }

    /**
     * Runs retention purge without explicit actor details.
     */
    @Override
    public RetentionPurgeResponse purgeRetention(String queueName, RetentionPurgeRequest request) {
        return purgeRetention(queueName, request, null);
    }

    /**
     * Runs bounded manual retention purge and audits real deletes transactionally.
     */
    @Override
    public RetentionPurgeResponse purgeRetention(String queueName, RetentionPurgeRequest request, String actorId) {
        requireRequest(request);
        requireText(queueName, "queueName");
        validateAdminReason(request.reason(), queueName);
        if (request.olderThan() == null) {
            throw new QueueException(QueueException.BAD_REQUEST, "olderThan is required").withQueueName(queueName);
        }
        List<ItemStatus> statuses = retentionStatuses(request.statuses(), queueName);
        int limit = retentionLimit(request.limit(), queueName);
        boolean dryRun = Boolean.TRUE.equals(request.dryRun());
        return transactions.inTransaction(() -> {
            long matched = repository.countRetentionEligible(queueName, request.olderThan(), statuses, limit);
            if (dryRun) {
                return new RetentionPurgeResponse(queueName, true, matched, 0);
            }
            long deleted = repository.deleteRetentionEligible(queueName, request.olderThan(), statuses, limit);
            insertAudit(actorId, "retention_purge", queueName, null, null, null, null, request.reason(), Map.of(
                "olderThan", request.olderThan().toString(),
                "statuses", statuses.stream().map(Enum::name).toList(),
                "limit", limit,
                "matched", matched,
                "deleted", deleted
            ));
            return new RetentionPurgeResponse(queueName, false, matched, deleted);
        });
    }

    /**
     * Lists admin audit rows with normalized pagination.
     */
    @Override
    public List<AdminAuditResponse> adminAudit(String queueName, int limit, int offset) {
        return transactions.inTransaction(() -> repository.listAdminAudit(queueName, normalizeLimit(limit), normalizeOffset(offset)).stream().map(this::toAdminAuditResponse).toList());
    }

    /**
     * Recovers expired processing leases when source/item lease identity is safe to mutate.
     */
    @Override
    public int recoverExpiredLeases() {
        return transactions.inTransaction(() -> {
            OffsetDateTime now = now();
            int recovered = 0;
            for (QueueItemRow candidate : repository.expiredProcessing(now, 100)) {
                if (recoverExpiredItem(candidate, now)) {
                    recovered++;
                }
            }
            return recovered;
        });
    }

    /**
     * Reads schema compatibility information.
     */
    @Override
    public QueueSchemaInfo getSchemaInfo() {
        return transactions.inTransaction(repository::getSchemaInfo);
    }

    /**
     * Reads aggregate queue/source metrics.
     */
    @Override
    public QueueMetricsSnapshot metricsSnapshot() {
        return transactions.inTransaction(repository::metricsSnapshot);
    }

    /**
     * Performs enqueue validation, source-first locking, sequence allocation, and insert.
     */
    private EnqueueResponse enqueueInTransaction(String queueName, EnqueueRequest request) {
        requireText(queueName, "queueName");
        requireText(request.sourceId(), "sourceId");
        requireText(request.itemType(), "itemType");
        if (request.maxAttempts() != null && request.maxAttempts() < 1) {
            throw new QueueException(QueueException.BAD_REQUEST, "maxAttempts must be >= 1");
        }
        String payloadJson = writeJson(defaultMap(request.payload()));
        String headersJson = writeJson(defaultMap(request.headers()));
        requireMaxBytes(payloadJson, settings.maxPayloadBytes(), "payload", queueName);
        requireMaxBytes(headersJson, settings.maxHeadersBytes(), "headers", queueName);

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

        QueueItemRow item = repository.insertItem(UUID.randomUUID(), queueName, request.sourceId(), sequenceNo, request.itemType(), payloadJson, headersJson, request.availableAt() == null ? now : request.availableAt(), request.maxAttempts() == null ? settings.defaultMaxAttempts() : request.maxAttempts(), blankToNull(request.idempotencyKey()), now);
        return toEnqueueResponse(item);
    }

    /**
     * Performs claim validation and source-aware head-item claim.
     */
    private ClaimResponse claimInTransaction(String queueName, ClaimRequest request) {
        requireText(queueName, "queueName");
        requireText(request.workerId(), "workerId");
        if (request.supportedItemTypes() == null || request.supportedItemTypes().isEmpty()) {
            throw new QueueException(QueueException.BAD_REQUEST, "supportedItemTypes is required");
        }
        if (request.maxItems() != null && request.maxItems() > 1) {
            throw new QueueException(QueueException.BAD_REQUEST, "MVP supports maxItems <= 1");
        }
        int leaseSeconds = request.leaseSeconds() == null ? settings.defaultLeaseSeconds() : request.leaseSeconds();
        if (leaseSeconds <= 0) {
            throw new QueueException(QueueException.BAD_REQUEST, "leaseSeconds must be > 0");
        }
        if (leaseSeconds > settings.maxLeaseSeconds()) {
            throw new QueueException(QueueException.BAD_REQUEST, "leaseSeconds must be <= " + settings.maxLeaseSeconds()).withQueueName(queueName);
        }

        repository.blockDeadLetteredHeadSources(queueName, now());
        UUID leaseId = UUID.randomUUID();
        OffsetDateTime leaseUntil = now().plusSeconds(leaseSeconds);
        return repository.claimHeadItem(queueName, request.supportedItemTypes(), request.workerId(), leaseId, leaseUntil, now())
            .map(item -> new ClaimResponse(leaseId, item.queueName(), item.sourceId(), leaseUntil, List.of(new ClaimItem(item.itemId(), item.sequenceNo(), item.itemType(), readJson(item.payloadJson()), readJson(item.headersJson())))))
            .orElseGet(ClaimResponse::empty);
    }

    /**
     * Applies failure semantics after lease verification.
     */
    private ItemResponse failInTransaction(String queueName, UUID itemId, FailRequest request) {
        if (request.backoffSeconds() != null && request.backoffSeconds() < 0) {
            throw new QueueException(QueueException.BAD_REQUEST, "backoffSeconds must be >= 0");
        }
        requireMaxBytes(request.errorType(), settings.maxErrorTypeBytes(), "errorType", queueName, itemId);
        requireMaxBytes(request.errorMessage(), settings.maxErrorMessageBytes(), "errorMessage", queueName, itemId);
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

    /**
     * Verifies that the item and source are both leased to the supplied worker/lease identity.
     */
    private QueueItemRow verifyLease(String queueName, UUID itemId, String workerId, UUID leaseId) {
        requireText(workerId, "workerId");
        if (leaseId == null) {
            throw new QueueException(QueueException.BAD_REQUEST, "leaseId is required");
        }
        QueueItemRow candidate = repository.findItem(queueName, itemId)
            .orElseThrow(() -> new QueueException(QueueErrorCode.ITEM_NOT_FOUND, QueueException.NOT_FOUND, "item not found").withContext(queueName, null, itemId));
        SourceStateRow source = repository.lockSource(candidate.queueName(), candidate.sourceId());
        QueueItemRow item = repository.lockItem(queueName, itemId);
        if (!item.sourceId().equals(candidate.sourceId())) {
            throw new QueueException(QueueErrorCode.QUEUE_CONFLICT, QueueException.CONFLICT, "item source changed while locking").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        if (item.status() != ItemStatus.processing) {
            throw new QueueException(QueueErrorCode.ITEM_NOT_PROCESSING, QueueException.CONFLICT, "item is not processing").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        if (!workerId.equals(item.claimedBy()) || !leaseId.equals(item.leaseId())) {
            throw new QueueException(QueueErrorCode.LEASE_LOST, QueueException.CONFLICT, "lease is not held by worker").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        if (item.leaseUntil() == null || !item.leaseUntil().isAfter(now())) {
            throw new QueueException(QueueErrorCode.LEASE_EXPIRED, QueueException.CONFLICT, "lease has expired").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        if (source.status() != SourceStatus.leased || !workerId.equals(source.leasedBy()) || !leaseId.equals(source.leaseId())) {
            throw new QueueException(QueueErrorCode.LEASE_LOST, QueueException.CONFLICT, "source lease is not held by worker").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        return item;
    }

    /**
     * Applies source-first admin repair for retry, skip, and cancel.
     */
    private ItemResponse adminRepair(String queueName, UUID itemId, ItemStatus targetStatus, List<ItemStatus> allowedStatuses, String operation, String actorId, String reason) {
        validateAdminReason(reason, queueName);
        QueueItemRow candidate = repository.findItem(queueName, itemId)
            .orElseThrow(() -> new QueueException(QueueErrorCode.ITEM_NOT_FOUND, QueueException.NOT_FOUND, "item not found").withContext(queueName, null, itemId));
        SourceStateRow source = repository.lockSource(candidate.queueName(), candidate.sourceId());
        QueueItemRow item = repository.lockItem(queueName, itemId);
        ensureAdminRepairAllowed(item, candidate, source, allowedStatuses);
        OffsetDateTime now = now();
        QueueItemRow updated = repository.adminStatus(item.itemId(), targetStatus, now, now);
        repository.releaseSource(item.queueName(), item.sourceId(), now);
        insertAudit(actorId, operation, item.queueName(), item.sourceId(), item.itemId(), item.status().name(), updated.status().name(), reason, Map.of());
        return toItemResponse(updated);
    }

    /**
     * Revalidates admin repair preconditions after source and item locks are held.
     */
    private void ensureAdminRepairAllowed(QueueItemRow item, QueueItemRow candidate, SourceStateRow source, List<ItemStatus> allowedStatuses) {
        if (!item.sourceId().equals(candidate.sourceId())) {
            throw new QueueException(QueueErrorCode.QUEUE_CONFLICT, QueueException.CONFLICT, "item source changed while locking").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        if (!allowedStatuses.contains(item.status())) {
            throw new QueueException(QueueErrorCode.QUEUE_CONFLICT, QueueException.CONFLICT, "admin repair is not allowed for item status " + item.status()).withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        if (source.status() == SourceStatus.leased) {
            throw new QueueException(QueueErrorCode.QUEUE_CONFLICT, QueueException.CONFLICT, "admin repair cannot modify an actively leased source").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
        QueueItemRow head = repository.findHeadBlockingItem(item.queueName(), item.sourceId())
            .orElseThrow(() -> new QueueException(QueueErrorCode.QUEUE_CONFLICT, QueueException.CONFLICT, "source has no blocking head item").withContext(item.queueName(), item.sourceId(), item.itemId()));
        if (!head.itemId().equals(item.itemId())) {
            throw new QueueException(QueueErrorCode.QUEUE_CONFLICT, QueueException.CONFLICT, "admin repair item is not the source head item").withContext(item.queueName(), item.sourceId(), item.itemId());
        }
    }

    /**
     * Recovers one expired item only when source state matches the item lease or is safely idle.
     */
    private boolean recoverExpiredItem(QueueItemRow candidate, OffsetDateTime now) {
        SourceStateRow source = repository.lockSource(candidate.queueName(), candidate.sourceId());
        QueueItemRow item = repository.lockItem(candidate.queueName(), candidate.itemId());
        if (item.status() != ItemStatus.processing || item.leaseUntil() == null || !item.leaseUntil().isBefore(now)) {
            return false;
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
            return false;
        }

        boolean exhausted = item.attemptCount() >= item.maxAttempts();
        if (exhausted) {
            repository.fail(item.itemId(), ItemStatus.dead_lettered, now, "LEASE_EXPIRED", "Worker lease expired", now);
            repository.blockSource(item.queueName(), item.sourceId(), now);
        } else {
            repository.fail(item.itemId(), ItemStatus.retry_wait, now.plus(retryPolicy.nextBackoff(item.attemptCount())), "LEASE_EXPIRED", "Worker lease expired", now);
            repository.releaseSource(item.queueName(), item.sourceId(), now);
        }
        return true;
    }

    /**
     * Maps an item row to the enqueue response contract.
     */
    private EnqueueResponse toEnqueueResponse(QueueItemRow item) {
        return new EnqueueResponse(item.itemId(), item.queueName(), item.sourceId(), item.sequenceNo(), item.status().name());
    }

    /**
     * Maps an item row to the public item response contract.
     */
    private ItemResponse toItemResponse(QueueItemRow item) {
        return new ItemResponse(item.itemId(), item.queueName(), item.sourceId(), item.sequenceNo(), item.itemType(), readJson(item.payloadJson()), readJson(item.headersJson()), item.status().name(), item.availableAt(), item.claimedBy(), item.leaseId(), item.leaseUntil(), item.attemptCount(), item.maxAttempts(), item.idempotencyKey(), item.lastErrorType(), item.lastErrorMessage(), readJson(item.resultJson()), item.createdAt(), item.updatedAt());
    }

    /**
     * Maps a source row to the public source response contract.
     */
    private SourceResponse toSourceResponse(SourceStateRow source) {
        return new SourceResponse(source.queueName(), source.sourceId(), source.nextSequenceNo(), source.status().name(), source.leasedBy(), source.leaseId(), source.leaseUntil(), source.updatedAt());
    }

    /**
     * Maps a blocked source row to the admin inspection response.
     */
    private BlockedSourceResponse toBlockedSourceResponse(BlockedSourceRow source) {
        return new BlockedSourceResponse(source.queueName(), source.sourceId(), source.status().name(), source.leasedBy(), source.leaseUntil(), source.headItemId(), source.headItemStatus() == null ? null : source.headItemStatus().name(), source.updatedAt());
    }

    /**
     * Maps an audit row to the admin audit response.
     */
    private AdminAuditResponse toAdminAuditResponse(AdminAuditRow audit) {
        return new AdminAuditResponse(audit.auditId(), audit.occurredAt(), audit.actorId(), audit.operation(), audit.queueName(), audit.sourceId(), audit.itemId(), audit.previousStatus(), audit.newStatus(), audit.reason(), readJson(audit.metadataJson()));
    }

    /**
     * Writes an admin audit record using compact JSON metadata.
     */
    private void insertAudit(String actorId, String operation, String queueName, String sourceId, UUID itemId, String previousStatus, String newStatus, String reason, Map<String, Object> metadata) {
        String metadataJson = writeJson(defaultMap(metadata));
        requireMaxBytes(metadataJson, settings.maxAdminMetadataBytes(), "adminMetadata", queueName, itemId);
        repository.insertAdminAudit(UUID.randomUUID(), now(), blankToNull(actorId), operation, queueName, sourceId, itemId, previousStatus, newStatus, blankToNull(reason), metadataJson);
    }

    /**
     * Returns the current time from the injected clock.
     */
    private OffsetDateTime now() {
        return OffsetDateTime.now(clock);
    }

    /**
     * Normalizes nullable JSON maps to empty objects.
     */
    private Map<String, Object> defaultMap(Map<String, Object> value) {
        return value == null ? Map.of() : value;
    }

    /**
     * Converts blank strings to null for optional persistence fields.
     */
    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /**
     * Validates and caps read-side pagination limits.
     */
    private int normalizeLimit(int limit) {
        if (limit < 1) {
            throw new QueueException(QueueException.BAD_REQUEST, "limit must be >= 1");
        }
        return Math.min(limit, 500);
    }

    /**
     * Validates read-side pagination offsets.
     */
    private int normalizeOffset(int offset) {
        if (offset < 0) {
            throw new QueueException(QueueException.BAD_REQUEST, "offset must be >= 0");
        }
        return offset;
    }

    /**
     * Requires a non-blank text field.
     */
    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new QueueException(QueueException.BAD_REQUEST, field + " is required");
        }
    }

    /**
     * Requires a non-null request body.
     */
    private void requireRequest(Object request) {
        if (request == null) {
            throw new QueueException(QueueException.BAD_REQUEST, "request body is required");
        }
    }

    /**
     * Requires a string value to fit within a configured UTF-8 byte limit.
     */
    private void requireMaxBytes(String value, int maxBytes, String field, String queueName) {
        requireMaxBytes(value, maxBytes, field, queueName, null);
    }

    /**
     * Requires a string value to fit within a configured UTF-8 byte limit and carries item context.
     */
    private void requireMaxBytes(String value, int maxBytes, String field, String queueName, UUID itemId) {
        if (value != null) {
            int actualBytes = value.getBytes(StandardCharsets.UTF_8).length;
            if (actualBytes > maxBytes) {
                throw new QueueFieldTooLargeException(field, maxBytes, actualBytes).withContext(queueName, null, itemId);
            }
        }
    }

    /**
     * Validates admin audit reason size.
     */
    private void validateAdminReason(String reason, String queueName) {
        requireMaxBytes(reason, settings.maxAdminReasonBytes(), "adminReason", queueName);
    }

    /**
     * Parses and validates retention purge statuses.
     */
    private List<ItemStatus> retentionStatuses(List<String> requestedStatuses, String queueName) {
        if (requestedStatuses == null || requestedStatuses.isEmpty()) {
            throw new QueueException(QueueException.BAD_REQUEST, "statuses is required").withQueueName(queueName);
        }
        return requestedStatuses.stream()
            .map(status -> retentionStatus(status, queueName))
            .distinct()
            .toList();
    }

    /**
     * Converts one retention status string and rejects non-purgeable statuses.
     */
    private ItemStatus retentionStatus(String status, String queueName) {
        if (status == null || status.isBlank()) {
            throw new QueueException(QueueException.BAD_REQUEST, "status is required").withQueueName(queueName);
        }
        ItemStatus itemStatus;
        try {
            itemStatus = ItemStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            throw new QueueException(QueueException.BAD_REQUEST, "unknown retention status " + status).withQueueName(queueName);
        }
        if (!RETENTION_PURGE_STATUSES.contains(itemStatus)) {
            throw new QueueException(QueueException.BAD_REQUEST, "retention purge is not allowed for status " + status).withQueueName(queueName);
        }
        return itemStatus;
    }

    /**
     * Applies default and maximum retention purge limits.
     */
    private int retentionLimit(Integer requestedLimit, String queueName) {
        int limit = requestedLimit == null ? 1000 : requestedLimit;
        if (limit < 1) {
            throw new QueueException(QueueException.BAD_REQUEST, "limit must be >= 1").withQueueName(queueName);
        }
        if (limit > settings.maxRetentionPurgeBatchSize()) {
            throw new QueueException(QueueException.BAD_REQUEST, "limit must be <= " + settings.maxRetentionPurgeBatchSize()).withQueueName(queueName);
        }
        return limit;
    }

    /**
     * Serializes a JSON-compatible map for persistence.
     */
    private String writeJson(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (Exception e) {
            throw new QueueException(QueueException.BAD_REQUEST, "invalid json payload");
        }
    }

    /**
     * Deserializes persisted JSON into a map response value.
     */
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
