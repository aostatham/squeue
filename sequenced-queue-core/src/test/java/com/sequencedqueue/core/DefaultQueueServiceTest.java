package com.sequencedqueue.core;

import static com.sequencedqueue.core.QueueDtos.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Test coverage for DefaultQueueServiceTest.
 */
class DefaultQueueServiceTest {
    /**
     * Verifies enqueue rereads existing item after duplicate idempotency race.
     */
    @Test
    void enqueueRereadsExistingItemAfterDuplicateIdempotencyRace() {
        FakeRepository repository = new FakeRepository();
        UUID existingId = UUID.randomUUID();
        repository.duplicateOnInsert = true;
        repository.existingAfterDuplicate = item(existingId, "q", "s", 1, ItemStatus.pending);

        DefaultQueueService service = service(repository);

        EnqueueResponse response = service.enqueue("q", new EnqueueRequest("s", "type", "idem-1", null, null, null, null));

        assertEquals(existingId, response.itemId());
        assertEquals(1, repository.insertAttempts);
        assertEquals(3, repository.findByIdempotencyCalls);
    }

    /**
     * Verifies claim marks idle dead lettered heads blocked before trying to claim.
     */
    @Test
    void claimMarksIdleDeadLetteredHeadsBlockedBeforeTryingToClaim() {
        FakeRepository repository = new FakeRepository();
        DefaultQueueService service = service(repository);

        service.claim("q", new ClaimRequest("worker-1", List.of("type"), 60, 1));

        assertEquals(1, repository.blockDeadLetteredHeadSourcesCalls);
    }

    /**
     * Verifies unblock does not release leased source.
     */
    @Test
    void unblockDoesNotReleaseLeasedSource() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.leased);
        DefaultQueueService service = service(repository);

        QueueException error = assertThrows(QueueException.class, () -> service.unblockSource("q", "s"));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(0, repository.releaseSourceCalls);
    }

    /**
     * Verifies unblock source rejects when dead lettered head remains.
     */
    @Test
    void unblockSourceRejectsWhenDeadLetteredHeadRemains() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.blocked);
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.dead_lettered);
        DefaultQueueService service = service(repository);

        QueueException error = assertThrows(QueueException.class, () -> service.unblockSource("q", "s"));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(0, repository.releaseSourceCalls);
        assertEquals(0, repository.skipDeadLetteredHeadCalls);
    }

    /**
     * Verifies unblock source rejects when processing head remains.
     */
    @Test
    void unblockSourceRejectsWhenProcessingHeadRemains() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.blocked);
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.processing);
        DefaultQueueService service = service(repository);

        QueueException error = assertThrows(QueueException.class, () -> service.unblockSource("q", "s"));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(0, repository.releaseSourceCalls);
    }

    /**
     * Verifies unblock source does not change item status.
     */
    @Test
    void unblockSourceDoesNotChangeItemStatus() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.blocked);
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.failed);
        DefaultQueueService service = service(repository);

        service.unblockSource("q", "s");

        assertEquals(1, repository.releaseSourceCalls);
        assertEquals(0, repository.adminStatusCalls);
        assertEquals(0, repository.skipDeadLetteredHeadCalls);
    }

    /**
     * Verifies unblock source succeeds only after dead lettered head was repaired.
     */
    @Test
    void unblockSourceSucceedsOnlyAfterDeadLetteredHeadWasRepaired() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.blocked);
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.skipped);
        DefaultQueueService service = service(repository);

        service.unblockSource("q", "s");

        assertEquals(1, repository.releaseSourceCalls);
    }

    /**
     * Verifies admin skip rejects processing item and does not release source.
     */
    @Test
    void adminSkipRejectsProcessingItemAndDoesNotReleaseSource() {
        FakeRepository repository = new FakeRepository();
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.processing);
        DefaultQueueService service = service(repository);

        QueueException error = assertThrows(QueueException.class, () -> service.skip("q", repository.lockedItem.itemId()));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(0, repository.releaseSourceCalls);
    }

    /**
     * Verifies recovery releases source only when lease matches expired item.
     */
    @Test
    void recoveryReleasesSourceOnlyWhenLeaseMatchesExpiredItem() {
        FakeRepository repository = new FakeRepository();
        UUID leaseId = UUID.randomUUID();
        QueueItemRow expired = new QueueItemRow(UUID.randomUUID(), "q", "s", 1, "type", "{}", "{}", ItemStatus.processing, now(), "worker-1", leaseId, now().minusSeconds(1), 1, 5, null, null, null, null, now(), now());
        repository.lockedItem = expired;
        repository.source = new SourceStateRow("q", "s", 1, SourceStatus.leased, "worker-1", leaseId, now().minusSeconds(1), now(), now());
        repository.expiredItems.add(expired);
        DefaultQueueService service = service(repository);

        service.recoverExpiredLeases();

        assertEquals(1, repository.failCalls);
        assertEquals(0, repository.releaseSourceIfLeaseMatchesCalls);
        assertEquals(1, repository.releaseSourceCalls);
    }

    /**
     * Verifies rejects null request bodies.
     */
    @Test
    void rejectsNullRequestBodies() {
        DefaultQueueService service = service(new FakeRepository());

        assertBadRequest(() -> service.enqueue("q", null));
        assertBadRequest(() -> service.claim("q", null));
        assertBadRequest(() -> service.complete("q", UUID.randomUUID(), null));
        assertBadRequest(() -> service.fail("q", UUID.randomUUID(), null));
        assertBadRequest(() -> service.heartbeat("q", UUID.randomUUID(), null));
        assertBadRequest(() -> service.purgeRetention("q", null));
    }

    /**
     * Verifies rejects invalid numeric request values.
     */
    @Test
    void rejectsInvalidNumericRequestValues() {
        DefaultQueueService service = service(new FakeRepository());

        assertBadRequest(() -> service.enqueue("q", new EnqueueRequest("s", "type", null, null, null, null, 0)));
        assertBadRequest(() -> service.claim("q", new ClaimRequest("worker-1", List.of("type"), 0, 1)));
        assertBadRequest(() -> service.claim("q", new ClaimRequest("worker-1", List.of("type"), -1, 1)));
        assertBadRequest(() -> service.heartbeat("q", UUID.randomUUID(), new HeartbeatRequest("worker-1", 0)));
        assertBadRequest(() -> service.heartbeat("q", UUID.randomUUID(), new HeartbeatRequest("worker-1", -1)));
        assertBadRequest(() -> service.fail("q", UUID.randomUUID(), new FailRequest("worker-1", UUID.randomUUID(), true, "ERR", "bad", -1)));
    }

    /**
     * Verifies payload within limit is accepted and omitted max attempts uses default.
     */
    @Test
    void payloadWithinLimitIsAcceptedAndOmittedMaxAttemptsUsesDefault() {
        FakeRepository repository = new FakeRepository();
        DefaultQueueService service = service(repository, new QueueSettings(60, 120, 7, 32, 32, 32, 32, 100));

        EnqueueResponse response = service.enqueue("q", new EnqueueRequest("s", "type", null, Map.of("x", "y"), Map.of(), null, null));

        assertEquals(1, response.sequenceNo());
        assertEquals(7, repository.lastInsertMaxAttempts);
    }

    /**
     * Verifies enqueue rejects payload over limit.
     */
    @Test
    void enqueueRejectsPayloadOverLimit() {
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 8, 32, 32, 32, 100));

        QueueFieldTooLargeException error = assertFieldTooLarge(() -> service.enqueue("q", new EnqueueRequest("s", "type", null, Map.of("tooLarge", "value"), Map.of(), null, null)));

        assertEquals("payload", error.fieldName());
        assertEquals(8, error.maxBytes());
        assertEquals("q", error.queueName());
    }

    /**
     * Verifies enqueue rejects headers over limit.
     */
    @Test
    void enqueueRejectsHeadersOverLimit() {
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 64, 8, 32, 32, 100));

        QueueFieldTooLargeException error = assertFieldTooLarge(() -> service.enqueue("q", new EnqueueRequest("s", "type", null, Map.of(), Map.of("tooLarge", "value"), null, null)));

        assertEquals("headers", error.fieldName());
        assertEquals(8, error.maxBytes());
    }

    /**
     * Verifies complete rejects result over limit.
     */
    @Test
    void completeRejectsResultOverLimit() {
        UUID itemId = UUID.randomUUID();
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 64, 64, 8, 32, 32, 32, 32, 100));

        QueueFieldTooLargeException error = assertFieldTooLarge(() -> service.complete("q", itemId, new CompleteRequest("worker-1", UUID.randomUUID(), Map.of("tooLarge", "value"))));

        assertEquals("result", error.fieldName());
        assertEquals(8, error.maxBytes());
        assertEquals(itemId, error.itemId());
    }

    /**
     * Verifies fail rejects error message over limit.
     */
    @Test
    void failRejectsErrorMessageOverLimit() {
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 64, 64, 4, 32, 100));

        QueueFieldTooLargeException error = assertFieldTooLarge(() -> service.fail("q", UUID.randomUUID(), new FailRequest("worker-1", UUID.randomUUID(), false, "ERR", "12345", null)));

        assertEquals("errorMessage", error.fieldName());
        assertEquals(4, error.maxBytes());
        assertFalse(error.getMessage().contains("12345"));
    }

    /**
     * Verifies fail rejects error type over limit.
     */
    @Test
    void failRejectsErrorTypeOverLimit() {
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 64, 64, 32, 4, 32, 32, 32, 100));

        QueueFieldTooLargeException error = assertFieldTooLarge(() -> service.fail("q", UUID.randomUUID(), new FailRequest("worker-1", UUID.randomUUID(), false, "TOO_LONG", "safe", null)));

        assertEquals("errorType", error.fieldName());
        assertEquals(4, error.maxBytes());
        assertFalse(error.getMessage().contains("TOO_LONG"));
    }

    /**
     * Verifies claim rejects lease seconds over max.
     */
    @Test
    void claimRejectsLeaseSecondsOverMax() {
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 64, 64, 32, 32, 100));

        assertBadRequest(() -> service.claim("q", new ClaimRequest("worker-1", List.of("type"), 121, 1)));
    }

    /**
     * Verifies admin operations reject reason over limit.
     */
    @Test
    void adminOperationsRejectReasonOverLimit() {
        FakeRepository repository = new FakeRepository();
        DefaultQueueService service = service(repository, new QueueSettings(60, 120, 5, 64, 64, 32, 4, 100));
        String reason = "12345";

        assertEquals("adminReason", assertFieldTooLarge(() -> service.retry("q", repository.lockedItem.itemId(), "admin", reason)).fieldName());
        assertEquals("adminReason", assertFieldTooLarge(() -> service.skip("q", repository.lockedItem.itemId(), "admin", reason)).fieldName());
        assertEquals("adminReason", assertFieldTooLarge(() -> service.cancel("q", repository.lockedItem.itemId(), "admin", reason)).fieldName());
        assertEquals("adminReason", assertFieldTooLarge(() -> service.unblockSource("q", "s", "admin", reason)).fieldName());
        assertEquals("adminReason", assertFieldTooLarge(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("succeeded"), false, reason), "admin")).fieldName());
    }

    /**
     * Verifies admin audit metadata is size limited.
     */
    @Test
    void adminAuditMetadataOverLimitIsRejected() {
        FakeRepository repository = new FakeRepository();
        repository.retentionMatched = 1;
        repository.retentionDeleted = 1;
        DefaultQueueService service = service(repository, new QueueSettings(60, 120, 5, 64, 64, 64, 64, 64, 64, 4, 100));

        QueueFieldTooLargeException error = assertFieldTooLarge(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("succeeded"), false, "ok", 1), "admin"));

        assertEquals("adminMetadata", error.fieldName());
        assertEquals(4, error.maxBytes());
    }

    /**
     * Verifies retention purge rejects blocking statuses.
     */
    @Test
    void retentionPurgeRejectsBlockingStatuses() {
        DefaultQueueService service = service(new FakeRepository());

        assertBadRequest(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("pending"), false, "cleanup")));
        assertBadRequest(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("processing"), false, "cleanup")));
        assertBadRequest(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("retry_wait"), false, "cleanup")));
        assertBadRequest(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("dead_lettered"), false, "cleanup")));
    }

    /**
     * Verifies retention purge rejects zero limit.
     */
    @Test
    void retentionPurgeRejectsZeroLimit() {
        DefaultQueueService service = service(new FakeRepository());

        assertBadRequest(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("succeeded"), false, "cleanup", 0)));
    }

    /**
     * Verifies retention purge rejects limit above max.
     */
    @Test
    void retentionPurgeRejectsLimitAboveMax() {
        DefaultQueueService service = service(new FakeRepository(), new QueueSettings(60, 120, 5, 64, 64, 32, 32, 10));

        assertBadRequest(() -> service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("succeeded"), false, "cleanup", 11)));
    }

    /**
     * Verifies retention dry run counts without deleting or auditing.
     */
    @Test
    void retentionDryRunCountsWithoutDeletingOrAuditing() {
        FakeRepository repository = new FakeRepository();
        repository.retentionMatched = 7;
        DefaultQueueService service = service(repository);

        RetentionPurgeResponse response = service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("succeeded", "failed"), true, "cleanup"), "admin-api-key");

        assertEquals(7, response.matched());
        assertEquals(0, response.deleted());
        assertEquals(1000, repository.retentionLimit);
        assertEquals(0, repository.retentionDeleteCalls);
        assertEquals(0, repository.adminAuditCalls);
    }

    /**
     * Verifies retention actual purge deletes eligible statuses and writes audit.
     */
    @Test
    void retentionActualPurgeDeletesEligibleStatusesAndWritesAudit() {
        FakeRepository repository = new FakeRepository();
        repository.retentionMatched = 5;
        repository.retentionDeleted = 4;
        DefaultQueueService service = service(repository);

        RetentionPurgeResponse response = service.purgeRetention("q", new RetentionPurgeRequest(now(), List.of("succeeded", "cancelled", "skipped", "failed"), false, "cleanup", 3), "admin-api-key");

        assertEquals(5, response.matched());
        assertEquals(4, response.deleted());
        assertEquals(3, repository.retentionLimit);
        assertEquals(1, repository.retentionDeleteCalls);
        assertEquals(1, repository.adminAuditCalls);
        assertEquals("retention_purge", repository.auditRows.getFirst().operation());
    }

    private DefaultQueueService service(FakeRepository repository) {
        return service(repository, QueueSettings.defaults());
    }

    private DefaultQueueService service(FakeRepository repository, QueueSettings settings) {
        TransactionRunner transactions = new TransactionRunner() {
            @Override
            public <T> T inTransaction(TransactionCallback<T> callback) {
                return callback.execute();
            }
        };
        return new DefaultQueueService(repository, transactions, new RetryPolicy(), new ObjectMapper(), Clock.fixed(now().toInstant(), ZoneOffset.UTC), settings);
    }

    private static OffsetDateTime now() {
        return OffsetDateTime.parse("2026-06-11T12:00:00Z");
    }

    private static QueueItemRow item(UUID itemId, String queueName, String sourceId, long sequenceNo, ItemStatus status) {
        return new QueueItemRow(itemId, queueName, sourceId, sequenceNo, "type", "{}", "{}", status, now(), null, null, null, 0, 5, "idem-1", null, null, null, now(), now());
    }

    private static SourceStateRow source(SourceStatus status) {
        return new SourceStateRow("q", "s", 1, status, status == SourceStatus.leased ? "worker-1" : null, status == SourceStatus.leased ? UUID.randomUUID() : null, status == SourceStatus.leased ? now().plusSeconds(60) : null, now(), now());
    }

    private static void assertBadRequest(Executable executable) {
        QueueException error = assertThrows(QueueException.class, executable);
        assertEquals(QueueException.BAD_REQUEST, error.statusCode());
    }

    private static QueueFieldTooLargeException assertFieldTooLarge(Executable executable) {
        QueueFieldTooLargeException error = assertThrows(QueueFieldTooLargeException.class, executable);
        assertEquals(QueueException.BAD_REQUEST, error.statusCode());
        assertEquals(QueueErrorCode.FIELD_TOO_LARGE, error.errorCode());
        return error;
    }

    private static final class FakeRepository implements QueueRepository {
        SourceStateRow source = source(SourceStatus.idle);
        QueueItemRow lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.pending);
        QueueItemRow existingAfterDuplicate;
        boolean duplicateOnInsert;
        int findByIdempotencyCalls;
        int insertAttempts;
        int lastInsertMaxAttempts;
        int blockDeadLetteredHeadSourcesCalls;
        int releaseSourceCalls;
        int releaseSourceIfLeaseMatchesCalls;
        int failCalls;
        int adminStatusCalls;
        int skipDeadLetteredHeadCalls;
        int adminAuditCalls;
        int retentionDeleteCalls;
        long retentionMatched;
        long retentionDeleted;
        int retentionLimit;
        UUID lastMatchedLeaseId;
        final List<QueueItemRow> expiredItems = new ArrayList<>();
        final List<AdminAuditRow> auditRows = new ArrayList<>();

        @Override
        public Optional<QueueItemRow> findByIdempotencyKey(String queueName, String idempotencyKey) {
            findByIdempotencyCalls++;
            return findByIdempotencyCalls > 2 ? Optional.ofNullable(existingAfterDuplicate) : Optional.empty();
        }

        @Override
        public void ensureSource(String queueName, String sourceId, OffsetDateTime now) {
        }

        @Override
        public SourceStateRow lockSource(String queueName, String sourceId) {
            return source;
        }

        @Override
        public QueueItemRow insertItem(UUID itemId, String queueName, String sourceId, long sequenceNo, String itemType, String payloadJson, String headersJson, OffsetDateTime availableAt, int maxAttempts, String idempotencyKey, OffsetDateTime now) {
            insertAttempts++;
            lastInsertMaxAttempts = maxAttempts;
            if (duplicateOnInsert) {
                throw new DuplicateIdempotencyKeyException("duplicate", null);
            }
            return item(itemId, queueName, sourceId, sequenceNo, ItemStatus.pending);
        }

        @Override
        public void incrementNextSequence(String queueName, String sourceId, OffsetDateTime now) {
        }

        @Override
        public Optional<QueueItemRow> claimHeadItem(String queueName, List<String> itemTypes, String workerId, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime now) {
            return Optional.empty();
        }

        @Override
        public Optional<QueueItemRow> findItem(String queueName, UUID itemId) {
            return Optional.of(lockedItem);
        }

        @Override
        public QueueItemRow lockItem(String queueName, UUID itemId) {
            return lockedItem;
        }

        @Override
        public List<QueueItemRow> listSourceItems(String queueName, String sourceId) {
            return List.of();
        }

        @Override
        public List<QueueItemRow> listDeadLettered(String queueName, int limit, int offset) {
            return List.of();
        }

        @Override
        public QueueItemRow complete(UUID itemId, String resultJson, OffsetDateTime now) {
            return lockedItem;
        }

        @Override
        public QueueItemRow fail(UUID itemId, ItemStatus status, OffsetDateTime availableAt, String errorType, String errorMessage, OffsetDateTime now) {
            failCalls++;
            return lockedItem;
        }

        @Override
        public void releaseSource(String queueName, String sourceId, OffsetDateTime now) {
            releaseSourceCalls++;
        }

        @Override
        public void blockSource(String queueName, String sourceId, OffsetDateTime now) {
        }

        @Override
        public int releaseSourceIfLeaseMatches(String queueName, String sourceId, UUID leaseId, String leasedBy, OffsetDateTime now) {
            releaseSourceIfLeaseMatchesCalls++;
            lastMatchedLeaseId = leaseId;
            return 1;
        }

        @Override
        public int blockSourceIfLeaseMatches(String queueName, String sourceId, UUID leaseId, String leasedBy, OffsetDateTime now) {
            return 1;
        }

        @Override
        public int heartbeat(String queueName, UUID leaseId, String workerId, OffsetDateTime leaseUntil, OffsetDateTime now) {
            return 1;
        }

        @Override
        public List<SourceStateRow> blockedSources(String queueName) {
            return List.of();
        }

        @Override
        public List<BlockedSourceRow> inspectBlockedSources(String queueName, int limit, int offset) {
            return List.of();
        }

        @Override
        public SourceStateRow findSource(String queueName, String sourceId) {
            return source;
        }

        @Override
        public QueueItemRow adminStatus(UUID itemId, ItemStatus status, OffsetDateTime availableAt, OffsetDateTime now) {
            adminStatusCalls++;
            return lockedItem;
        }

        @Override
        public Optional<QueueItemRow> skipDeadLetteredHead(String queueName, String sourceId, OffsetDateTime now) {
            skipDeadLetteredHeadCalls++;
            return Optional.empty();
        }

        @Override
        public Optional<QueueItemRow> findHeadBlockingItem(String queueName, String sourceId) {
            if (List.of(ItemStatus.succeeded, ItemStatus.cancelled, ItemStatus.skipped, ItemStatus.failed).contains(lockedItem.status())) {
                return Optional.empty();
            }
            return Optional.of(lockedItem);
        }

        @Override
        public int blockDeadLetteredHeadSources(String queueName, OffsetDateTime now) {
            blockDeadLetteredHeadSourcesCalls++;
            return 0;
        }

        @Override
        public List<QueueItemRow> expiredProcessing(OffsetDateTime now, int limit) {
            return expiredItems;
        }

        @Override
        public void insertAdminAudit(UUID auditId, OffsetDateTime occurredAt, String actorId, String operation, String queueName, String sourceId, UUID itemId, String previousStatus, String newStatus, String reason, String metadataJson) {
            adminAuditCalls++;
            auditRows.add(new AdminAuditRow(auditId, occurredAt, actorId, operation, queueName, sourceId, itemId, previousStatus, newStatus, reason, metadataJson));
        }

        @Override
        public List<AdminAuditRow> listAdminAudit(String queueName, int limit, int offset) {
            return auditRows;
        }

        @Override
        public long countRetentionEligible(String queueName, OffsetDateTime olderThan, List<ItemStatus> statuses, int limit) {
            retentionLimit = limit;
            return retentionMatched;
        }

        @Override
        public long deleteRetentionEligible(String queueName, OffsetDateTime olderThan, List<ItemStatus> statuses, int limit) {
            retentionLimit = limit;
            retentionDeleteCalls++;
            return retentionDeleted;
        }

        @Override
        public QueueMetricsSnapshot metricsSnapshot() {
            return new QueueMetricsSnapshot(0, 0, 0, 0, 0, 0, 0);
        }

        @Override
        public QueueSchemaInfo getSchemaInfo() {
            return new QueueSchemaInfo("3");
        }
    }
}
