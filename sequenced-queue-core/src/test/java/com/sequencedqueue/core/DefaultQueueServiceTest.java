package com.sequencedqueue.core;

import static com.sequencedqueue.core.QueueDtos.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class DefaultQueueServiceTest {
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

    @Test
    void claimMarksIdleDeadLetteredHeadsBlockedBeforeTryingToClaim() {
        FakeRepository repository = new FakeRepository();
        DefaultQueueService service = service(repository);

        service.claim("q", new ClaimRequest("worker-1", List.of("type"), 60, 1));

        assertEquals(1, repository.blockDeadLetteredHeadSourcesCalls);
    }

    @Test
    void unblockDoesNotReleaseLeasedSource() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.leased);
        DefaultQueueService service = service(repository);

        QueueException error = assertThrows(QueueException.class, () -> service.unblockSource("q", "s"));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(0, repository.releaseSourceCalls);
    }

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

    @Test
    void unblockSourceSucceedsOnlyAfterDeadLetteredHeadWasRepaired() {
        FakeRepository repository = new FakeRepository();
        repository.source = source(SourceStatus.blocked);
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.skipped);
        DefaultQueueService service = service(repository);

        service.unblockSource("q", "s");

        assertEquals(1, repository.releaseSourceCalls);
    }

    @Test
    void adminSkipRejectsProcessingItemAndDoesNotReleaseSource() {
        FakeRepository repository = new FakeRepository();
        repository.lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.processing);
        DefaultQueueService service = service(repository);

        QueueException error = assertThrows(QueueException.class, () -> service.skip("q", repository.lockedItem.itemId()));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(0, repository.releaseSourceCalls);
    }

    @Test
    void recoveryReleasesSourceOnlyWhenLeaseMatchesExpiredItem() {
        FakeRepository repository = new FakeRepository();
        UUID leaseId = UUID.randomUUID();
        repository.expiredItems.add(new QueueItemRow(UUID.randomUUID(), "q", "s", 1, "type", "{}", "{}", ItemStatus.processing, now(), "worker-1", leaseId, now().minusSeconds(1), 1, 5, null, null, null, null, now(), now()));
        DefaultQueueService service = service(repository);

        service.recoverExpiredLeases();

        assertEquals(1, repository.failCalls);
        assertEquals(1, repository.releaseSourceIfLeaseMatchesCalls);
        assertEquals(0, repository.releaseSourceCalls);
        assertEquals(leaseId, repository.lastMatchedLeaseId);
    }

    private DefaultQueueService service(FakeRepository repository) {
        TransactionRunner transactions = new TransactionRunner() {
            @Override
            public <T> T inTransaction(TransactionCallback<T> callback) {
                return callback.execute();
            }
        };
        return new DefaultQueueService(repository, transactions, new RetryPolicy(), new ObjectMapper(), Clock.fixed(now().toInstant(), ZoneOffset.UTC), 60, 5);
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

    private static final class FakeRepository implements QueueRepository {
        SourceStateRow source = source(SourceStatus.idle);
        QueueItemRow lockedItem = item(UUID.randomUUID(), "q", "s", 1, ItemStatus.pending);
        QueueItemRow existingAfterDuplicate;
        boolean duplicateOnInsert;
        int findByIdempotencyCalls;
        int insertAttempts;
        int blockDeadLetteredHeadSourcesCalls;
        int releaseSourceCalls;
        int releaseSourceIfLeaseMatchesCalls;
        int failCalls;
        int adminStatusCalls;
        int skipDeadLetteredHeadCalls;
        UUID lastMatchedLeaseId;
        final List<QueueItemRow> expiredItems = new ArrayList<>();

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
        public QueueSchemaInfo getSchemaInfo() {
            return new QueueSchemaInfo("1");
        }
    }
}
