package com.sequencedqueue.core;

import static com.sequencedqueue.core.QueueDtos.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresQueueContractTest {
    private static final String QUEUE = "wf.commands";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static DataSource dataSource;
    static QueueOperations queue;

    @BeforeAll
    static void setUpSchema() throws Exception {
        dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        applySchema(dataSource);
        queue = QueueCoreFactory.create(dataSource, new ObjectMapper(), 60, 5);
    }

    @BeforeEach
    void clearTables() throws Exception {
        execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
    }

    @Test
    void concurrentEnqueuesSameSourceProduceSequenceOneToOneHundred() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(16);
        try {
            List<Callable<Long>> tasks = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                tasks.add(() -> {
                    start.await();
                    return enqueue("source-1").sequenceNo();
                });
            }
            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();

            List<Long> sequences = new ArrayList<>();
            for (var future : futures) {
                sequences.add(future.get());
            }
            Collections.sort(sequences);

            assertEquals(100, count("queue_item"));
            assertEquals(100, new HashSet<>(sequences).size());
            for (int i = 1; i <= 100; i++) {
                assertEquals(i, sequences.get(i - 1));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentDuplicateIdempotencyCreatesOneItemAndDoesNotAdvanceSequence() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(16);
        try {
            List<Callable<UUID>> tasks = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                tasks.add(() -> {
                    start.await();
                    return queue.enqueue(QUEUE, new EnqueueRequest("source-1", "type", "idem-1", Map.of("item", "x"), Map.of(), null, 5)).itemId();
                });
            }
            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();

            List<UUID> itemIds = new ArrayList<>();
            for (var future : futures) {
                itemIds.add(future.get());
            }

            assertEquals(1, new HashSet<>(itemIds).size());
            assertEquals(1, count("queue_item"));
            assertEquals(2, enqueue("source-1").sequenceNo());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void sameSourceCannotBeClaimedConcurrently() throws Exception {
        for (int i = 0; i < 10; i++) {
            enqueue("source-1");
        }
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(20);
        try {
            List<Callable<ClaimResponse>> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                int worker = i;
                tasks.add(() -> {
                    start.await();
                    return claim("worker-" + worker);
                });
            }
            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();

            List<ClaimResponse> claims = new ArrayList<>();
            for (var future : futures) {
                claims.add(future.get());
            }

            assertEquals(1, claims.stream().filter(c -> !c.items().isEmpty()).count());
            assertEquals(1, countWhere("queue_item", "status = 'processing'"));
            assertEquals("leased", scalar("SELECT status FROM queue_source_state WHERE queue_name = 'wf.commands' AND source_id = 'source-1'"));
            assertTrue(claim("late-worker").items().isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void differentSourcesCanBeClaimedConcurrently() throws Exception {
        for (int i = 0; i < 20; i++) {
            enqueue("source-" + i);
        }
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(20);
        try {
            List<Callable<ClaimResponse>> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                int worker = i;
                tasks.add(() -> {
                    start.await();
                    return claim("worker-" + worker);
                });
            }
            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();

            List<String> sourceIds = new ArrayList<>();
            for (var future : futures) {
                ClaimResponse claim = future.get();
                if (!claim.items().isEmpty()) {
                    sourceIds.add(claim.sourceId());
                }
            }

            assertTrue(sourceIds.size() > 1);
            assertEquals(sourceIds.size(), new HashSet<>(sourceIds).size());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void claimOnlyReturnsHeadItem() throws Exception {
        enqueue("source-1");
        enqueue("source-1");
        enqueue("source-1");

        ClaimResponse claim = claim("worker-1");

        assertEquals(1, claim.items().size());
        assertEquals(1, claim.items().getFirst().sequenceNo());
        assertEquals("pending", scalar("SELECT status FROM queue_item WHERE sequence_no = 2"));
        assertEquals("pending", scalar("SELECT status FROM queue_item WHERE sequence_no = 3"));
    }

    @Test
    void retryWaitHeadBlocksLaterPendingItemUntilAvailableAt() throws Exception {
        enqueue("source-1");
        enqueue("source-1");
        ClaimResponse first = claim("worker-1");
        fail(first, true, 3600);

        assertTrue(claim("worker-2").items().isEmpty());

        execute("UPDATE queue_item SET available_at = now() - interval '1 second' WHERE sequence_no = 1");
        ClaimResponse retry = claim("worker-3");

        assertEquals(1, retry.items().getFirst().sequenceNo());
    }

    @Test
    void deadLetteredHeadBlocksSource() throws Exception {
        enqueue("source-1", 1);
        enqueue("source-1");
        ClaimResponse first = claim("worker-1");

        fail(first, true, null);

        assertEquals("dead_lettered", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
        assertEquals("blocked", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
        assertTrue(claim("worker-2").items().isEmpty());
    }

    @Test
    void failedHeadIsPassableAndDoesNotStallSource() throws Exception {
        enqueue("source-1");
        enqueue("source-1");
        ClaimResponse first = claim("worker-1");

        queue.fail(QUEUE, first.items().getFirst().itemId(), new FailRequest("worker-1", first.leaseId(), false, "ERR", "no retry", null));

        assertEquals("failed", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
        assertEquals("idle", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
        ClaimResponse second = claim("worker-2");
        assertEquals(2, second.items().getFirst().sequenceNo());
    }

    @Test
    void wrongWorkerCannotComplete() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");

        QueueException error = assertThrows(QueueException.class, () -> queue.complete(QUEUE, claim.items().getFirst().itemId(), new CompleteRequest("worker-2", claim.leaseId(), Map.of())));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals("processing", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
    }

    @Test
    void wrongLeaseCannotComplete() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");

        QueueException error = assertThrows(QueueException.class, () -> queue.complete(QUEUE, claim.items().getFirst().itemId(), new CompleteRequest("worker-1", UUID.randomUUID(), Map.of())));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals("processing", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
    }

    @Test
    void expiredLeaseCannotComplete() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");
        expireLeases();

        QueueException error = assertThrows(QueueException.class, () -> queue.complete(QUEUE, claim.items().getFirst().itemId(), new CompleteRequest("worker-1", claim.leaseId(), Map.of())));

        assertEquals(QueueException.CONFLICT, error.statusCode());
    }

    @Test
    void heartbeatAfterLeaseExpiryFails() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");
        expireLeases();
        OffsetDateTime before = leaseUntil();

        QueueException error = assertThrows(QueueException.class, () -> queue.heartbeat(QUEUE, claim.leaseId(), new HeartbeatRequest("worker-1", 60)));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals(before, leaseUntil());
    }

    @Test
    void recoverExpiredLeaseMovesItemToRetryWaitAndReleasesMatchingSourceLease() throws Exception {
        enqueue("source-1");
        claim("worker-1");
        expireLeases();

        queue.recoverExpiredLeases();

        assertEquals("retry_wait", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
        assertEquals(0, countWhere("queue_item", "lease_id IS NOT NULL OR claimed_by IS NOT NULL OR lease_until IS NOT NULL"));
        assertEquals("idle", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
    }

    @Test
    void recoverExpiredLeaseWithAttemptsExhaustedDeadLettersAndBlocksSource() throws Exception {
        enqueue("source-1", 1);
        claim("worker-1");
        expireLeases();

        queue.recoverExpiredLeases();

        assertEquals("dead_lettered", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
        assertEquals("blocked", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
    }

    @Test
    void recoverExpiredLeaseDoesNotMutateItemWhenSourceLeaseDoesNotMatch() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");
        UUID mismatchedLease = UUID.randomUUID();
        execute("UPDATE queue_item SET lease_until = now() - interval '1 second'");
        execute("UPDATE queue_source_state SET lease_id = '" + mismatchedLease + "', leased_by = 'other-worker', lease_until = now() - interval '1 second'");

        queue.recoverExpiredLeases();

        assertEquals("processing", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
        assertEquals(claim.leaseId().toString(), scalar("SELECT lease_id::text FROM queue_item WHERE sequence_no = 1"));
        assertEquals(mismatchedLease.toString(), scalar("SELECT lease_id::text FROM queue_source_state WHERE source_id = 'source-1'"));
    }

    @Test
    void adminSkipDeadLetteredHeadUnblocksSource() throws Exception {
        enqueue("source-1", 1);
        enqueue("source-1");
        ClaimResponse first = claim("worker-1");
        fail(first, true, null);

        queue.skip(QUEUE, first.items().getFirst().itemId());

        assertEquals("skipped", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
        assertEquals("idle", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
        assertEquals(2, claim("worker-2").items().getFirst().sequenceNo());
    }

    @Test
    void adminRetryWritesAudit() throws Exception {
        enqueue("source-1", 1);
        ClaimResponse first = claim("worker-1");
        fail(first, true, null);

        queue.retry(QUEUE, first.items().getFirst().itemId(), "admin-api-key", "try again");

        assertEquals(1, count("queue_admin_audit"));
        assertEquals("retry", scalar("SELECT operation FROM queue_admin_audit"));
        assertEquals("dead_lettered", scalar("SELECT previous_status FROM queue_admin_audit"));
        assertEquals("retry_wait", scalar("SELECT new_status FROM queue_admin_audit"));
        assertEquals("admin-api-key", scalar("SELECT actor_id FROM queue_admin_audit"));
        assertEquals("try again", scalar("SELECT reason FROM queue_admin_audit"));
    }

    @Test
    void enqueueEmitsNotificationAfterCommit() throws Exception {
        QueueOperations notifyingQueue = QueueCoreFactory.create(dataSource, new ObjectMapper(), QueueSettings.defaults(), PostgresQueueNotifier.onDefaultChannel());
        try (Connection listener = listen()) {
            notifyingQueue.enqueue(QUEUE, new EnqueueRequest("notify-source", "type", null, Map.of(), Map.of(), null, 5));

            String payload = nextNotification(listener);

            assertTrue(payload.contains("\"queueName\":\"" + QUEUE + "\""));
            assertTrue(payload.contains("\"reason\":\"ENQUEUE\""));
            assertFalse(payload.contains("notify-source"));
        }
    }

    @Test
    void manualRollbackDoesNotDeliverNotification() throws Exception {
        try (Connection listener = listen();
             Connection sender = dataSource.getConnection()) {
            sender.setAutoCommit(false);
            PostgresQueueNotifier.onDefaultChannel().notifyWorkAvailable(sender, new QueueWakeupEvent(QUEUE, QueueWakeupReason.ENQUEUE));
            sender.rollback();

            assertEquals(null, maybeNextNotification(listener, 500));
        }
    }

    @Test
    void notificationChannelValidationIsLowercaseOnly() {
        assertEquals(PostgresQueueNotifier.DEFAULT_CHANNEL, PostgresQueueNotifier.onDefaultChannel().channel());
        assertEquals("custom_wakeup_1", PostgresQueueNotifier.onChannel("custom_wakeup_1").channel());
        assertThrows(IllegalArgumentException.class, () -> PostgresQueueNotifier.onChannel("BadChannel"));
        assertThrows(IllegalArgumentException.class, () -> PostgresQueueNotifier.onChannel("bad-channel"));
        assertThrows(IllegalArgumentException.class, () -> PostgresQueueNotifier.onChannel(""));
        assertThrows(IllegalArgumentException.class, () -> PostgresQueueNotifier.onChannel("a".repeat(64)));
    }

    @Test
    void notificationFailureDoesNotRollbackQueueMutation() throws Exception {
        QueueOperations bestEffortQueue = QueueCoreFactory.create(
            dataSource,
            new ObjectMapper(),
            QueueSettings.defaults(),
            (connection, event) -> {
                throw new SQLException("notification failed");
            }
        );

        EnqueueResponse enqueued = bestEffortQueue.enqueue(QUEUE, new EnqueueRequest("best-effort-source", "type", null, Map.of(), Map.of(), null, 5));

        assertNotNull(enqueued.itemId());
        assertEquals(1, count("queue_item"));
        assertEquals("pending", scalar("SELECT status FROM queue_item WHERE item_id = '" + enqueued.itemId() + "'"));
    }

    @Test
    void adminOperationsEmitNotifications() throws Exception {
        QueueOperations notifyingQueue = QueueCoreFactory.create(dataSource, new ObjectMapper(), QueueSettings.defaults(), PostgresQueueNotifier.onDefaultChannel());
        try (Connection listener = listen()) {
            EnqueueResponse retry = notifyingQueue.enqueue(QUEUE, new EnqueueRequest("retry-source", "type", null, Map.of(), Map.of(), null, 1));
            ClaimResponse retryClaim = notifyingQueue.claim(QUEUE, new ClaimRequest("worker-1", List.of("type"), 60, 1));
            notifyingQueue.fail(QUEUE, retryClaim.items().getFirst().itemId(), new FailRequest("worker-1", retryClaim.leaseId(), true, "ERR", "failed", null));
            drainNotifications(listener);

            notifyingQueue.retry(QUEUE, retry.itemId(), "admin", "retry");
            assertTrue(nextNotification(listener).contains("\"reason\":\"ADMIN_RETRY\""));
            execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
            drainNotifications(listener);

            EnqueueResponse skip = notifyingQueue.enqueue(QUEUE, new EnqueueRequest("skip-source", "type", null, Map.of(), Map.of(), null, 1));
            ClaimResponse skipClaim = notifyingQueue.claim(QUEUE, new ClaimRequest("worker-2", List.of("type"), 60, 1));
            notifyingQueue.fail(QUEUE, skipClaim.items().getFirst().itemId(), new FailRequest("worker-2", skipClaim.leaseId(), true, "ERR", "failed", null));
            drainNotifications(listener);
            notifyingQueue.skip(QUEUE, skip.itemId(), "admin", "skip");
            assertTrue(nextNotification(listener).contains("\"reason\":\"ADMIN_SKIP\""));
            execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
            drainNotifications(listener);

            EnqueueResponse cancel = notifyingQueue.enqueue(QUEUE, new EnqueueRequest("cancel-source", "type", null, Map.of(), Map.of(), null, 5));
            drainNotifications(listener);
            notifyingQueue.cancel(QUEUE, cancel.itemId(), "admin", "cancel");
            assertTrue(nextNotification(listener).contains("\"reason\":\"ADMIN_CANCEL\""));
            execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
            drainNotifications(listener);

            notifyingQueue.enqueue(QUEUE, new EnqueueRequest("unblock-source", "type", null, Map.of(), Map.of(), null, 5));
            execute("UPDATE queue_item SET status = 'skipped' WHERE source_id = 'unblock-source'");
            execute("UPDATE queue_source_state SET status = 'blocked' WHERE source_id = 'unblock-source'");
            drainNotifications(listener);
            notifyingQueue.unblockSource(QUEUE, "unblock-source", "admin", "unblock");
            assertTrue(nextNotification(listener).contains("\"reason\":\"ADMIN_UNBLOCK\""));
        }
    }

    @Test
    void leaseRecoveryEmitsNotificationWhenItMutatesWork() throws Exception {
        QueueOperations notifyingQueue = QueueCoreFactory.create(dataSource, new ObjectMapper(), QueueSettings.defaults(), PostgresQueueNotifier.onDefaultChannel());
        try (Connection listener = listen()) {
            notifyingQueue.enqueue(QUEUE, new EnqueueRequest("recovery-source", "type", null, Map.of(), Map.of(), null, 5));
            ClaimResponse claim = notifyingQueue.claim(QUEUE, new ClaimRequest("worker-1", List.of("type"), 60, 1));
            assertFalse(claim.items().isEmpty());
            expireLeases();
            drainNotifications(listener);

            assertEquals(1, notifyingQueue.recoverExpiredLeases());

            assertTrue(nextNotification(listener).contains("\"reason\":\"RECOVERY\""));
        }
    }

    @Test
    void adminSkipCancelAndUnblockWriteAudit() throws Exception {
        UUID skipped = enqueue("skip-source", 1).itemId();
        ClaimResponse skipClaim = claim("worker-1");
        fail(skipClaim, true, null);
        queue.skip(QUEUE, skipped, "admin-api-key", "skip bad work");

        UUID cancelled = enqueue("cancel-source").itemId();
        queue.cancel(QUEUE, cancelled, "admin-api-key", "cancel queued work");

        enqueue("unblock-source");
        execute("UPDATE queue_item SET status = 'skipped' WHERE source_id = 'unblock-source'");
        execute("UPDATE queue_source_state SET status = 'blocked' WHERE source_id = 'unblock-source'");
        queue.unblockSource(QUEUE, "unblock-source", "admin-api-key", "source repaired");

        assertEquals(3, count("queue_admin_audit"));
        assertEquals(1, countWhere("queue_admin_audit", "operation = 'skip' AND previous_status = 'dead_lettered' AND new_status = 'skipped' AND item_id = '" + skipped + "'"));
        assertEquals(1, countWhere("queue_admin_audit", "operation = 'cancel' AND previous_status = 'pending' AND new_status = 'cancelled' AND item_id = '" + cancelled + "'"));
        assertEquals(1, countWhere("queue_admin_audit", "operation = 'unblock' AND previous_status = 'blocked' AND new_status = 'idle' AND source_id = 'unblock-source'"));
    }

    @Test
    void lifecycleOperationsDoNotWriteAdminAudit() throws Exception {
        enqueue("complete-source");
        ClaimResponse completeClaim = claim("worker-1");
        queue.heartbeat(QUEUE, completeClaim.leaseId(), new HeartbeatRequest("worker-1", 60));
        queue.complete(QUEUE, completeClaim.items().getFirst().itemId(), new CompleteRequest("worker-1", completeClaim.leaseId(), Map.of()));

        enqueue("fail-source");
        ClaimResponse failClaim = claim("worker-2");
        queue.fail(QUEUE, failClaim.items().getFirst().itemId(), new FailRequest("worker-2", failClaim.leaseId(), false, "ERR", "failed", null));

        assertEquals(0, count("queue_admin_audit"));
    }

    @Test
    void retentionDryRunCountsWithoutDeletingOrAuditing() throws Exception {
        enqueue("old-succeeded");
        execute("UPDATE queue_item SET status = 'succeeded', updated_at = now() - interval '10 days' WHERE source_id = 'old-succeeded'");

        RetentionPurgeResponse response = queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("succeeded"), true, "dry run"),
            "admin-api-key");

        assertEquals(1, response.matched());
        assertEquals(0, response.deleted());
        assertEquals(1, count("queue_item"));
        assertEquals(0, count("queue_admin_audit"));
    }

    @Test
    void retentionPurgeDryRunDoesNotDelete() throws Exception {
        oldItem("dry-run-1", "succeeded");
        oldItem("dry-run-2", "failed");

        RetentionPurgeResponse response = queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("succeeded", "failed"), true, "dry run", 1),
            "admin-api-key");

        assertEquals(1, response.matched());
        assertEquals(0, response.deleted());
        assertEquals(2, count("queue_item"));
        assertEquals(0, count("queue_admin_audit"));
    }

    @Test
    void retentionPurgeDeletesOnlyEligibleTerminalRowsAndWritesAudit() throws Exception {
        oldItem("old-succeeded", "succeeded");
        oldItem("old-cancelled", "cancelled");
        oldItem("old-skipped", "skipped");
        oldItem("old-failed", "failed");
        oldItem("old-pending", "pending");
        oldItem("old-processing", "processing");
        oldItem("old-retry-wait", "retry_wait");
        oldItem("old-dead-lettered", "dead_lettered");

        RetentionPurgeResponse response = queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("succeeded", "cancelled", "skipped", "failed"), false, "delete terminal"),
            "admin-api-key");

        assertEquals(4, response.matched());
        assertEquals(4, response.deleted());
        assertEquals(4, count("queue_item"));
        assertEquals(1, countWhere("queue_item", "status = 'pending'"));
        assertEquals(1, countWhere("queue_item", "status = 'processing'"));
        assertEquals(1, countWhere("queue_item", "status = 'retry_wait'"));
        assertEquals(1, countWhere("queue_item", "status = 'dead_lettered'"));
        assertEquals(1, countWhere("queue_admin_audit", "operation = 'retention_purge' AND actor_id = 'admin-api-key' AND reason = 'delete terminal'"));
    }

    @Test
    void retentionPurgeDeletesAtMostLimit() throws Exception {
        oldItem("bounded-1", "succeeded");
        oldItem("bounded-2", "succeeded");
        oldItem("bounded-3", "succeeded");

        RetentionPurgeResponse response = queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("succeeded"), false, "delete bounded", 2),
            "admin-api-key");

        assertEquals(2, response.matched());
        assertEquals(2, response.deleted());
        assertEquals(1, count("queue_item"));
    }

    @Test
    void retentionPurgeRejectsZeroLimit() {
        QueueException error = assertThrows(QueueException.class, () -> queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("succeeded"), false, "bad", 0),
            "admin-api-key"));

        assertEquals(QueueException.BAD_REQUEST, error.statusCode());
    }

    @Test
    void retentionPurgeRejectsLimitAboveMax() {
        JdbcTransactionRunner boundedTransactions = new JdbcTransactionRunner(dataSource);
        PostgresQueueRepository boundedRepository = new PostgresQueueRepository(boundedTransactions);
        DefaultQueueService boundedQueue = new DefaultQueueService(boundedRepository, boundedTransactions, new RetryPolicy(), new ObjectMapper(), java.time.Clock.systemUTC(),
            new QueueSettings(60, 600, 5, 262_144, 65_536, 8_192, 2_048, 2));

        QueueException error = assertThrows(QueueException.class, () -> boundedQueue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("succeeded"), false, "bad", 3),
            "admin-api-key"));

        assertEquals(QueueException.BAD_REQUEST, error.statusCode());
    }

    @Test
    void retentionPurgeRejectsBlockingStatuses() throws Exception {
        QueueException error = assertThrows(QueueException.class, () -> queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("dead_lettered"), false, "bad"),
            "admin-api-key"));

        assertEquals(QueueException.BAD_REQUEST, error.statusCode());
        assertEquals(0, count("queue_admin_audit"));
    }

    @Test
    void retentionPurgeStillRejectsBlockingStatuses() throws Exception {
        QueueException error = assertThrows(QueueException.class, () -> queue.purgeRetention(QUEUE,
            new RetentionPurgeRequest(OffsetDateTime.now().plusYears(100), List.of("pending", "processing", "retry_wait", "dead_lettered"), false, "bad", 1),
            "admin-api-key"));

        assertEquals(QueueException.BAD_REQUEST, error.statusCode());
        assertEquals(0, count("queue_admin_audit"));
    }

    @Test
    void failedAdminOperationDoesNotWriteSuccessAudit() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");

        assertThrows(QueueException.class, () -> queue.skip(QUEUE, claim.items().getFirst().itemId(), "admin-api-key", "invalid"));

        assertEquals(0, count("queue_admin_audit"));
    }

    @Test
    void adminSkipProcessingItemIsRejected() throws Exception {
        enqueue("source-1");
        ClaimResponse claim = claim("worker-1");

        QueueException error = assertThrows(QueueException.class, () -> queue.skip(QUEUE, claim.items().getFirst().itemId()));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals("leased", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
        assertEquals("processing", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
    }

    @Test
    void unblockSourceRejectsBlockedSourceWithProcessingHead() throws Exception {
        enqueue("source-1");
        claim("worker-1");
        execute("UPDATE queue_source_state SET status = 'blocked', leased_by = NULL, lease_id = NULL, lease_until = NULL WHERE source_id = 'source-1'");

        QueueException error = assertThrows(QueueException.class, () -> queue.unblockSource(QUEUE, "source-1"));

        assertEquals(QueueException.CONFLICT, error.statusCode());
        assertEquals("blocked", scalar("SELECT status FROM queue_source_state WHERE source_id = 'source-1'"));
        assertEquals("processing", scalar("SELECT status FROM queue_item WHERE sequence_no = 1"));
    }

    @Test
    void claimAndAdminSkipSamePendingHeadDoNotDeadlock() throws Exception {
        UUID itemId = enqueue("source-1").itemId();

        raceClaimWithAdminRepair(itemId, "skipped", id -> queue.skip(QUEUE, id));
    }

    @Test
    void claimAndAdminCancelSamePendingHeadDoNotDeadlock() throws Exception {
        UUID itemId = enqueue("source-1").itemId();

        raceClaimWithAdminRepair(itemId, "cancelled", id -> queue.cancel(QUEUE, id));
    }

    @Test
    void schemaInfoReportsFlywayVersion() {
        assertEquals("1", queue.getSchemaInfo().schemaVersion());
    }

    private EnqueueResponse enqueue(String sourceId) {
        return enqueue(sourceId, 5);
    }

    private EnqueueResponse enqueue(String sourceId, int maxAttempts) {
        return queue.enqueue(QUEUE, new EnqueueRequest(sourceId, "type", null, Map.of("source", sourceId), Map.of(), null, maxAttempts));
    }

    private ClaimResponse claim(String workerId) {
        return queue.claim(QUEUE, new ClaimRequest(workerId, List.of("type"), 60, 1));
    }

    private void fail(ClaimResponse claim, boolean retryable, Integer backoffSeconds) {
        queue.fail(QUEUE, claim.items().getFirst().itemId(), new FailRequest("worker-1", claim.leaseId(), retryable, "ERR", "failed", backoffSeconds));
    }

    private void oldItem(String sourceId, String status) throws Exception {
        enqueue(sourceId);
        execute("UPDATE queue_item SET status = '" + status + "', updated_at = now() - interval '10 days' WHERE source_id = '" + sourceId + "'");
    }

    private static void applySchema(DataSource dataSource) throws Exception {
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load()
            .migrate();
    }

    private void raceClaimWithAdminRepair(UUID itemId, String terminalStatus, AdminRepair repair) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var claimFuture = executor.submit(() -> {
                start.await();
                return claim("race-worker");
            });
            var adminFuture = executor.submit(() -> {
                start.await();
                try {
                    return repair.apply(itemId).status();
                } catch (QueueException e) {
                    assertEquals(QueueException.CONFLICT, e.statusCode());
                    return "conflict";
                }
            });

            start.countDown();
            ClaimResponse claim = claimFuture.get(5, TimeUnit.SECONDS);
            String adminResult = adminFuture.get(5, TimeUnit.SECONDS);
            String finalStatus = scalar("SELECT status FROM queue_item WHERE item_id = '" + itemId + "'");

            if ("processing".equals(finalStatus)) {
                assertFalse(claim.items().isEmpty());
                assertEquals("conflict", adminResult);
            } else {
                assertEquals(terminalStatus, finalStatus);
                assertEquals(terminalStatus, adminResult);
                assertTrue(claim.items().isEmpty());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private void expireLeases() throws Exception {
        execute("UPDATE queue_item SET lease_until = now() - interval '1 second'");
        execute("UPDATE queue_source_state SET lease_until = now() - interval '1 second'");
    }

    private OffsetDateTime leaseUntil() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT lease_until FROM queue_item WHERE sequence_no = 1")) {
            rs.next();
            return rs.getObject(1, OffsetDateTime.class);
        }
    }

    private static Connection listen() throws Exception {
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
            statement.execute("LISTEN " + PostgresQueueNotifier.DEFAULT_CHANNEL);
        }
        return connection;
    }

    private static String nextNotification(Connection listener) throws Exception {
        String payload = maybeNextNotification(listener, 5000);
        assertNotNull(payload);
        return payload;
    }

    private static String maybeNextNotification(Connection listener, int timeoutMillis) throws Exception {
        PGNotification[] notifications = listener.unwrap(PGConnection.class).getNotifications(timeoutMillis);
        return notifications == null || notifications.length == 0 ? null : notifications[0].getParameter();
    }

    private static void drainNotifications(Connection listener) throws Exception {
        while (maybeNextNotification(listener, 100) != null) {
            // Drain prior enqueue/fail wake-ups so the next assertion targets the operation under test.
        }
    }

    private static int count(String table) throws Exception {
        return countWhere(table, "true");
    }

    private static int countWhere(String table, String where) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + table + " WHERE " + where)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static String scalar(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @FunctionalInterface
    private interface AdminRepair {
        ItemResponse apply(UUID itemId);
    }

    private record DriverManagerDataSource(String url, String username, String password) implements DataSource {
        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, username, password);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return DriverManager.getConnection(url, username, password);
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("unwrap is not supported");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
