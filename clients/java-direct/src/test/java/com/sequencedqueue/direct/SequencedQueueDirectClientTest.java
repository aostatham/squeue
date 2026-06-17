package com.sequencedqueue.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.sequencedqueue.core.PostgresQueueNotifier;
import com.sequencedqueue.core.QueueWakeupEvent;
import com.sequencedqueue.core.QueueWakeupReason;

/**
 * Test coverage for SequencedQueueDirectClientTest.
 */
@Testcontainers
class SequencedQueueDirectClientTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static DataSource dataSource;
    static SequencedQueueDirectClient client;

    @BeforeAll
    static void setUp() throws Exception {
        dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        applySchema(dataSource);
        client = SequencedQueueDirectClient.builder()
            .dataSource(dataSource)
            .defaultQueueName("wf.commands")
            .validateSchemaOnBuild(true)
            .build();
    }

    @BeforeEach
    void clearTables() throws Exception {
        execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
    }

    /**
     * Verifies concurrent enqueues to same source produce contiguous sequence numbers.
     */
    @Test
    void concurrentEnqueuesToSameSourceProduceContiguousSequenceNumbers() throws Exception {
        int itemCount = 100;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Callable<Long>> tasks = new ArrayList<>();
            for (int i = 0; i < itemCount; i++) {
                int item = i;
                tasks.add(() -> {
                    start.await();
                    return client.enqueue(EnqueueRequest.builder()
                        .sourceId("inst-123")
                        .itemType("wf.command")
                        .payloadJson("{\"item\":" + item + "}")
                        .headersJson("{}")
                        .build()).sequenceNo();
                });
            }

            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();

            List<Long> sequenceNumbers = new ArrayList<>();
            for (var future : futures) {
                sequenceNumbers.add(future.get());
            }
            Collections.sort(sequenceNumbers);

            assertEquals(itemCount, sequenceNumbers.size());
            for (int i = 1; i <= itemCount; i++) {
                assertEquals(i, sequenceNumbers.get(i - 1));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Verifies idempotency key returns existing item without advancing source sequence.
     */
    @Test
    void idempotencyKeyReturnsExistingItemWithoutAdvancingSourceSequence() {
        EnqueueRequest request = EnqueueRequest.builder()
            .sourceId("inst-idempotent")
            .itemType("wf.command")
            .idempotencyKey("cmd-1")
            .payloadJson("{\"commandName\":\"sendEmail\"}")
            .headersJson("{}")
            .build();

        EnqueueResponse first = client.enqueue("wf.commands", request);
        EnqueueResponse second = client.enqueue("wf.commands", request);
        EnqueueResponse next = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-idempotent")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());

        assertEquals(first, second);
        assertEquals(2, next.sequenceNo());
    }

    /**
     * Verifies direct claim complete uses shared core.
     */
    @Test
    void directClaimCompleteUsesSharedCore() {
        EnqueueResponse enqueued = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-complete")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());

        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));
        ItemResponse completed = client.complete("wf.commands", enqueued.itemId(), new CompleteRequest("worker-1", claim.leaseId(), Map.of("ok", true)));

        assertEquals("succeeded", completed.status());
    }

    /**
     * Verifies direct wrong lease cannot complete.
     */
    @Test
    void directWrongLeaseCannotComplete() {
        client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-wrong-lease")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));

        LeaseLostException error = assertThrows(LeaseLostException.class, () ->
            client.complete("wf.commands", claim.items().getFirst().itemId(), new CompleteRequest("worker-1", java.util.UUID.randomUUID(), Map.of())));

        assertEquals(LeaseLostException.class, error.getClass());
    }

    /**
     * Verifies direct heartbeat after expiry fails.
     */
    @Test
    void directHeartbeatAfterExpiryFails() throws Exception {
        client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-heartbeat")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));
        execute("UPDATE queue_item SET lease_until = now() - interval '1 second'");
        execute("UPDATE queue_source_state SET lease_until = now() - interval '1 second'");

        assertThrows(LeaseLostException.class, () ->
            client.heartbeat("wf.commands", claim.leaseId(), new HeartbeatRequest("worker-1", 60)));
    }

    /**
     * Verifies direct exception mapping uses core error codes.
     */
    @Test
    void directExceptionMappingUsesCoreErrorCodes() {
        assertThrows(InvalidQueueRequestException.class, () ->
            client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 0, 1)));

        assertThrows(ItemNotFoundException.class, () ->
            client.complete("wf.commands", java.util.UUID.randomUUID(), new CompleteRequest("worker-1", java.util.UUID.randomUUID(), Map.of())));

        EnqueueResponse pending = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-not-claimed")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        assertThrows(ItemNotClaimedException.class, () ->
            client.complete("wf.commands", pending.itemId(), new CompleteRequest("worker-1", java.util.UUID.randomUUID(), Map.of())));

        client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-conflict")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));
        assertThrows(QueueConflictException.class, () -> client.skip("wf.commands", claim.items().getFirst().itemId()));

        assertThrows(SourceBlockedException.class, () -> client.unblockSource("wf.commands", "inst-not-claimed"));
    }

    /**
     * Verifies direct enqueue maps oversized payload to structured typed exception.
     */
    @Test
    void directEnqueuePayloadTooLargeThrowsStructuredException() {
        String secretPayload = "secret-direct-payload-" + "x".repeat(270_000);

        QueueFieldTooLargeException error = assertThrows(QueueFieldTooLargeException.class, () ->
            client.enqueue("wf.commands", EnqueueRequest.builder()
                .sourceId("inst-direct-limit-payload")
                .itemType("wf.command")
                .payloadJson("{\"secret\":\"" + secretPayload + "\"}")
                .headersJson("{}")
                .build()));

        assertEquals("payload", error.fieldName());
        assertEquals(262144, error.maxBytes());
        assertTrue(error.actualBytes() > 262144);
        assertEquals("wf.commands", error.queueName());
        assertFalse(error.getMessage().contains(secretPayload));
    }

    /**
     * Verifies direct complete maps oversized result to structured typed exception.
     */
    @Test
    void directCompleteResultTooLargeThrowsStructuredException() {
        EnqueueResponse enqueued = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-direct-limit-result")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));
        String secretResult = "secret-direct-result-" + "x".repeat(270_000);

        QueueFieldTooLargeException error = assertThrows(QueueFieldTooLargeException.class, () ->
            client.complete("wf.commands", enqueued.itemId(), new CompleteRequest("worker-1", claim.leaseId(), Map.of("secret", secretResult))));

        assertEquals("result", error.fieldName());
        assertEquals(enqueued.itemId(), error.itemId());
        assertEquals(262144, error.maxBytes());
        assertTrue(error.actualBytes() > 262144);
        assertFalse(error.getMessage().contains(secretResult));
    }

    /**
     * Verifies direct fail maps oversized error type to structured typed exception.
     */
    @Test
    void directFailErrorTypeTooLargeThrowsStructuredException() {
        EnqueueResponse enqueued = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-direct-limit-error-type")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));
        String secretType = "SECRET_" + "X".repeat(300);

        QueueFieldTooLargeException error = assertThrows(QueueFieldTooLargeException.class, () ->
            client.fail("wf.commands", enqueued.itemId(), new FailRequest("worker-1", claim.leaseId(), false, secretType, "safe", null)));

        assertEquals("errorType", error.fieldName());
        assertEquals(enqueued.itemId(), error.itemId());
        assertEquals(256, error.maxBytes());
        assertTrue(error.actualBytes() > 256);
        assertFalse(error.getMessage().contains(secretType));
    }

    /**
     * Verifies direct admin skip dead lettered head unblocks source.
     */
    @Test
    void directAdminSkipDeadLetteredHeadUnblocksSource() throws Exception {
        EnqueueResponse first = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-skip")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .maxAttempts(1)
            .build());
        client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-skip")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));
        client.fail("wf.commands", first.itemId(), new FailRequest("worker-1", claim.leaseId(), true, "ERR", "failed", null));

        ItemResponse skipped = client.skip("wf.commands", first.itemId());
        ClaimResponse next = client.claim("wf.commands", new ClaimRequest("worker-2", List.of("wf.command"), 60, 1));

        assertEquals("skipped", skipped.status());
        assertEquals(2, next.items().getFirst().sequenceNo());
    }

    /**
     * Verifies direct client reads flyway schema version.
     */
    @Test
    void directClientReadsFlywaySchemaVersion() {
        assertEquals("1", client.getSchemaInfo().schemaVersion());
    }

    /**
     * Verifies direct worker run once completes one item against flyway managed postgres.
     */
    @Test
    void directWorkerRunOnceCompletesOneItemAgainstFlywayManagedPostgres() {
        EnqueueResponse enqueued = client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-direct-worker")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        SequencedQueueDirectWorker worker = client.worker("wf.commands")
            .workerId("direct-worker-1")
            .leaseSeconds(60)
            .handler("wf.command", item -> DirectQueueResult.success(Map.of("handledBy", "direct-worker-1")))
            .build();

        assertTrue(worker.runOnce());
        assertFalse(worker.runOnce());
        ClaimResponse nextClaim = client.claim("wf.commands", new ClaimRequest("worker-2", List.of("wf.command"), 60, 1));

        assertTrue(nextClaim.items().isEmpty());
        assertEquals("succeeded", itemStatus(enqueued.itemId()));
    }

    @Test
    void notificationOptionsRejectUppercaseChannel() {
        assertThrows(IllegalArgumentException.class, () -> PostgresNotificationOptions.enabled().channel("BadChannel"));
    }

    @Test
    void notifyWorkerBuildDoesNotOpenListenerConnection() {
        TrackingDataSource trackingDataSource = new TrackingDataSource(dataSource);
        SequencedQueueDirectClient trackingClient = SequencedQueueDirectClient.builder()
            .dataSource(trackingDataSource)
            .defaultQueueName("wf.commands")
            .build();

        trackingClient.worker("wf.commands")
            .workerId("notify-worker-build")
            .handler("wf.command", item -> DirectQueueResult.success(Map.of()))
            .waitStrategy(DirectWorkerWaitStrategy.postgresNotify())
            .build();

        assertEquals(0, trackingDataSource.connectionCount());
    }

    @Test
    void notifyWorkerRunOnceDoesNotOpenWaitStrategy() {
        CountingWaitStrategy waitStrategy = new CountingWaitStrategy();
        SequencedQueueDirectWorker worker = client.worker("wf.commands")
            .workerId("notify-worker-run-once")
            .handler("wf.command", item -> DirectQueueResult.success(Map.of()))
            .waitStrategy(waitStrategy)
            .build();

        assertFalse(worker.runOnce());

        assertEquals(0, waitStrategy.openCalls());
    }

    @Test
    void notifyWorkerRunForeverOpensAndClosesWaitStrategy() throws Exception {
        CountingWaitStrategy waitStrategy = new CountingWaitStrategy();
        SequencedQueueDirectWorker worker = client.worker("wf.commands")
            .workerId("notify-worker-run-forever")
            .handler("wf.command", item -> DirectQueueResult.success(Map.of()))
            .waitStrategy(waitStrategy)
            .build();
        Thread workerThread = new Thread(worker::runForever, "notify-worker-lifecycle-test");
        workerThread.start();
        try {
            assertTrue(waitStrategy.awaitOpened());
        } finally {
            worker.stop();
            workerThread.join(2000);
        }

        assertFalse(workerThread.isAlive());
        assertEquals(1, waitStrategy.openCalls());
        assertEquals(1, waitStrategy.closeCalls());
    }

    @Test
    void notifyWorkerDefaultFallbackStopsPromptlyWhileWaiting() throws Exception {
        SequencedQueueDirectWorker worker = client.worker("wf.commands")
            .workerId("notify-worker-stop")
            .handler("wf.command", item -> DirectQueueResult.success(Map.of()))
            .waitStrategy(DirectWorkerWaitStrategy.postgresNotify())
            .build();
        Thread workerThread = new Thread(worker::runForever, "notify-worker-stop-test");
        workerThread.start();
        Thread.sleep(500);

        worker.stop();
        workerThread.join(2000);

        assertFalse(workerThread.isAlive());
    }

    @Test
    void notifyWorkerWakesWhenDirectEnqueueEmitsPgNotify() throws Exception {
        SequencedQueueDirectClient notifyingClient = notifyingClient();
        CountDownLatch handled = new CountDownLatch(1);
        SequencedQueueDirectWorker worker = notifyingClient.worker("wf.commands")
            .workerId("notify-worker-1")
            .handler("wf.command", item -> {
                handled.countDown();
                return DirectQueueResult.success(Map.of("mode", "notify"));
            })
            .waitStrategy(DirectWorkerWaitStrategy.postgresNotify()
                .fallbackPollInterval(Duration.ofSeconds(5)))
            .build();
        Thread workerThread = new Thread(worker::runForever, "notify-worker-test");
        workerThread.start();
        try {
            Thread.sleep(300);
            EnqueueResponse enqueued = notifyingClient.enqueue("wf.commands", EnqueueRequest.builder()
                .sourceId("notify-source")
                .itemType("wf.command")
                .payloadJson("{}")
                .headersJson("{}")
                .build());

            assertTrue(handled.await(5, TimeUnit.SECONDS));
            assertEventuallyStatus(enqueued.itemId(), "succeeded");
        } finally {
            worker.stop();
            workerThread.join(5000);
        }
    }

    @Test
    void notifyWorkerProcessesAlreadyExistingWorkBeforeWaiting() throws Exception {
        SequencedQueueDirectClient notifyingClient = notifyingClient();
        EnqueueResponse enqueued = notifyingClient.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("notify-existing")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        CountDownLatch handled = new CountDownLatch(1);
        SequencedQueueDirectWorker worker = notifyingClient.worker("wf.commands")
            .workerId("notify-worker-2")
            .handler("wf.command", item -> {
                handled.countDown();
                return DirectQueueResult.success(Map.of());
            })
            .waitStrategy(DirectWorkerWaitStrategy.postgresNotify()
                .fallbackPollInterval(Duration.ofSeconds(5)))
            .build();
        Thread workerThread = new Thread(worker::runForever, "notify-worker-existing-test");
        workerThread.start();
        try {
            assertTrue(handled.await(5, TimeUnit.SECONDS));
            assertEventuallyStatus(enqueued.itemId(), "succeeded");
        } finally {
            worker.stop();
            workerThread.join(5000);
        }
    }

    @Test
    void notifyWorkerFallbackProcessesWorkWithoutNotification() throws Exception {
        SequencedQueueDirectClient notifyingClient = notifyingClient();
        CountDownLatch handled = new CountDownLatch(1);
        SequencedQueueDirectWorker worker = notifyingClient.worker("wf.commands")
            .workerId("notify-worker-3")
            .handler("wf.command", item -> {
                handled.countDown();
                return DirectQueueResult.success(Map.of());
            })
            .waitStrategy(DirectWorkerWaitStrategy.postgresNotify()
                .fallbackPollInterval(Duration.ofMillis(200)))
            .build();
        Thread workerThread = new Thread(worker::runForever, "notify-worker-fallback-test");
        workerThread.start();
        try {
            Thread.sleep(300);
            EnqueueResponse enqueued = client.enqueue("wf.commands", EnqueueRequest.builder()
                .sourceId("notify-fallback")
                .itemType("wf.command")
                .payloadJson("{}")
                .headersJson("{}")
                .build());

            assertTrue(handled.await(5, TimeUnit.SECONDS));
            assertEventuallyStatus(enqueued.itemId(), "succeeded");
        } finally {
            worker.stop();
            workerThread.join(5000);
        }
    }

    @Test
    void notifyWaiterIgnoresOtherQueueNames() throws Exception {
        DirectWorkerWaitStrategy.Waiter waiter = DirectWorkerWaitStrategy.postgresNotify()
            .fallbackPollInterval(Duration.ofMillis(400))
            .open(dataSource);
        try {
            notifyWakeup("other.queue");

            long ignoredStart = System.nanoTime();
            waiter.waitForWork("wf.commands");
            long ignoredMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ignoredStart);

            notifyWakeup("wf.commands");
            long matchedStart = System.nanoTime();
            waiter.waitForWork("wf.commands");
            long matchedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - matchedStart);

            assertTrue(ignoredMillis >= 250);
            assertTrue(matchedMillis < 250);
        } finally {
            waiter.close();
        }
    }

    @Test
    void notifyWaiterReconnectsForSafetySweepAfterListenerConnectionCloses() throws Exception {
        TrackingDataSource trackingDataSource = new TrackingDataSource(dataSource);
        DirectWorkerWaitStrategy.Waiter waiter = DirectWorkerWaitStrategy.postgresNotify()
            .fallbackPollInterval(Duration.ofSeconds(1))
            .open(trackingDataSource);
        try {
            trackingDataSource.lastConnection().close();

            waiter.waitForWork("wf.commands");

            assertTrue(trackingDataSource.connectionCount() >= 2);
        } finally {
            waiter.close();
        }
    }

    @Test
    void notifyWorkersPreserveSameSourceNonConcurrency() throws Exception {
        SequencedQueueDirectClient notifyingClient = notifyingClient();
        EnqueueResponse first = notifyingClient.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("same-source-notify")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        EnqueueResponse second = notifyingClient.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("same-source-notify")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        SequencedQueueDirectWorker workerOne = notifyWorker(notifyingClient, "notify-worker-same-1", item -> {
            if (item.sequenceNo() == 1) {
                firstStarted.countDown();
                await(releaseFirst);
            } else {
                secondStarted.countDown();
            }
            return DirectQueueResult.success(Map.of());
        });
        SequencedQueueDirectWorker workerTwo = notifyWorker(notifyingClient, "notify-worker-same-2", item -> {
            if (item.sequenceNo() == 1) {
                firstStarted.countDown();
                await(releaseFirst);
            } else {
                secondStarted.countDown();
            }
            return DirectQueueResult.success(Map.of());
        });
        Thread threadOne = new Thread(workerOne::runForever, "notify-worker-same-1");
        Thread threadTwo = new Thread(workerTwo::runForever, "notify-worker-same-2");
        threadOne.start();
        threadTwo.start();
        try {
            assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            assertFalse(secondStarted.await(500, TimeUnit.MILLISECONDS));

            releaseFirst.countDown();

            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            assertEventuallyStatus(first.itemId(), "succeeded");
            assertEventuallyStatus(second.itemId(), "succeeded");
        } finally {
            releaseFirst.countDown();
            workerOne.stop();
            workerTwo.stop();
            threadOne.join(5000);
            threadTwo.join(5000);
        }
    }

    @Test
    void notifyWorkersAllowDifferentSourceConcurrency() throws Exception {
        SequencedQueueDirectClient notifyingClient = notifyingClient();
        EnqueueResponse first = notifyingClient.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("different-source-notify-1")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        EnqueueResponse second = notifyingClient.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("different-source-notify-2")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releaseBoth = new CountDownLatch(1);
        SequencedQueueDirectWorker workerOne = notifyWorker(notifyingClient, "notify-worker-different-1", item -> {
            bothStarted.countDown();
            await(releaseBoth);
            return DirectQueueResult.success(Map.of());
        });
        SequencedQueueDirectWorker workerTwo = notifyWorker(notifyingClient, "notify-worker-different-2", item -> {
            bothStarted.countDown();
            await(releaseBoth);
            return DirectQueueResult.success(Map.of());
        });
        Thread threadOne = new Thread(workerOne::runForever, "notify-worker-different-1");
        Thread threadTwo = new Thread(workerTwo::runForever, "notify-worker-different-2");
        threadOne.start();
        threadTwo.start();
        try {
            assertTrue(bothStarted.await(5, TimeUnit.SECONDS));

            releaseBoth.countDown();

            assertEventuallyStatus(first.itemId(), "succeeded");
            assertEventuallyStatus(second.itemId(), "succeeded");
        } finally {
            releaseBoth.countDown();
            workerOne.stop();
            workerTwo.stop();
            threadOne.join(5000);
            threadTwo.join(5000);
        }
    }

    private static void applySchema(DataSource dataSource) throws Exception {
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load()
            .migrate();
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static SequencedQueueDirectClient notifyingClient() {
        return SequencedQueueDirectClient.builder()
            .dataSource(dataSource)
            .defaultQueueName("wf.commands")
            .validateSchemaOnBuild(true)
            .postgresNotifications(PostgresNotificationOptions.enabled())
            .build();
    }

    private static SequencedQueueDirectWorker notifyWorker(
        SequencedQueueDirectClient notifyingClient,
        String workerId,
        java.util.function.Function<ClaimItem, DirectQueueResult> handler
    ) {
        return notifyingClient.worker("wf.commands")
            .workerId(workerId)
            .handler("wf.command", handler)
            .waitStrategy(DirectWorkerWaitStrategy.postgresNotify()
                .fallbackPollInterval(Duration.ofMillis(200)))
            .build();
    }

    private static void notifyWakeup(String queueName) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            PostgresQueueNotifier.onDefaultChannel().notifyWorkAvailable(connection, new QueueWakeupEvent(queueName, QueueWakeupReason.ENQUEUE));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static String itemStatus(java.util.UUID itemId) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             var resultSet = statement.executeQuery("SELECT status FROM queue_item WHERE item_id = '" + itemId + "'")) {
            resultSet.next();
            return resultSet.getString("status");
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertEventuallyStatus(java.util.UUID itemId, String expectedStatus) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        String status = itemStatus(itemId);
        while (!expectedStatus.equals(status) && System.nanoTime() < deadline) {
            Thread.sleep(25);
            status = itemStatus(itemId);
        }
        assertEquals(expectedStatus, status);
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

    private static final class TrackingDataSource implements DataSource {
        private final DataSource delegate;
        private final List<Connection> connections = new ArrayList<>();
        private final AtomicInteger connectionCount = new AtomicInteger();

        private TrackingDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection connection = delegate.getConnection();
            connections.add(connection);
            connectionCount.incrementAndGet();
            return connection;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            Connection connection = delegate.getConnection(username, password);
            connections.add(connection);
            connectionCount.incrementAndGet();
            return connection;
        }

        Connection lastConnection() {
            return connections.getLast();
        }

        int connectionCount() {
            return connectionCount.get();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }

    private static final class CountingWaitStrategy implements DirectWorkerWaitStrategy {
        private final CountDownLatch opened = new CountDownLatch(1);
        private final AtomicInteger openCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private volatile boolean closed;

        @Override
        public Waiter open(DataSource dataSource) {
            openCalls.incrementAndGet();
            opened.countDown();
            return new Waiter() {
                @Override
                public void waitForWork(String queueName) {
                    while (!closed) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }

                @Override
                public void close() {
                    closed = true;
                    closeCalls.incrementAndGet();
                }
            };
        }

        boolean awaitOpened() throws InterruptedException {
            return opened.await(5, TimeUnit.SECONDS);
        }

        int openCalls() {
            return openCalls.get();
        }

        int closeCalls() {
            return closeCalls.get();
        }
    }
}
