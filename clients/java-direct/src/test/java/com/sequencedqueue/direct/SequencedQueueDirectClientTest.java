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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
