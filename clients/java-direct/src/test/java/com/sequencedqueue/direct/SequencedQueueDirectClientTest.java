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

    @Test
    void directWrongLeaseCannotComplete() {
        client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("inst-wrong-lease")
            .itemType("wf.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());
        ClaimResponse claim = client.claim("wf.commands", new ClaimRequest("worker-1", List.of("wf.command"), 60, 1));

        QueueConflictException error = assertThrows(QueueConflictException.class, () ->
            client.complete("wf.commands", claim.items().getFirst().itemId(), new CompleteRequest("worker-1", java.util.UUID.randomUUID(), Map.of())));

        assertEquals(QueueConflictException.class, error.getClass());
    }

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

        assertThrows(QueueConflictException.class, () ->
            client.heartbeat("wf.commands", claim.leaseId(), new HeartbeatRequest("worker-1", 60)));
    }

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

    @Test
    void directClientReadsFlywaySchemaVersion() {
        assertEquals("2", client.getSchemaInfo().schemaVersion());
    }

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
