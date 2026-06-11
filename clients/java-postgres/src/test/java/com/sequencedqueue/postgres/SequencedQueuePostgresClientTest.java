package com.sequencedqueue.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class SequencedQueuePostgresClientTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static SequencedQueuePostgresClient client;

    @BeforeAll
    static void setUp() throws Exception {
        DataSource dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        applySchema(dataSource);
        client = SequencedQueuePostgresClient.builder()
            .dataSource(dataSource)
            .defaultQueueName("wf.commands")
            .build();
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

    private static void applySchema(DataSource dataSource) throws Exception {
        Path migrationPath = Path.of("sequenced-queue-server/src/main/resources/db/migration/V1__initial_queue_schema.sql");
        if (!Files.exists(migrationPath)) {
            migrationPath = Path.of("../../sequenced-queue-server/src/main/resources/db/migration/V1__initial_queue_schema.sql");
        }
        String migration = Files.readString(migrationPath);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(migration);
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
