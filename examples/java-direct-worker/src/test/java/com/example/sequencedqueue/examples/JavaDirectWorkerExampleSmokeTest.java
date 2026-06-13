package com.example.sequencedqueue.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.sequencedqueue.direct.EnqueueRequest;
import com.sequencedqueue.direct.SequencedQueueDirectClient;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class JavaDirectWorkerExampleSmokeTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static PGSimpleDataSource dataSource;

    @BeforeAll
    static void setUp() {
        dataSource = dataSource();
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load()
            .migrate();
    }

    @BeforeEach
    void clearTables() throws Exception {
        execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
    }

    @Test
    void javaDirectWorkerCompletesOneItemAgainstFlywayManagedPostgres() throws Exception {
        SequencedQueueDirectClient client = SequencedQueueDirectClient.builder()
            .dataSource(dataSource)
            .validateSchemaOnBuild(true)
            .build();
        client.enqueue("wf.commands", EnqueueRequest.builder()
            .sourceId("java-direct-worker-smoke")
            .itemType("example.command")
            .payloadJson("{}")
            .headersJson("{}")
            .build());

        boolean handled = JavaDirectWorkerExample.runOnce(client, "wf.commands", "java-direct-worker-smoke", "example.command", 60);

        assertTrue(handled);
        assertEquals("succeeded", itemStatus("java-direct-worker-smoke"));
    }

    private static PGSimpleDataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String itemStatus(String sourceId) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT status FROM queue_item WHERE source_id = '" + sourceId + "'")) {
            resultSet.next();
            return resultSet.getString("status");
        }
    }
}
