package com.example.sequencedqueue.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.Map;

import com.example.sequencedqueue.client.SequencedQueueClient;
import com.example.sequencedqueue.server.SequencedQueueServerApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(classes = SequencedQueueServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JavaRestWorkerExampleSmokeTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @BeforeEach
    void clearTables() throws Exception {
        execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
    }

    @Test
    void javaRestWorkerCompletesOneItem() throws Exception {
        String baseUrl = "http://localhost:" + port;
        client(baseUrl).enqueue("wf.commands", new SequencedQueueClient.EnqueueRequest(
            "java-rest-worker-smoke",
            "example.command",
            null,
            Map.of("message", "smoke"),
            Map.of(),
            OffsetDateTime.now(),
            5
        ));

        boolean handled = JavaRestWorkerExample.runOnce(baseUrl, "dev-key", "wf.commands", "java-rest-worker-smoke", "example.command", 60);

        assertTrue(handled);
        assertEquals("succeeded", itemStatus("java-rest-worker-smoke"));
    }

    private static SequencedQueueClient client(String baseUrl) {
        return SequencedQueueClient.builder()
            .baseUrl(baseUrl)
            .apiKey("dev-key")
            .build();
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
