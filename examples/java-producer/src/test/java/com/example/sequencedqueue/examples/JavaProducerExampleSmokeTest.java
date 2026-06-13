package com.example.sequencedqueue.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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
class JavaProducerExampleSmokeTest {
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
    void javaRestProducerEnqueuesAgainstRunningServer() {
        var response = JavaProducerExample.run(new JavaProducerExample.Config(
            "http://localhost:" + port,
            "dev-key",
            "wf.commands",
            "java-producer-smoke",
            "example.command"
        ));

        assertEquals("wf.commands", response.queueName());
        assertEquals("java-producer-smoke", response.sourceId());
        assertEquals("pending", response.status());
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
