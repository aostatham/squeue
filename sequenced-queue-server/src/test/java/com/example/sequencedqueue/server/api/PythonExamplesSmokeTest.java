package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PythonExamplesSmokeTest {
    private static final String QUEUE = "wf.commands";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

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
    void pythonRestProducerEnqueuesAgainstRunningServer() throws Exception {
        runPythonExample("examples/python-producer/producer.py", Map.of(
            "SQ_SOURCE_ID", "python-producer-smoke"
        ));

        assertEquals("pending", itemStatus("python-producer-smoke"));
    }

    @Test
    void pythonRestWorkerCompletesOneItem() throws Exception {
        post("/queues/" + QUEUE + "/items", Map.of(
            "sourceId", "python-worker-smoke",
            "itemType", "example.command",
            "payload", Map.of("message", "smoke"),
            "headers", Map.of(),
            "maxAttempts", 5
        ));

        runPythonExample("examples/python-worker/worker.py", Map.of(
            "SQ_RUN_ONCE", "true",
            "SQ_WORKER_ID", "python-worker-smoke"
        ));

        assertEquals("succeeded", itemStatus("python-worker-smoke"));
    }

    private void runPythonExample(String script, Map<String, String> extraEnvironment) throws Exception {
        Path repoRoot = repoRoot();
        ProcessBuilder processBuilder = new ProcessBuilder("python", repoRoot.resolve(script).toString());
        processBuilder.directory(repoRoot.toFile());
        processBuilder.environment().put("PYTHONPATH", repoRoot.resolve("sequenced-queue-python-client").toString());
        processBuilder.environment().put("SQ_BASE_URL", "http://localhost:" + port);
        processBuilder.environment().put("SQ_API_KEY", "dev-key");
        processBuilder.environment().put("SQ_QUEUE", QUEUE);
        processBuilder.environment().put("SQ_ITEM_TYPE", "example.command");
        processBuilder.environment().putAll(extraEnvironment);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        boolean exited = process.waitFor(Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS);
        String output = new String(process.getInputStream().readAllBytes());
        if (!exited) {
            process.destroyForcibly();
            throw new AssertionError("python example timed out: " + output);
        }
        assertEquals(0, process.exitValue(), output);
    }

    private void post(String path, Object body) {
        rest.exchange(url(path), HttpMethod.POST, new HttpEntity<>(body, workerHeaders()), Map.class);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static HttpHeaders workerHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth("dev-key");
        return headers;
    }

    private static Path repoRoot() {
        Path current = Path.of("").toAbsolutePath();
        return current.getFileName().toString().equals("sequenced-queue-server") ? current.getParent() : current;
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
