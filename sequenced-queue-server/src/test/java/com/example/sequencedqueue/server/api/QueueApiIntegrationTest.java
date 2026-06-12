package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class QueueApiIntegrationTest {
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
        execute("TRUNCATE queue_item, queue_source_state");
    }

    @Test
    void restEnqueueThenClaimThenComplete() {
        UUID itemId = UUID.fromString((String) enqueue("source-1", 5).getBody().get("itemId"));
        Map<String, Object> claim = claim("worker-1").getBody();
        UUID leaseId = UUID.fromString((String) claim.get("leaseId"));

        ResponseEntity<Map> complete = post("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-1", "leaseId", leaseId, "result", Map.of("ok", true)), Map.class);

        assertEquals(HttpStatus.OK, complete.getStatusCode());
        assertEquals("succeeded", complete.getBody().get("status"));
    }

    @Test
    void restWrongWorkerCannotComplete() {
        UUID itemId = UUID.fromString((String) enqueue("source-1", 5).getBody().get("itemId"));
        Map<String, Object> claim = claim("worker-1").getBody();

        ResponseEntity<Map> complete = post("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-2", "leaseId", claim.get("leaseId"), "result", Map.of()), Map.class);

        assertEquals(HttpStatus.CONFLICT, complete.getStatusCode());
    }

    @Test
    void restWrongLeaseCannotComplete() {
        UUID itemId = UUID.fromString((String) enqueue("source-1", 5).getBody().get("itemId"));
        claim("worker-1");

        ResponseEntity<Map> complete = post("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-1", "leaseId", UUID.randomUUID(), "result", Map.of()), Map.class);

        assertEquals(HttpStatus.CONFLICT, complete.getStatusCode());
    }

    @Test
    void restHeartbeatAfterExpiryFails() throws Exception {
        enqueue("source-1", 5);
        Map<String, Object> claim = claim("worker-1").getBody();
        execute("UPDATE queue_item SET lease_until = now() - interval '1 second'");
        execute("UPDATE queue_source_state SET lease_until = now() - interval '1 second'");

        ResponseEntity<Void> heartbeat = post("/queues/" + QUEUE + "/leases/" + claim.get("leaseId") + "/heartbeat", Map.of("workerId", "worker-1", "extendBySeconds", 60), Void.class);

        assertEquals(HttpStatus.CONFLICT, heartbeat.getStatusCode());
    }

    @Test
    void restDeadLetteredHeadBlocksLaterItem() {
        enqueue("source-1", 1);
        enqueue("source-1", 5);
        Map<String, Object> claim = claim("worker-1").getBody();
        UUID itemId = firstClaimItemId(claim);

        ResponseEntity<Map> fail = post("/queues/" + QUEUE + "/items/" + itemId + "/fail", Map.of("workerId", "worker-1", "leaseId", claim.get("leaseId"), "retryable", true, "errorType", "ERR", "errorMessage", "failed"), Map.class);
        Map<String, Object> secondClaim = claim("worker-2").getBody();

        assertEquals(HttpStatus.OK, fail.getStatusCode());
        assertEquals("dead_lettered", fail.getBody().get("status"));
        assertTrue(((List<?>) secondClaim.get("items")).isEmpty());
    }

    @Test
    void restAdminSkipDeadLetteredHeadUnblocksSource() {
        UUID firstId = UUID.fromString((String) enqueue("source-1", 1).getBody().get("itemId"));
        enqueue("source-1", 5);
        Map<String, Object> claim = claim("worker-1").getBody();
        post("/queues/" + QUEUE + "/items/" + firstId + "/fail", Map.of("workerId", "worker-1", "leaseId", claim.get("leaseId"), "retryable", true, "errorType", "ERR", "errorMessage", "failed"), Map.class);

        ResponseEntity<Map> skipped = post("/admin/queues/" + QUEUE + "/items/" + firstId + "/skip", Map.of(), Map.class, adminHeaders());
        Map<String, Object> nextClaim = claim("worker-2").getBody();

        assertEquals(HttpStatus.OK, skipped.getStatusCode());
        assertEquals("skipped", skipped.getBody().get("status"));
        assertFalse(((List<?>) nextClaim.get("items")).isEmpty());
    }

    private ResponseEntity<Map> enqueue(String sourceId, int maxAttempts) {
        return post("/queues/" + QUEUE + "/items", Map.of("sourceId", sourceId, "itemType", "type", "payload", Map.of("source", sourceId), "headers", Map.of(), "maxAttempts", maxAttempts), Map.class);
    }

    private ResponseEntity<Map> claim(String workerId) {
        return post("/queues/" + QUEUE + "/claims", Map.of("workerId", workerId, "supportedItemTypes", List.of("type"), "leaseSeconds", 60, "maxItems", 1), Map.class);
    }

    private UUID firstClaimItemId(Map<String, Object> claim) {
        List<Map<String, Object>> items = (List<Map<String, Object>>) claim.get("items");
        assertNotNull(items);
        assertFalse(items.isEmpty());
        return UUID.fromString((String) items.getFirst().get("itemId"));
    }

    private <T> ResponseEntity<T> post(String path, Object body, Class<T> responseType) {
        return post(path, body, responseType, workerHeaders());
    }

    private <T> ResponseEntity<T> post(String path, Object body, Class<T> responseType, HttpHeaders headers) {
        return rest.exchange(url(path), HttpMethod.POST, new HttpEntity<>(body, headers), responseType);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders workerHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth("dev-key");
        return headers;
    }

    private HttpHeaders adminHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth("dev-admin-key");
        return headers;
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
