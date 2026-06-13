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
        execute("TRUNCATE queue_admin_audit, queue_item, queue_source_state");
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

    @Test
    void adminInspectionEndpointsExposeDeadLetterBlockedSourceItemsItemDetailAndAudit() {
        UUID firstId = UUID.fromString((String) enqueue("source-1", 1).getBody().get("itemId"));
        UUID secondId = UUID.fromString((String) enqueue("source-1", 5).getBody().get("itemId"));
        Map<String, Object> claim = claim("worker-1").getBody();
        post("/queues/" + QUEUE + "/items/" + firstId + "/fail", Map.of("workerId", "worker-1", "leaseId", claim.get("leaseId"), "retryable", true, "errorType", "ERR", "errorMessage", "failed"), Map.class);

        ResponseEntity<Map[]> deadLettered = get("/admin/queues/" + QUEUE + "/dead-lettered", Map[].class, adminHeaders());
        ResponseEntity<Map[]> blocked = get("/admin/queues/" + QUEUE + "/blocked-sources", Map[].class, adminHeaders());
        ResponseEntity<Map[]> sourceItems = get("/admin/queues/" + QUEUE + "/sources/source-1/items", Map[].class, adminHeaders());
        ResponseEntity<Map> item = get("/admin/queues/" + QUEUE + "/items/" + firstId, Map.class, adminHeaders());

        assertEquals(HttpStatus.OK, deadLettered.getStatusCode());
        assertEquals(firstId.toString(), deadLettered.getBody()[0].get("itemId"));
        assertEquals("dead_lettered", deadLettered.getBody()[0].get("status"));
        assertEquals(HttpStatus.OK, blocked.getStatusCode());
        assertEquals("source-1", blocked.getBody()[0].get("sourceId"));
        assertEquals(firstId.toString(), blocked.getBody()[0].get("headItemId"));
        assertEquals("dead_lettered", blocked.getBody()[0].get("headItemStatus"));
        assertEquals(HttpStatus.OK, sourceItems.getStatusCode());
        assertEquals(firstId.toString(), sourceItems.getBody()[0].get("itemId"));
        assertEquals(secondId.toString(), sourceItems.getBody()[1].get("itemId"));
        assertEquals(HttpStatus.OK, item.getStatusCode());
        assertEquals(firstId.toString(), item.getBody().get("itemId"));

        post("/admin/queues/" + QUEUE + "/items/" + firstId + "/skip", Map.of("reason", "operator repair"), Map.class, adminHeaders());
        ResponseEntity<Map[]> audit = get("/admin/queues/" + QUEUE + "/audit", Map[].class, adminHeaders());

        assertEquals(HttpStatus.OK, audit.getStatusCode());
        assertEquals("skip", audit.getBody()[0].get("operation"));
        assertEquals(firstId.toString(), audit.getBody()[0].get("itemId"));
        assertEquals("source-1", audit.getBody()[0].get("sourceId"));
        assertEquals("dead_lettered", audit.getBody()[0].get("previousStatus"));
        assertEquals("skipped", audit.getBody()[0].get("newStatus"));
    }

    @Test
    void actuatorHealthIncludesQueueDetails() {
        ResponseEntity<Map> health = get("/actuator/health", Map.class, new HttpHeaders());

        assertEquals(HttpStatus.OK, health.getStatusCode());
        Map<String, Object> components = (Map<String, Object>) health.getBody().get("components");
        assertNotNull(components.get("queue"));
        Map<String, Object> queue = (Map<String, Object>) components.get("queue");
        assertEquals("UP", queue.get("status"));
        Map<String, Object> details = (Map<String, Object>) queue.get("details");
        assertEquals(true, details.get("queueItemTablePresent"));
        assertEquals(true, details.get("queueSourceStateTablePresent"));
        assertEquals(true, details.get("adminAuditTablePresent"));
        assertEquals("2", details.get("schemaVersion"));
        assertEquals(true, details.get("recoveryEnabled"));
    }

    @Test
    void metricsEndpointExposesCountersAndGauges() {
        UUID itemId = UUID.fromString((String) enqueue("source-1", 5).getBody().get("itemId"));
        assertEquals(1.0, metricValue("queue.items.pending"));

        Map<String, Object> claim = claim("worker-1").getBody();
        assertTrue(metricValue("queue.claims.total") >= 1.0);
        assertEquals(1.0, metricValue("queue.items.processing"));

        post("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-1", "leaseId", claim.get("leaseId"), "result", Map.of()), Map.class);

        assertTrue(metricValue("queue.completions.total") >= 1.0);
        assertEquals(1.0, metricValue("queue.sources.idle"));
    }

    @Test
    void structuredErrorResponsesAreStable() {
        UUID itemId = UUID.fromString((String) enqueue("source-1", 5).getBody().get("itemId"));
        claim("worker-1");

        ResponseEntity<Map> wrongLease = post("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-1", "leaseId", UUID.randomUUID(), "result", Map.of()), Map.class);
        ResponseEntity<Map> missingItem = get("/queues/" + QUEUE + "/items/" + UUID.randomUUID(), Map.class, workerHeaders());
        ResponseEntity<Map> invalidRequest = post("/queues/" + QUEUE + "/claims", Map.of("workerId", "worker-1", "supportedItemTypes", List.of("type"), "leaseSeconds", 0, "maxItems", 1), Map.class);

        assertEquals(HttpStatus.CONFLICT, wrongLease.getStatusCode());
        assertEquals("LEASE_LOST", wrongLease.getBody().get("errorCode"));
        assertEquals(HttpStatus.NOT_FOUND, missingItem.getStatusCode());
        assertEquals("ITEM_NOT_FOUND", missingItem.getBody().get("errorCode"));
        assertEquals(HttpStatus.BAD_REQUEST, invalidRequest.getStatusCode());
        assertEquals("VALIDATION_ERROR", invalidRequest.getBody().get("errorCode"));
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

    private <T> ResponseEntity<T> get(String path, Class<T> responseType, HttpHeaders headers) {
        return rest.exchange(url(path), HttpMethod.GET, new HttpEntity<>(headers), responseType);
    }

    private double metricValue(String name) {
        ResponseEntity<Map> response = get("/actuator/metrics/" + name, Map.class, new HttpHeaders());
        assertEquals(HttpStatus.OK, response.getStatusCode());
        List<Map<String, Object>> measurements = (List<Map<String, Object>>) response.getBody().get("measurements");
        assertFalse(measurements.isEmpty());
        return ((Number) measurements.getFirst().get("value")).doubleValue();
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
