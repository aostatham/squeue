package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sequencedqueue.direct.ClaimRequest;
import com.sequencedqueue.direct.ClaimResponse;
import com.sequencedqueue.direct.CompleteRequest;
import com.sequencedqueue.direct.EnqueueRequest;
import com.sequencedqueue.direct.FailRequest;
import com.sequencedqueue.direct.QueueConflictException;
import com.sequencedqueue.direct.SequencedQueueDirectClient;
import org.junit.jupiter.api.BeforeAll;
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
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RestDirectCompatibilityTest {
    private static final String QUEUE = "wf.commands";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static SequencedQueueDirectClient direct;

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

    @BeforeAll
    static void setUpDirectClient() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        direct = SequencedQueueDirectClient.builder()
            .dataSource(dataSource)
            .build();
    }

    @BeforeEach
    void clearTables() throws Exception {
        execute("TRUNCATE queue_item, queue_source_state");
    }

    @Test
    void restEnqueueDirectClaimDirectComplete() {
        UUID itemId = UUID.fromString((String) restEnqueue("source-1", 5).getBody().get("itemId"));

        ClaimResponse claim = direct.claim(QUEUE, new ClaimRequest("worker-1", List.of("type"), 60, 1));
        var completed = direct.complete(QUEUE, itemId, new CompleteRequest("worker-1", claim.leaseId(), Map.of("ok", true)));

        assertEquals("succeeded", completed.status());
    }

    @Test
    void directEnqueueRestClaimRestComplete() {
        UUID itemId = directEnqueue("source-1", 5);
        Map<String, Object> claim = restClaim("worker-1").getBody();

        ResponseEntity<Map> complete = restPost("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-1", "leaseId", claim.get("leaseId"), "result", Map.of()), Map.class);

        assertEquals(HttpStatus.OK, complete.getStatusCode());
        assertEquals("succeeded", complete.getBody().get("status"));
    }

    @Test
    void restClaimDirectCompleteRequiresCorrectLease() {
        UUID itemId = UUID.fromString((String) restEnqueue("source-1", 5).getBody().get("itemId"));
        restClaim("worker-1");

        org.junit.jupiter.api.Assertions.assertThrows(QueueConflictException.class, () ->
            direct.complete(QUEUE, itemId, new CompleteRequest("worker-1", UUID.randomUUID(), Map.of())));
    }

    @Test
    void directClaimRestCompleteRequiresCorrectLease() {
        UUID itemId = directEnqueue("source-1", 5);
        direct.claim(QUEUE, new ClaimRequest("worker-1", List.of("type"), 60, 1));

        ResponseEntity<Map> complete = restPost("/queues/" + QUEUE + "/items/" + itemId + "/complete", Map.of("workerId", "worker-1", "leaseId", UUID.randomUUID(), "result", Map.of()), Map.class);

        assertEquals(HttpStatus.CONFLICT, complete.getStatusCode());
    }

    @Test
    void restAdminSkipAffectsDirectWorker() {
        UUID first = directEnqueue("source-1", 1);
        directEnqueue("source-1", 5);
        ClaimResponse claim = direct.claim(QUEUE, new ClaimRequest("worker-1", List.of("type"), 60, 1));
        direct.fail(QUEUE, first, new FailRequest("worker-1", claim.leaseId(), true, "ERR", "failed", null));

        ResponseEntity<Map> skipped = restPost("/admin/queues/" + QUEUE + "/items/" + first + "/skip", Map.of(), Map.class, adminHeaders());
        ClaimResponse next = direct.claim(QUEUE, new ClaimRequest("worker-2", List.of("type"), 60, 1));

        assertEquals(HttpStatus.OK, skipped.getStatusCode());
        assertFalse(next.items().isEmpty());
    }

    @Test
    void directAdminSkipAffectsRestWorker() {
        UUID first = UUID.fromString((String) restEnqueue("source-1", 1).getBody().get("itemId"));
        restEnqueue("source-1", 5);
        Map<String, Object> claim = restClaim("worker-1").getBody();
        restPost("/queues/" + QUEUE + "/items/" + first + "/fail", Map.of("workerId", "worker-1", "leaseId", claim.get("leaseId"), "retryable", true, "errorType", "ERR", "errorMessage", "failed"), Map.class);

        direct.skip(QUEUE, first);
        Map<String, Object> next = restClaim("worker-2").getBody();

        assertFalse(((List<?>) next.get("items")).isEmpty());
    }

    private UUID directEnqueue(String sourceId, int maxAttempts) {
        return direct.enqueue(QUEUE, EnqueueRequest.builder()
            .sourceId(sourceId)
            .itemType("type")
            .payloadJson("{}")
            .headersJson("{}")
            .maxAttempts(maxAttempts)
            .build()).itemId();
    }

    private ResponseEntity<Map> restEnqueue(String sourceId, int maxAttempts) {
        return restPost("/queues/" + QUEUE + "/items", Map.of("sourceId", sourceId, "itemType", "type", "payload", Map.of(), "headers", Map.of(), "maxAttempts", maxAttempts), Map.class);
    }

    private ResponseEntity<Map> restClaim(String workerId) {
        return restPost("/queues/" + QUEUE + "/claims", Map.of("workerId", workerId, "supportedItemTypes", List.of("type"), "leaseSeconds", 60, "maxItems", 1), Map.class);
    }

    private <T> ResponseEntity<T> restPost(String path, Object body, Class<T> responseType) {
        return restPost(path, body, responseType, workerHeaders());
    }

    private <T> ResponseEntity<T> restPost(String path, Object body, Class<T> responseType, HttpHeaders headers) {
        return rest.exchange("http://localhost:" + port + path, HttpMethod.POST, new HttpEntity<>(body, headers), responseType);
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
