package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
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
class ApiKeySecurityTest {
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
    void queueEndpointRejectsMissingApiKey() {
        ResponseEntity<Map> response = post("/queues/" + QUEUE + "/items", Map.of(), Map.class, new HttpHeaders());

        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
        assertEquals("UNAUTHORIZED", response.getBody().get("errorCode"));
    }

    @Test
    void queueEndpointAcceptsWorkerApiKey() {
        ResponseEntity<Map> response = post("/queues/" + QUEUE + "/items",
            Map.of("sourceId", "source-1", "itemType", "type", "payload", Map.of(), "headers", Map.of()),
            Map.class,
            bearer("dev-key"));

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void adminEndpointRejectsWorkerApiKey() {
        ResponseEntity<Map> response = get("/admin/queues/" + QUEUE + "/blocked-sources", Map.class, bearer("dev-key"));

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        assertEquals("FORBIDDEN", response.getBody().get("errorCode"));
    }

    @Test
    void adminEndpointAcceptsAdminApiKey() {
        ResponseEntity<Map[]> response = get("/admin/queues/" + QUEUE + "/blocked-sources", Map[].class, bearer("dev-admin-key"));

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void equalApiKeysAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ApiKeyFilter("same-key", "same-key", new ObjectMapper()));
    }

    private <T> ResponseEntity<T> post(String path, Object body, Class<T> responseType, HttpHeaders headers) {
        return rest.exchange(url(path), HttpMethod.POST, new HttpEntity<>(body, headers), responseType);
    }

    private <T> ResponseEntity<T> get(String path, Class<T> responseType, HttpHeaders headers) {
        return rest.exchange(url(path), HttpMethod.GET, new HttpEntity<>(headers), responseType);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static HttpHeaders bearer(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        return headers;
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
