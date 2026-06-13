package com.example.sequencedqueue.client;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class SequencedQueueClient {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAPS_TYPE = new TypeReference<>() {
    };

    private final URI baseUrl;
    private final String apiKey;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private SequencedQueueClient(Builder builder) {
        this.baseUrl = URI.create(stripSlash(builder.baseUrl));
        this.apiKey = builder.apiKey;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public static Builder builder() {
        return new Builder();
    }

    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        return post("/queues/" + pathSegment(queueName) + "/items", request, EnqueueResponse.class);
    }

    public ClaimResponse claim(String queueName, ClaimRequest request) {
        return post("/queues/" + pathSegment(queueName) + "/claims", request, ClaimResponse.class);
    }

    public void complete(String queueName, UUID itemId, CompleteRequest request) {
        post("/queues/" + pathSegment(queueName) + "/items/" + pathSegment(itemId.toString()) + "/complete", request, Map.class);
    }

    public void fail(String queueName, UUID itemId, FailRequest request) {
        post("/queues/" + pathSegment(queueName) + "/items/" + pathSegment(itemId.toString()) + "/fail", request, Map.class);
    }

    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        postNoBody("/queues/" + pathSegment(queueName) + "/leases/" + pathSegment(leaseId.toString()) + "/heartbeat", request);
    }

    public List<Map<String, Object>> sourceItems(String queueName, String sourceId) {
        return get("/queues/" + pathSegment(queueName) + "/sources/" + pathSegment(sourceId) + "/items", LIST_OF_MAPS_TYPE);
    }

    public SequencedQueueWorker.Builder worker(String queueName) {
        return SequencedQueueWorker.builder(this, queueName);
    }

    private <T> T post(String path, Object body, Class<T> responseType) {
        try {
            HttpResponse<String> response = httpClient.send(request(path, body), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new QueueClientException(response.statusCode(), response.body());
            }
            return objectMapper.readValue(response.body(), responseType);
        } catch (IOException e) {
            throw new QueueClientException(0, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueClientException(0, "interrupted");
        }
    }

    private void postNoBody(String path, Object body) {
        try {
            HttpResponse<String> response = httpClient.send(request(path, body), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new QueueClientException(response.statusCode(), response.body());
            }
        } catch (IOException e) {
            throw new QueueClientException(0, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueClientException(0, "interrupted");
        }
    }

    private <T> T get(String path, TypeReference<T> responseType) {
        try {
            HttpResponse<String> response = httpClient.send(getRequest(path), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new QueueClientException(response.statusCode(), response.body());
            }
            return objectMapper.readValue(response.body(), responseType);
        } catch (IOException e) {
            throw new QueueClientException(0, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueClientException(0, "interrupted");
        }
    }

    private HttpRequest request(String path, Object body) throws IOException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(resolvePath(path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)));
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        return builder.build();
    }

    private HttpRequest getRequest(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(resolvePath(path))
            .GET();
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        return builder.build();
    }

    private URI resolvePath(String path) {
        String base = baseUrl.toString();
        String suffix = path.startsWith("/") ? path.substring(1) : path;
        return URI.create(base + "/" + suffix);
    }

    private static String stripSlash(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("baseUrl is required");
        }
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    Map<String, Object> readMap(Object value) {
        return objectMapper.convertValue(value, MAP_TYPE);
    }

    private static String pathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    public static final class Builder {
        private String baseUrl;
        private String apiKey;

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        public SequencedQueueClient build() {
            return new SequencedQueueClient(this);
        }
    }

    public record EnqueueRequest(String sourceId, String itemType, String idempotencyKey, Map<String, Object> payload, Map<String, Object> headers, OffsetDateTime availableAt, Integer maxAttempts) {
        public static EnqueueRequest of(String sourceId, String itemType, Map<String, Object> payload) {
            return new EnqueueRequest(sourceId, itemType, null, payload, Map.of(), null, null);
        }
    }

    public record EnqueueResponse(UUID itemId, String queueName, String sourceId, long sequenceNo, String status) {
    }

    public record ClaimRequest(String workerId, List<String> supportedItemTypes, Integer leaseSeconds, Integer maxItems) {
    }

    public record ClaimItem(UUID itemId, long sequenceNo, String itemType, Map<String, Object> payload, Map<String, Object> headers) {
    }

    public record ClaimResponse(UUID leaseId, String queueName, String sourceId, OffsetDateTime leaseUntil, List<ClaimItem> items) {
    }

    public record CompleteRequest(String workerId, UUID leaseId, Map<String, Object> result) {
    }

    public record FailRequest(String workerId, UUID leaseId, boolean retryable, String errorType, String errorMessage, Integer backoffSeconds) {
    }

    public record HeartbeatRequest(String workerId, Integer extendBySeconds) {
    }
}
