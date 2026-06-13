package com.example.sequencedqueue.examples;

import java.time.OffsetDateTime;
import java.util.Map;

import com.example.sequencedqueue.client.SequencedQueueClient;

public final class JavaProducerExample {
    private JavaProducerExample() {
    }

    public static void main(String[] args) {
        run(configFromEnv());
    }

    public static SequencedQueueClient.EnqueueResponse run(Config config) {
        SequencedQueueClient client = SequencedQueueClient.builder()
            .baseUrl(config.baseUrl())
            .apiKey(config.apiKey())
            .build();

        var response = client.enqueue(config.queueName(), new SequencedQueueClient.EnqueueRequest(
            config.sourceId(),
            config.itemType(),
            null,
            Map.of("message", "hello from Java", "createdBy", "java-producer"),
            Map.of("example", true),
            OffsetDateTime.now(),
            5
        ));

        System.out.printf("enqueued itemId=%s queue=%s source=%s sequenceNo=%d status=%s%n",
            response.itemId(), response.queueName(), response.sourceId(), response.sequenceNo(), response.status());
        return response;
    }

    private static Config configFromEnv() {
        String baseUrl = env("SQ_BASE_URL", "http://localhost:8080");
        String apiKey = env("SQ_API_KEY", "dev-key");
        String queueName = env("SQ_QUEUE", "wf.commands");
        String sourceId = env("SQ_SOURCE_ID", "example-source");
        String itemType = env("SQ_ITEM_TYPE", "example.command");
        return new Config(baseUrl, apiKey, queueName, sourceId, itemType);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    public record Config(String baseUrl, String apiKey, String queueName, String sourceId, String itemType) {
    }
}
