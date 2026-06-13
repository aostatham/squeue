package com.example.sequencedqueue.examples;

import java.time.OffsetDateTime;
import java.util.Map;

import com.example.sequencedqueue.client.SequencedQueueClient;

public final class JavaProducerExample {
    private JavaProducerExample() {
    }

    public static void main(String[] args) {
        String baseUrl = env("SQ_BASE_URL", "http://localhost:8080");
        String apiKey = env("SQ_API_KEY", "dev-key");
        String queueName = env("SQ_QUEUE", "wf.commands");
        String sourceId = env("SQ_SOURCE_ID", "example-source");
        String itemType = env("SQ_ITEM_TYPE", "example.command");

        SequencedQueueClient client = SequencedQueueClient.builder()
            .baseUrl(baseUrl)
            .apiKey(apiKey)
            .build();

        var response = client.enqueue(queueName, new SequencedQueueClient.EnqueueRequest(
            sourceId,
            itemType,
            null,
            Map.of("message", "hello from Java", "createdBy", "java-producer"),
            Map.of("example", true),
            OffsetDateTime.now(),
            5
        ));

        System.out.printf("enqueued itemId=%s queue=%s source=%s sequenceNo=%d status=%s%n",
            response.itemId(), response.queueName(), response.sourceId(), response.sequenceNo(), response.status());
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
