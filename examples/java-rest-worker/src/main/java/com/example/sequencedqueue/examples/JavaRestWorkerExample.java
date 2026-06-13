package com.example.sequencedqueue.examples;

import java.util.Map;

import com.example.sequencedqueue.client.QueueResult;
import com.example.sequencedqueue.client.SequencedQueueClient;
import com.example.sequencedqueue.client.SequencedQueueWorker;

public final class JavaRestWorkerExample {
    private JavaRestWorkerExample() {
    }

    public static void main(String[] args) {
        String baseUrl = env("SQ_BASE_URL", "http://localhost:8080");
        String apiKey = env("SQ_API_KEY", "dev-key");
        String queueName = env("SQ_QUEUE", "wf.commands");
        String workerId = env("SQ_WORKER_ID", "java-rest-worker");
        String itemType = env("SQ_ITEM_TYPE", "example.command");
        int leaseSeconds = Integer.parseInt(env("SQ_LEASE_SECONDS", "30"));

        SequencedQueueClient client = SequencedQueueClient.builder()
            .baseUrl(baseUrl)
            .apiKey(apiKey)
            .build();

        SequencedQueueWorker worker = client.worker(queueName)
            .workerId(workerId)
            .leaseSeconds(leaseSeconds)
            .handler(itemType, item -> {
                System.out.printf("java REST worker handled itemId=%s sequenceNo=%d payload=%s%n",
                    item.itemId(), item.sequenceNo(), item.payload());
                return QueueResult.success(Map.of("handledBy", workerId));
            })
            .build();

        Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
        worker.runForever();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
