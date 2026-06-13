package com.example.sequencedqueue.examples;

import java.util.List;
import java.util.Map;

import com.sequencedqueue.direct.ClaimRequest;
import com.sequencedqueue.direct.CompleteRequest;
import com.sequencedqueue.direct.FailRequest;
import com.sequencedqueue.direct.SequencedQueueDirectClient;
import org.postgresql.ds.PGSimpleDataSource;

public final class JavaDirectWorkerExample {
    private JavaDirectWorkerExample() {
    }

    public static void main(String[] args) throws InterruptedException {
        String queueName = env("SQ_QUEUE", "wf.commands");
        String workerId = env("SQ_WORKER_ID", "java-direct-worker");
        String itemType = env("SQ_ITEM_TYPE", "example.command");
        int leaseSeconds = Integer.parseInt(env("SQ_LEASE_SECONDS", "30"));

        SequencedQueueDirectClient client = SequencedQueueDirectClient.builder()
            .dataSource(dataSource())
            .validateSchemaOnBuild(true)
            .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            System.out.println("java direct worker stopping")));

        while (!Thread.currentThread().isInterrupted()) {
            var claim = client.claim(queueName, new ClaimRequest(workerId, List.of(itemType), leaseSeconds, 1));
            if (claim.items().isEmpty()) {
                Thread.sleep(500);
                continue;
            }

            var item = claim.items().getFirst();
            try {
                System.out.printf("java direct worker handled itemId=%s sequenceNo=%d payload=%s%n",
                    item.itemId(), item.sequenceNo(), item.payload());
                client.complete(queueName, item.itemId(), new CompleteRequest(workerId, claim.leaseId(), Map.of("handledBy", workerId)));
            } catch (RuntimeException e) {
                client.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), true, e.getClass().getSimpleName(), e.getMessage(), null));
            }
        }
    }

    private static PGSimpleDataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(env("DATABASE_URL", "jdbc:postgresql://localhost:5432/sequenced_queue"));
        dataSource.setUser(env("DATABASE_USERNAME", "sequenced_queue"));
        dataSource.setPassword(env("DATABASE_PASSWORD", "sequenced_queue"));
        return dataSource;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
