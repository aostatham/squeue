package com.example.sequencedqueue.client;

import static com.example.sequencedqueue.client.SequencedQueueClient.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SequencedQueueWorker implements AutoCloseable {
    private final SequencedQueueClient client;
    private final String queueName;
    private final String workerId;
    private final List<String> supportedItemTypes;
    private final int leaseSeconds;
    private final Map<String, Function<ClaimItem, QueueResult>> handlers;
    private volatile boolean running = true;

    private SequencedQueueWorker(Builder builder) {
        this.client = builder.client;
        this.queueName = builder.queueName;
        this.workerId = builder.workerId;
        this.supportedItemTypes = List.copyOf(builder.supportedItemTypes);
        this.leaseSeconds = builder.leaseSeconds;
        this.handlers = Map.copyOf(builder.handlers);
    }

    public static Builder builder(SequencedQueueClient client, String queueName) {
        return new Builder(client, queueName);
    }

    public void runForever() {
        long emptySleepMillis = 100;
        while (running && !Thread.currentThread().isInterrupted()) {
            ClaimResponse claim = client.claim(queueName, new ClaimRequest(workerId, supportedItemTypes, leaseSeconds, 1));
            if (claim.items() == null || claim.items().isEmpty()) {
                sleep(emptySleepMillis);
                emptySleepMillis = Math.min(5000, emptySleepMillis * 2);
                continue;
            }
            emptySleepMillis = 100;
            handleClaim(claim);
        }
    }

    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        stop();
    }

    private void handleClaim(ClaimResponse claim) {
        ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
        heartbeats.scheduleAtFixedRate(
            () -> client.heartbeat(queueName, claim.leaseId(), new HeartbeatRequest(workerId, leaseSeconds)),
            Math.max(1, leaseSeconds / 2),
            Math.max(1, leaseSeconds / 2),
            TimeUnit.SECONDS
        );
        try {
            ClaimItem item = claim.items().getFirst();
            Function<ClaimItem, QueueResult> handler = handlers.get(item.itemType());
            if (handler == null) {
                client.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), false, "NO_HANDLER", "No handler for " + item.itemType(), null));
                return;
            }
            QueueResult result = handler.apply(item);
            if (result.succeeded()) {
                client.complete(queueName, item.itemId(), new CompleteRequest(workerId, claim.leaseId(), result.result()));
            } else {
                client.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), result.retryable(), result.errorType(), result.errorMessage(), null));
            }
        } catch (Exception e) {
            ClaimItem item = claim.items().getFirst();
            client.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), true, e.getClass().getSimpleName(), e.getMessage(), null));
        } finally {
            heartbeats.shutdownNow();
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(Duration.ofMillis(millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Builder {
        private final SequencedQueueClient client;
        private final String queueName;
        private String workerId;
        private int leaseSeconds = 60;
        private final List<String> supportedItemTypes = new ArrayList<>();
        private final Map<String, Function<ClaimItem, QueueResult>> handlers = new HashMap<>();

        private Builder(SequencedQueueClient client, String queueName) {
            this.client = client;
            this.queueName = queueName;
        }

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder supports(String itemType) {
            this.supportedItemTypes.add(itemType);
            return this;
        }

        public Builder leaseSeconds(int leaseSeconds) {
            this.leaseSeconds = leaseSeconds;
            return this;
        }

        public Builder handler(String itemType, Function<ClaimItem, QueueResult> handler) {
            this.supportedItemTypes.add(itemType);
            this.handlers.put(itemType, handler);
            return this;
        }

        public SequencedQueueWorker build() {
            if (workerId == null || workerId.isBlank()) {
                throw new IllegalArgumentException("workerId is required");
            }
            if (supportedItemTypes.isEmpty()) {
                throw new IllegalArgumentException("at least one supported item type is required");
            }
            return new SequencedQueueWorker(this);
        }
    }
}
