package com.sequencedqueue.direct;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class SequencedQueueDirectWorker implements AutoCloseable {
    private final DirectWorkerTransport transport;
    private final DirectWorkerLeaseMonitorFactory leaseMonitorFactory;
    private final String queueName;
    private final String workerId;
    private final List<String> supportedItemTypes;
    private final int leaseSeconds;
    private final Map<String, Function<ClaimItem, DirectQueueResult>> handlers;
    private volatile boolean running = true;

    private SequencedQueueDirectWorker(Builder builder) {
        this.transport = new ClientDirectWorkerTransport(builder.client);
        this.leaseMonitorFactory = new ScheduledDirectWorkerLeaseMonitorFactory(builder.client);
        this.queueName = builder.queueName;
        this.workerId = builder.workerId;
        this.supportedItemTypes = List.copyOf(builder.supportedItemTypes);
        this.leaseSeconds = builder.leaseSeconds;
        this.handlers = Map.copyOf(builder.handlers);
    }

    SequencedQueueDirectWorker(
        DirectWorkerTransport transport,
        DirectWorkerLeaseMonitorFactory leaseMonitorFactory,
        String queueName,
        String workerId,
        List<String> supportedItemTypes,
        int leaseSeconds,
        Map<String, Function<ClaimItem, DirectQueueResult>> handlers
    ) {
        this.transport = transport;
        this.leaseMonitorFactory = leaseMonitorFactory;
        this.queueName = queueName;
        this.workerId = workerId;
        this.supportedItemTypes = List.copyOf(supportedItemTypes);
        this.leaseSeconds = leaseSeconds;
        this.handlers = Map.copyOf(handlers);
    }

    public static Builder builder(SequencedQueueDirectClient client, String queueName) {
        return new Builder(client, queueName);
    }

    public void runForever() {
        long emptySleepMillis = 100;
        while (running && !Thread.currentThread().isInterrupted()) {
            if (!runOnce()) {
                sleep(emptySleepMillis);
                emptySleepMillis = Math.min(5000, emptySleepMillis * 2);
                continue;
            }
            emptySleepMillis = 100;
        }
    }

    public boolean runOnce() {
        ClaimResponse claim = transport.claim(queueName, new ClaimRequest(workerId, supportedItemTypes, leaseSeconds, 1));
        if (claim.items() == null || claim.items().isEmpty()) {
            return false;
        }
        handleClaim(claim);
        return true;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        stop();
    }

    private void handleClaim(ClaimResponse claim) {
        AtomicBoolean leaseLost = new AtomicBoolean(false);
        DirectWorkerLeaseMonitor leaseMonitor = leaseMonitorFactory.start(queueName, claim.leaseId(), new HeartbeatRequest(workerId, leaseSeconds), leaseLost);
        try {
            ClaimItem item = claim.items().getFirst();
            Function<ClaimItem, DirectQueueResult> handler = handlers.get(item.itemType());
            if (handler == null) {
                if (leaseLost.get()) {
                    return;
                }
                transport.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), false, "NO_HANDLER", "No handler for " + item.itemType(), null));
                return;
            }
            DirectQueueResult result = handler.apply(item);
            if (leaseLost.get()) {
                return;
            }
            if (result.succeeded()) {
                transport.complete(queueName, item.itemId(), new CompleteRequest(workerId, claim.leaseId(), result.result()));
            } else {
                transport.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), result.retryable(), result.errorType(), result.errorMessage(), null));
            }
        } catch (Exception e) {
            if (leaseLost.get()) {
                return;
            }
            ClaimItem item = claim.items().getFirst();
            transport.fail(queueName, item.itemId(), new FailRequest(workerId, claim.leaseId(), true, e.getClass().getSimpleName(), e.getMessage(), null));
        } finally {
            leaseMonitor.close();
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
        private final SequencedQueueDirectClient client;
        private final String queueName;
        private String workerId;
        private int leaseSeconds = 60;
        private final List<String> supportedItemTypes = new ArrayList<>();
        private final Map<String, Function<ClaimItem, DirectQueueResult>> handlers = new HashMap<>();

        private Builder(SequencedQueueDirectClient client, String queueName) {
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

        public Builder handler(String itemType, Function<ClaimItem, DirectQueueResult> handler) {
            this.supportedItemTypes.add(itemType);
            this.handlers.put(itemType, handler);
            return this;
        }

        public SequencedQueueDirectWorker build() {
            if (workerId == null || workerId.isBlank()) {
                throw new IllegalArgumentException("workerId is required");
            }
            if (supportedItemTypes.isEmpty()) {
                throw new IllegalArgumentException("at least one supported item type is required");
            }
            return new SequencedQueueDirectWorker(this);
        }
    }
}

interface DirectWorkerTransport {
    ClaimResponse claim(String queueName, ClaimRequest request);

    ItemResponse complete(String queueName, java.util.UUID itemId, CompleteRequest request);

    ItemResponse fail(String queueName, java.util.UUID itemId, FailRequest request);
}

interface DirectWorkerLeaseMonitor extends AutoCloseable {
    @Override
    void close();
}

interface DirectWorkerLeaseMonitorFactory {
    DirectWorkerLeaseMonitor start(String queueName, java.util.UUID leaseId, HeartbeatRequest request, AtomicBoolean leaseLost);
}

final class ClientDirectWorkerTransport implements DirectWorkerTransport {
    private final SequencedQueueDirectClient client;

    ClientDirectWorkerTransport(SequencedQueueDirectClient client) {
        this.client = client;
    }

    @Override
    public ClaimResponse claim(String queueName, ClaimRequest request) {
        return client.claim(queueName, request);
    }

    @Override
    public ItemResponse complete(String queueName, java.util.UUID itemId, CompleteRequest request) {
        return client.complete(queueName, itemId, request);
    }

    @Override
    public ItemResponse fail(String queueName, java.util.UUID itemId, FailRequest request) {
        return client.fail(queueName, itemId, request);
    }
}

final class ScheduledDirectWorkerLeaseMonitorFactory implements DirectWorkerLeaseMonitorFactory {
    private final SequencedQueueDirectClient client;

    ScheduledDirectWorkerLeaseMonitorFactory(SequencedQueueDirectClient client) {
        this.client = client;
    }

    @Override
    public DirectWorkerLeaseMonitor start(String queueName, java.util.UUID leaseId, HeartbeatRequest request, AtomicBoolean leaseLost) {
        ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
        int leaseSeconds = request.extendBySeconds() == null ? 60 : request.extendBySeconds();
        heartbeats.scheduleAtFixedRate(
            () -> {
                try {
                    client.heartbeat(queueName, leaseId, request);
                } catch (RuntimeException e) {
                    leaseLost.set(true);
                    heartbeats.shutdown();
                }
            },
            Math.max(1, leaseSeconds / 2),
            Math.max(1, leaseSeconds / 2),
            TimeUnit.SECONDS
        );
        return heartbeats::shutdownNow;
    }
}
