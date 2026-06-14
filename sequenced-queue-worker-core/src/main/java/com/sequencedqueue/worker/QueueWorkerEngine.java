package com.sequencedqueue.worker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class QueueWorkerEngine<I, R> implements AutoCloseable {
    private final Transport<I> transport;
    private final LeaseMonitorFactory leaseMonitorFactory;
    private final ItemView<I> itemView;
    private final ResultView<R> resultView;
    private final String queueName;
    private final String workerId;
    private final List<String> supportedItemTypes;
    private final int leaseSeconds;
    private final Map<String, Function<I, R>> handlers;
    private volatile boolean running = true;

    public QueueWorkerEngine(
        Transport<I> transport,
        LeaseMonitorFactory leaseMonitorFactory,
        ItemView<I> itemView,
        ResultView<R> resultView,
        String queueName,
        String workerId,
        List<String> supportedItemTypes,
        int leaseSeconds,
        Map<String, Function<I, R>> handlers
    ) {
        this.transport = Objects.requireNonNull(transport, "transport is required");
        this.leaseMonitorFactory = Objects.requireNonNull(leaseMonitorFactory, "leaseMonitorFactory is required");
        this.itemView = Objects.requireNonNull(itemView, "itemView is required");
        this.resultView = Objects.requireNonNull(resultView, "resultView is required");
        this.queueName = requireText(queueName, "queueName is required");
        this.workerId = requireText(workerId, "workerId is required");
        this.supportedItemTypes = List.copyOf(supportedItemTypes);
        this.leaseSeconds = leaseSeconds;
        this.handlers = Map.copyOf(handlers);
        if (this.supportedItemTypes.isEmpty()) {
            throw new IllegalArgumentException("at least one supported item type is required");
        }
        if (leaseSeconds <= 0) {
            throw new IllegalArgumentException("leaseSeconds must be greater than zero");
        }
    }

    public void runForever() {
        long emptySleepMillis = 100;
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                if (!runOnce()) {
                    sleep(emptySleepMillis);
                    emptySleepMillis = Math.min(5000, emptySleepMillis * 2);
                    continue;
                }
                emptySleepMillis = 100;
            } catch (RuntimeException e) {
                if (!running || Thread.currentThread().isInterrupted()) {
                    return;
                }
                throw e;
            }
        }
    }

    public boolean runOnce() {
        Claim<I> claim = transport.claim(queueName, new ClaimCommand(workerId, supportedItemTypes, leaseSeconds, 1));
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

    private void handleClaim(Claim<I> claim) {
        AtomicBoolean leaseLost = new AtomicBoolean(false);
        LeaseMonitor leaseMonitor = leaseMonitorFactory.start(queueName, claim.leaseId(), new HeartbeatCommand(workerId, leaseSeconds), leaseLost);
        try {
            I item = claim.items().getFirst();
            Function<I, R> handler = handlers.get(itemView.itemType(item));
            if (handler == null) {
                if (leaseLost.get()) {
                    return;
                }
                transport.fail(queueName, itemView.itemId(item), new FailCommand(workerId, claim.leaseId(), false, "NO_HANDLER", "No handler for " + itemView.itemType(item), null));
                return;
            }
            R result = handler.apply(item);
            if (leaseLost.get()) {
                return;
            }
            if (resultView.succeeded(result)) {
                transport.complete(queueName, itemView.itemId(item), new CompleteCommand(workerId, claim.leaseId(), resultView.result(result)));
            } else {
                transport.fail(queueName, itemView.itemId(item), new FailCommand(workerId, claim.leaseId(), resultView.retryable(result), resultView.errorType(result), resultView.errorMessage(result), null));
            }
        } catch (Exception e) {
            if (leaseLost.get()) {
                return;
            }
            I item = claim.items().getFirst();
            transport.fail(queueName, itemView.itemId(item), new FailCommand(workerId, claim.leaseId(), true, e.getClass().getSimpleName(), e.getMessage(), null));
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

    private static String requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static LeaseMonitorFactory scheduledLeaseMonitorFactory(HeartbeatSender heartbeatSender) {
        return (queueName, leaseId, command, leaseLost) -> {
            ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
            int leaseSeconds = command.extendBySeconds();
            heartbeats.scheduleAtFixedRate(
                () -> {
                    try {
                        heartbeatSender.heartbeat(queueName, leaseId, command);
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
        };
    }

    public interface Transport<I> {
        Claim<I> claim(String queueName, ClaimCommand command);

        void complete(String queueName, UUID itemId, CompleteCommand command);

        void fail(String queueName, UUID itemId, FailCommand command);
    }

    public interface HeartbeatSender {
        void heartbeat(String queueName, UUID leaseId, HeartbeatCommand command);
    }

    public interface LeaseMonitor extends AutoCloseable {
        @Override
        void close();
    }

    public interface LeaseMonitorFactory {
        LeaseMonitor start(String queueName, UUID leaseId, HeartbeatCommand command, AtomicBoolean leaseLost);
    }

    public interface ItemView<I> {
        UUID itemId(I item);

        String itemType(I item);
    }

    public interface ResultView<R> {
        boolean succeeded(R result);

        boolean retryable(R result);

        String errorType(R result);

        String errorMessage(R result);

        Map<String, Object> result(R result);
    }

    public record Claim<I>(UUID leaseId, List<I> items) {
    }

    public record ClaimCommand(String workerId, List<String> supportedItemTypes, int leaseSeconds, int maxItems) {
    }

    public record HeartbeatCommand(String workerId, int extendBySeconds) {
    }

    public record CompleteCommand(String workerId, UUID leaseId, Map<String, Object> result) {
    }

    public record FailCommand(String workerId, UUID leaseId, boolean retryable, String errorType, String errorMessage, Integer backoffSeconds) {
    }
}
