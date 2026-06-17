package com.sequencedqueue.direct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.sql.DataSource;

import com.sequencedqueue.worker.QueueWorkerEngine;
import com.sequencedqueue.worker.QueueWorkerEngine.Claim;
import com.sequencedqueue.worker.QueueWorkerEngine.ClaimCommand;
import com.sequencedqueue.worker.QueueWorkerEngine.CompleteCommand;
import com.sequencedqueue.worker.QueueWorkerEngine.FailCommand;
import com.sequencedqueue.worker.QueueWorkerEngine.HeartbeatCommand;

public final class SequencedQueueDirectWorker implements AutoCloseable {
    private final QueueWorkerEngine<ClaimItem, DirectQueueResult> engine;
    private final DirectWorkerWaitStrategy waitStrategy;
    private final DataSource dataSource;
    private volatile DirectWorkerWaitStrategy.Waiter waiter;
    private volatile boolean running = true;

    private SequencedQueueDirectWorker(Builder builder) {
        DirectWorkerTransport transport = new ClientDirectWorkerTransport(builder.client);
        this.waitStrategy = builder.waitStrategy;
        this.dataSource = builder.client.dataSource();
        this.engine = engine(
            transport,
            QueueWorkerEngine.scheduledLeaseMonitorFactory(transport),
            builder.queueName,
            builder.workerId,
            builder.supportedItemTypes,
            builder.leaseSeconds,
            builder.handlers
        );
    }

    SequencedQueueDirectWorker(
        DirectWorkerTransport transport,
        QueueWorkerEngine.LeaseMonitorFactory leaseMonitorFactory,
        String queueName,
        String workerId,
        List<String> supportedItemTypes,
        int leaseSeconds,
        Map<String, Function<ClaimItem, DirectQueueResult>> handlers
    ) {
        this.waitStrategy = null;
        this.dataSource = null;
        this.engine = engine(transport, leaseMonitorFactory, queueName, workerId, supportedItemTypes, leaseSeconds, handlers);
    }

    private static QueueWorkerEngine<ClaimItem, DirectQueueResult> engine(
        DirectWorkerTransport transport,
        QueueWorkerEngine.LeaseMonitorFactory leaseMonitorFactory,
        String queueName,
        String workerId,
        List<String> supportedItemTypes,
        int leaseSeconds,
        Map<String, Function<ClaimItem, DirectQueueResult>> handlers
    ) {
        return new QueueWorkerEngine<>(
            transport,
            leaseMonitorFactory,
            new DirectItemView(),
            new DirectResultView(),
            queueName,
            workerId,
            supportedItemTypes,
            leaseSeconds,
            handlers
        );
    }

    public static Builder builder(SequencedQueueDirectClient client, String queueName) {
        return new Builder(client, queueName);
    }

    public void runForever() {
        if (waitStrategy == null) {
            engine.runForever();
            return;
        }
        if (!running) {
            return;
        }
        DirectWorkerWaitStrategy.Waiter openedWaiter = waitStrategy.open(dataSource);
        waiter = openedWaiter;
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                while (running && engine.runOnce()) {
                    // Drain currently available work using the normal claim path before waiting.
                }
                if (running) {
                    openedWaiter.waitForWork(engineQueueName());
                }
            }
        } catch (RuntimeException e) {
            if (!running || Thread.currentThread().isInterrupted()) {
                return;
            }
            throw e;
        } finally {
            openedWaiter.close();
            if (waiter == openedWaiter) {
                waiter = null;
            }
        }
    }

    public boolean runOnce() {
        return engine.runOnce();
    }

    public void stop() {
        running = false;
        engine.stop();
        if (waiter != null) {
            waiter.close();
        }
    }

    @Override
    public void close() {
        stop();
    }

    public static final class Builder {
        private final SequencedQueueDirectClient client;
        private final String queueName;
        private String workerId;
        private int leaseSeconds = 60;
        private final List<String> supportedItemTypes = new ArrayList<>();
        private final Map<String, Function<ClaimItem, DirectQueueResult>> handlers = new HashMap<>();
        private DirectWorkerWaitStrategy waitStrategy;

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

        public Builder waitStrategy(DirectWorkerWaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
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

    private String engineQueueName() {
        return engine.queueName();
    }
}

interface DirectWorkerTransport extends QueueWorkerEngine.Transport<ClaimItem>, QueueWorkerEngine.HeartbeatSender {
    @Override
    default Claim<ClaimItem> claim(String queueName, ClaimCommand command) {
        ClaimResponse claim = claim(queueName, new ClaimRequest(command.workerId(), command.supportedItemTypes(), command.leaseSeconds(), command.maxItems()));
        return new Claim<>(claim.leaseId(), claim.items());
    }

    ClaimResponse claim(String queueName, ClaimRequest request);

    @Override
    default void heartbeat(String queueName, java.util.UUID leaseId, HeartbeatCommand command) {
        heartbeat(queueName, leaseId, new HeartbeatRequest(command.workerId(), command.extendBySeconds()));
    }

    void heartbeat(String queueName, java.util.UUID leaseId, HeartbeatRequest request);

    @Override
    default void complete(String queueName, java.util.UUID itemId, CompleteCommand command) {
        complete(queueName, itemId, new CompleteRequest(command.workerId(), command.leaseId(), command.result()));
    }

    ItemResponse complete(String queueName, java.util.UUID itemId, CompleteRequest request);

    @Override
    default void fail(String queueName, java.util.UUID itemId, FailCommand command) {
        fail(queueName, itemId, new FailRequest(command.workerId(), command.leaseId(), command.retryable(), command.errorType(), command.errorMessage(), command.backoffSeconds()));
    }

    ItemResponse fail(String queueName, java.util.UUID itemId, FailRequest request);
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
    public void heartbeat(String queueName, java.util.UUID leaseId, HeartbeatRequest request) {
        client.heartbeat(queueName, leaseId, request);
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

final class DirectItemView implements QueueWorkerEngine.ItemView<ClaimItem> {
    @Override
    public java.util.UUID itemId(ClaimItem item) {
        return item.itemId();
    }

    @Override
    public String itemType(ClaimItem item) {
        return item.itemType();
    }
}

final class DirectResultView implements QueueWorkerEngine.ResultView<DirectQueueResult> {
    @Override
    public boolean succeeded(DirectQueueResult result) {
        return result.succeeded();
    }

    @Override
    public boolean retryable(DirectQueueResult result) {
        return result.retryable();
    }

    @Override
    public String errorType(DirectQueueResult result) {
        return result.errorType();
    }

    @Override
    public String errorMessage(DirectQueueResult result) {
        return result.errorMessage();
    }

    @Override
    public Map<String, Object> result(DirectQueueResult result) {
        return result.result();
    }
}
