package com.example.sequencedqueue.client;

import static com.example.sequencedqueue.client.SequencedQueueClient.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.sequencedqueue.worker.QueueWorkerEngine;
import com.sequencedqueue.worker.QueueWorkerEngine.Claim;
import com.sequencedqueue.worker.QueueWorkerEngine.ClaimCommand;
import com.sequencedqueue.worker.QueueWorkerEngine.CompleteCommand;
import com.sequencedqueue.worker.QueueWorkerEngine.FailCommand;
import com.sequencedqueue.worker.QueueWorkerEngine.HeartbeatCommand;

public class SequencedQueueWorker implements AutoCloseable {
    private final QueueWorkerEngine<ClaimItem, QueueResult> engine;

    private SequencedQueueWorker(Builder builder) {
        RestWorkerTransport transport = new RestWorkerTransport(builder.client);
        this.engine = new QueueWorkerEngine<>(
            transport,
            QueueWorkerEngine.scheduledLeaseMonitorFactory(transport),
            new RestItemView(),
            new RestResultView(),
            builder.queueName,
            builder.workerId,
            builder.supportedItemTypes,
            builder.leaseSeconds,
            builder.handlers
        );
    }

    public static Builder builder(SequencedQueueClient client, String queueName) {
        return new Builder(client, queueName);
    }

    public void runForever() {
        engine.runForever();
    }

    public boolean runOnce() {
        return engine.runOnce();
    }

    public void stop() {
        engine.stop();
    }

    @Override
    public void close() {
        stop();
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

final class RestWorkerTransport implements QueueWorkerEngine.Transport<ClaimItem>, QueueWorkerEngine.HeartbeatSender {
    private final SequencedQueueClient client;

    RestWorkerTransport(SequencedQueueClient client) {
        this.client = client;
    }

    @Override
    public Claim<ClaimItem> claim(String queueName, ClaimCommand command) {
        ClaimResponse claim = client.claim(queueName, new ClaimRequest(command.workerId(), command.supportedItemTypes(), command.leaseSeconds(), command.maxItems()));
        return new Claim<>(claim.leaseId(), claim.items());
    }

    @Override
    public void heartbeat(String queueName, java.util.UUID leaseId, HeartbeatCommand command) {
        client.heartbeat(queueName, leaseId, new HeartbeatRequest(command.workerId(), command.extendBySeconds()));
    }

    @Override
    public void complete(String queueName, java.util.UUID itemId, CompleteCommand command) {
        client.complete(queueName, itemId, new CompleteRequest(command.workerId(), command.leaseId(), command.result()));
    }

    @Override
    public void fail(String queueName, java.util.UUID itemId, FailCommand command) {
        client.fail(queueName, itemId, new FailRequest(command.workerId(), command.leaseId(), command.retryable(), command.errorType(), command.errorMessage(), command.backoffSeconds()));
    }
}

final class RestItemView implements QueueWorkerEngine.ItemView<ClaimItem> {
    @Override
    public java.util.UUID itemId(ClaimItem item) {
        return item.itemId();
    }

    @Override
    public String itemType(ClaimItem item) {
        return item.itemType();
    }
}

final class RestResultView implements QueueWorkerEngine.ResultView<QueueResult> {
    @Override
    public boolean succeeded(QueueResult result) {
        return result.succeeded();
    }

    @Override
    public boolean retryable(QueueResult result) {
        return result.retryable();
    }

    @Override
    public String errorType(QueueResult result) {
        return result.errorType();
    }

    @Override
    public String errorMessage(QueueResult result) {
        return result.errorMessage();
    }

    @Override
    public Map<String, Object> result(QueueResult result) {
        return result.result();
    }
}
