package com.sequencedqueue.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

class SequencedQueueDirectWorkerTest {
    @Test
    void directWorkerDoesNotCompleteAfterHeartbeatLosesLease() {
        FakeTransport transport = new FakeTransport("type");
        SequencedQueueDirectWorker worker = worker(transport, Map.of(
            "type", item -> DirectQueueResult.success(Map.of("ok", true))
        ));

        assertTrue(worker.runOnce());

        assertEquals(0, transport.completeCalls);
        assertEquals(0, transport.failCalls);
    }

    @Test
    void directWorkerDoesNotFailAfterHeartbeatLosesLease() {
        FakeTransport transport = new FakeTransport("type");
        SequencedQueueDirectWorker worker = worker(transport, Map.of(
            "type", item -> DirectQueueResult.failure("ERR", "failed")
        ));

        assertTrue(worker.runOnce());

        assertEquals(0, transport.completeCalls);
        assertEquals(0, transport.failCalls);
    }

    @Test
    void directWorkerNoHandlerDoesNotFailAfterHeartbeatLosesLease() {
        FakeTransport transport = new FakeTransport("unhandled");
        SequencedQueueDirectWorker worker = worker(transport, Map.of());

        assertTrue(worker.runOnce());

        assertEquals(0, transport.completeCalls);
        assertEquals(0, transport.failCalls);
    }

    private static SequencedQueueDirectWorker worker(FakeTransport transport, Map<String, java.util.function.Function<ClaimItem, DirectQueueResult>> handlers) {
        return new SequencedQueueDirectWorker(
            transport,
            new ImmediateLeaseLostMonitorFactory(),
            "wf.commands",
            "worker-1",
            List.of("type", "unhandled"),
            60,
            handlers
        );
    }

    private static final class ImmediateLeaseLostMonitorFactory implements DirectWorkerLeaseMonitorFactory {
        @Override
        public DirectWorkerLeaseMonitor start(String queueName, UUID leaseId, HeartbeatRequest request, AtomicBoolean leaseLost) {
            leaseLost.set(true);
            return () -> {
            };
        }
    }

    private static final class FakeTransport implements DirectWorkerTransport {
        private final ClaimResponse claim;
        private int completeCalls;
        private int failCalls;

        FakeTransport(String itemType) {
            UUID leaseId = UUID.randomUUID();
            ClaimItem item = new ClaimItem(UUID.randomUUID(), 1, itemType, Map.of(), Map.of());
            this.claim = new ClaimResponse(leaseId, "wf.commands", "source-1", OffsetDateTime.now().plusSeconds(60), List.of(item));
        }

        @Override
        public ClaimResponse claim(String queueName, ClaimRequest request) {
            return claim;
        }

        @Override
        public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
            completeCalls++;
            return item(itemId, "succeeded");
        }

        @Override
        public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
            failCalls++;
            return item(itemId, request.retryable() ? "retry_wait" : "failed");
        }

        private ItemResponse item(UUID itemId, String status) {
            OffsetDateTime now = OffsetDateTime.now();
            return new ItemResponse(itemId, "wf.commands", "source-1", 1, "type", Map.of(), Map.of(), status, now, null, null, null, 1, 5, null, null, null, Map.of(), now, now);
        }
    }
}
