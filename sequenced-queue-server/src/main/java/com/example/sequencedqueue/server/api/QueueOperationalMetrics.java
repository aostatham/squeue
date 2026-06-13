package com.example.sequencedqueue.server.api;

import com.sequencedqueue.core.QueueDtos.QueueMetricsSnapshot;
import com.sequencedqueue.core.QueueOperations;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class QueueOperationalMetrics {
    private final QueueOperations queueOperations;
    private final Counter claimsTotal;
    private final Counter claimsEmpty;
    private final Counter completionsTotal;
    private final Counter failuresTotal;
    private final Counter heartbeatsTotal;
    private final Counter heartbeatsFailed;
    private final Counter leaseExpiriesTotal;
    private final Counter adminRetryTotal;
    private final Counter adminSkipTotal;
    private final Counter adminCancelTotal;
    private final Counter adminUnblockTotal;

    public QueueOperationalMetrics(MeterRegistry registry, QueueOperations queueOperations) {
        this.queueOperations = queueOperations;
        this.claimsTotal = registry.counter("queue.claims.total");
        this.claimsEmpty = registry.counter("queue.claims.empty");
        this.completionsTotal = registry.counter("queue.completions.total");
        this.failuresTotal = registry.counter("queue.failures.total");
        this.heartbeatsTotal = registry.counter("queue.heartbeats.total");
        this.heartbeatsFailed = registry.counter("queue.heartbeats.failed");
        this.leaseExpiriesTotal = registry.counter("queue.lease_expiries.total");
        this.adminRetryTotal = registry.counter("queue.admin.retry.total");
        this.adminSkipTotal = registry.counter("queue.admin.skip.total");
        this.adminCancelTotal = registry.counter("queue.admin.cancel.total");
        this.adminUnblockTotal = registry.counter("queue.admin.unblock.total");

        Gauge.builder("queue.items.pending", this, metrics -> metrics.snapshot().pendingItems()).register(registry);
        Gauge.builder("queue.items.processing", this, metrics -> metrics.snapshot().processingItems()).register(registry);
        Gauge.builder("queue.items.retry_wait", this, metrics -> metrics.snapshot().retryWaitItems()).register(registry);
        Gauge.builder("queue.items.dead_lettered", this, metrics -> metrics.snapshot().deadLetteredItems()).register(registry);
        Gauge.builder("queue.sources.idle", this, metrics -> metrics.snapshot().idleSources()).register(registry);
        Gauge.builder("queue.sources.leased", this, metrics -> metrics.snapshot().leasedSources()).register(registry);
        Gauge.builder("queue.sources.blocked", this, metrics -> metrics.snapshot().blockedSources()).register(registry);
    }

    public void claim(boolean empty) {
        claimsTotal.increment();
        if (empty) {
            claimsEmpty.increment();
        }
    }

    public void completion() {
        completionsTotal.increment();
    }

    public void failure() {
        failuresTotal.increment();
    }

    public void heartbeat() {
        heartbeatsTotal.increment();
    }

    public void heartbeatFailed() {
        heartbeatsFailed.increment();
    }

    public void leaseExpiries(int count) {
        if (count > 0) {
            leaseExpiriesTotal.increment(count);
        }
    }

    public void adminRetry() {
        adminRetryTotal.increment();
    }

    public void adminSkip() {
        adminSkipTotal.increment();
    }

    public void adminCancel() {
        adminCancelTotal.increment();
    }

    public void adminUnblock() {
        adminUnblockTotal.increment();
    }

    private QueueMetricsSnapshot snapshot() {
        return queueOperations.metricsSnapshot();
    }
}
