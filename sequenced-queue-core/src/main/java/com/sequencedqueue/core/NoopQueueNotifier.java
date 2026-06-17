package com.sequencedqueue.core;

import java.sql.Connection;

public final class NoopQueueNotifier implements QueueNotifier {
    public static final NoopQueueNotifier INSTANCE = new NoopQueueNotifier();

    private NoopQueueNotifier() {
    }

    @Override
    public void notifyWorkAvailable(Connection connection, QueueWakeupEvent event) {
        // Default notifier intentionally preserves existing behavior.
    }
}
