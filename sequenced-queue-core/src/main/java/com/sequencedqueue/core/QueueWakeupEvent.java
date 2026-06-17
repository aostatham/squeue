package com.sequencedqueue.core;

public record QueueWakeupEvent(String queueName, QueueWakeupReason reason) {
    public QueueWakeupEvent {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("queueName is required");
        }
        if (reason == null) {
            throw new IllegalArgumentException("reason is required");
        }
    }
}
