package com.sequencedqueue.direct;

import com.sequencedqueue.core.PostgresQueueNotifier;
import com.sequencedqueue.core.QueueNotifier;

public final class PostgresNotificationOptions {
    private final String channel;

    private PostgresNotificationOptions(String channel) {
        this.channel = PostgresQueueNotifier.validateChannel(channel);
    }

    public static PostgresNotificationOptions enabled() {
        return new PostgresNotificationOptions(PostgresQueueNotifier.DEFAULT_CHANNEL);
    }

    public PostgresNotificationOptions channel(String channel) {
        return new PostgresNotificationOptions(channel);
    }

    public String channel() {
        return channel;
    }

    QueueNotifier queueNotifier() {
        return PostgresQueueNotifier.onChannel(channel);
    }
}
