package com.sequencedqueue.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class PostgresQueueNotifier implements QueueNotifier {
    public static final String DEFAULT_CHANNEL = "sequenced_queue_wakeup";
    private static final Pattern CHANNEL_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]{0,62}$");

    private final String channel;
    private final ObjectMapper objectMapper;

    private PostgresQueueNotifier(String channel, ObjectMapper objectMapper) {
        this.channel = validateChannel(channel);
        this.objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
    }

    public static PostgresQueueNotifier onDefaultChannel() {
        return onChannel(DEFAULT_CHANNEL);
    }

    public static PostgresQueueNotifier onChannel(String channel) {
        return new PostgresQueueNotifier(channel, new ObjectMapper());
    }

    public String channel() {
        return channel;
    }

    @Override
    public void notifyWorkAvailable(Connection connection, QueueWakeupEvent event) throws SQLException {
        String payload = payload(event);
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_notify(?, ?)")) {
            statement.setString(1, channel);
            statement.setString(2, payload);
            statement.execute();
        }
    }

    private String payload(QueueWakeupEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "queue wake-up payload could not be written", e);
        }
    }

    public static String validateChannel(String channel) {
        if (channel == null || !CHANNEL_PATTERN.matcher(channel).matches()) {
            throw new IllegalArgumentException("PostgreSQL notification channel must match " + CHANNEL_PATTERN.pattern());
        }
        return channel;
    }
}
