package com.sequencedqueue.direct;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sequencedqueue.core.PostgresQueueNotifier;

public final class PostgresNotifyWorkerWaitStrategy implements DirectWorkerWaitStrategy {
    private static final Duration DEFAULT_FALLBACK_POLL_INTERVAL = Duration.ofSeconds(30);

    private final String channel;
    private final Duration fallbackPollInterval;

    private PostgresNotifyWorkerWaitStrategy(String channel, Duration fallbackPollInterval) {
        this.channel = PostgresQueueNotifier.validateChannel(channel);
        this.fallbackPollInterval = requirePositive(fallbackPollInterval);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String channel() {
        return channel;
    }

    public Duration fallbackPollInterval() {
        return fallbackPollInterval;
    }

    @Override
    public Waiter open(DataSource dataSource) {
        return new PostgresNotifyWaiter(dataSource, channel, fallbackPollInterval);
    }

    private static Duration requirePositive(Duration duration) {
        Duration normalized = duration == null ? DEFAULT_FALLBACK_POLL_INTERVAL : duration;
        if (normalized.isZero() || normalized.isNegative()) {
            throw new IllegalArgumentException("fallbackPollInterval must be > 0");
        }
        return normalized;
    }

    public static final class Builder implements DirectWorkerWaitStrategy {
        private String channel = PostgresQueueNotifier.DEFAULT_CHANNEL;
        private Duration fallbackPollInterval = DEFAULT_FALLBACK_POLL_INTERVAL;

        public Builder channel(String channel) {
            this.channel = PostgresQueueNotifier.validateChannel(channel);
            return this;
        }

        public Builder fallbackPollInterval(Duration fallbackPollInterval) {
            this.fallbackPollInterval = requirePositive(fallbackPollInterval);
            return this;
        }

        public PostgresNotifyWorkerWaitStrategy build() {
            return new PostgresNotifyWorkerWaitStrategy(channel, fallbackPollInterval);
        }

        @Override
        public Waiter open(DataSource dataSource) {
            return build().open(dataSource);
        }
    }

    private static final class PostgresNotifyWaiter implements Waiter {
        private final DataSource dataSource;
        private final String channel;
        private final Duration fallbackPollInterval;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private Connection connection;
        private PGConnection pgConnection;
        private volatile boolean closed;

        private PostgresNotifyWaiter(DataSource dataSource, String channel, Duration fallbackPollInterval) {
            this.dataSource = Objects.requireNonNull(dataSource, "dataSource is required");
            this.channel = channel;
            this.fallbackPollInterval = fallbackPollInterval;
            connect();
        }

        @Override
        public void waitForWork(String queueName) {
            long deadline = System.nanoTime() + fallbackPollInterval.toNanos();
            while (!closed && System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
                int remainingMillis = (int) Math.max(1, Math.min(Integer.MAX_VALUE, Duration.ofNanos(deadline - System.nanoTime()).toMillis()));
                try {
                    PGNotification[] notifications = pgConnection.getNotifications(remainingMillis);
                    if (notifications == null || notifications.length == 0) {
                        return;
                    }
                    for (PGNotification notification : notifications) {
                        if (matchesQueue(notification.getParameter(), queueName)) {
                            return;
                        }
                    }
                } catch (SQLException e) {
                    reconnectForSafetySweep();
                    return;
                }
            }
        }

        @Override
        public void close() {
            closed = true;
            closeConnection();
        }

        private void connect() {
            if (closed) {
                return;
            }
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(true);
                try (Statement statement = connection.createStatement()) {
                    statement.execute("LISTEN " + channel);
                }
                pgConnection = connection.unwrap(PGConnection.class);
            } catch (SQLException e) {
                closeConnection();
                throw new QueueUnavailableException("PostgreSQL LISTEN connection could not be opened", e);
            }
        }

        private void reconnectForSafetySweep() {
            if (closed) {
                return;
            }
            closeConnection();
            connect();
        }

        private boolean matchesQueue(String payload, String queueName) {
            try {
                JsonNode node = objectMapper.readTree(payload);
                JsonNode payloadQueue = node.get("queueName");
                return payloadQueue != null && queueName.equals(payloadQueue.asText());
            } catch (Exception e) {
                return false;
            }
        }

        private void closeConnection() {
            Connection current = connection;
            connection = null;
            pgConnection = null;
            if (current != null) {
                try (Statement statement = current.createStatement()) {
                    statement.execute("UNLISTEN " + channel);
                } catch (SQLException ignored) {
                }
                try {
                    current.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
