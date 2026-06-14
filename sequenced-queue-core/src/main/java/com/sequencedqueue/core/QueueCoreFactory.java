package com.sequencedqueue.core;

import java.time.Clock;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class QueueCoreFactory {
    private QueueCoreFactory() {
    }

    public static QueueOperations create(DataSource dataSource, ObjectMapper objectMapper, int defaultLeaseSeconds, int defaultMaxAttempts) {
        QueueSettings defaults = QueueSettings.defaults();
        return create(dataSource, objectMapper, new QueueSettings(
            defaultLeaseSeconds,
            defaults.maxLeaseSeconds(),
            defaultMaxAttempts,
            defaults.maxPayloadBytes(),
            defaults.maxHeadersBytes(),
            defaults.maxErrorMessageBytes(),
            defaults.maxAdminReasonBytes()
        ));
    }

    public static QueueOperations create(DataSource dataSource, ObjectMapper objectMapper, QueueSettings settings) {
        JdbcTransactionRunner transactions = new JdbcTransactionRunner(dataSource);
        PostgresQueueRepository repository = new PostgresQueueRepository(transactions);
        return new DefaultQueueService(repository, transactions, new RetryPolicy(), objectMapper, Clock.systemUTC(), settings);
    }
}
