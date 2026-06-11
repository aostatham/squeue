package com.sequencedqueue.core;

import java.time.Clock;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class QueueCoreFactory {
    private QueueCoreFactory() {
    }

    public static QueueOperations create(DataSource dataSource, ObjectMapper objectMapper, int defaultLeaseSeconds, int defaultMaxAttempts) {
        JdbcTransactionRunner transactions = new JdbcTransactionRunner(dataSource);
        PostgresQueueRepository repository = new PostgresQueueRepository(transactions);
        return new DefaultQueueService(repository, transactions, new RetryPolicy(), objectMapper, Clock.systemUTC(), defaultLeaseSeconds, defaultMaxAttempts);
    }
}
