package com.sequencedqueue.direct;

import javax.sql.DataSource;

public interface DirectWorkerWaitStrategy {
    Waiter open(DataSource dataSource);

    static PostgresNotifyWorkerWaitStrategy.Builder postgresNotify() {
        return PostgresNotifyWorkerWaitStrategy.builder();
    }

    interface Waiter extends AutoCloseable {
        void waitForWork(String queueName);

        @Override
        void close();
    }
}
