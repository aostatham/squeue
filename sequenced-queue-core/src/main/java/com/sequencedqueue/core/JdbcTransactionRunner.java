package com.sequencedqueue.core;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class JdbcTransactionRunner implements TransactionRunner, SqlConnectionProvider {
    private final DataSource dataSource;
    private final ThreadLocal<Connection> currentConnection = new ThreadLocal<>();

    public JdbcTransactionRunner(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public <T> T inTransaction(TransactionCallback<T> callback) {
        if (currentConnection.get() != null) {
            return callback.execute();
        }
        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            currentConnection.set(connection);
            try {
                T result = callback.execute();
                connection.commit();
                return result;
            } catch (RuntimeException e) {
                rollbackQuietly(connection);
                throw e;
            } finally {
                currentConnection.remove();
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "database transaction failed", e);
        }
    }

    @Override
    public Connection currentConnection() {
        Connection connection = currentConnection.get();
        if (connection == null) {
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "no active transaction");
        }
        return connection;
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }
}
