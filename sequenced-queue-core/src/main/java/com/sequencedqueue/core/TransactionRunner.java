package com.sequencedqueue.core;

public interface TransactionRunner {
    <T> T inTransaction(TransactionCallback<T> callback);

    default void inTransaction(Runnable runnable) {
        inTransaction(() -> {
            runnable.run();
            return null;
        });
    }

    @FunctionalInterface
    interface TransactionCallback<T> {
        T execute();
    }
}
