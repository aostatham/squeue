package com.sequencedqueue.postgres;

public class QueueUnavailableException extends QueueException {
    public QueueUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
