package com.sequencedqueue.postgres;

public class QueueConflictException extends QueueException {
    public QueueConflictException(String message) {
        super(message);
    }

    public QueueConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
