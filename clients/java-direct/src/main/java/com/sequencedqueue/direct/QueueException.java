package com.sequencedqueue.direct;

public class QueueException extends RuntimeException {
    public QueueException(String message) {
        super(message);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
