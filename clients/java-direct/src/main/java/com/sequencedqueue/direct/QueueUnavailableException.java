package com.sequencedqueue.direct;

public class QueueUnavailableException extends QueueException {
    public QueueUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
