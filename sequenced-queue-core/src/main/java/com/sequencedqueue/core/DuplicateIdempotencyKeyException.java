package com.sequencedqueue.core;

public class DuplicateIdempotencyKeyException extends QueueException {
    public DuplicateIdempotencyKeyException(String message, Throwable cause) {
        super(QueueException.CONFLICT, message, cause);
    }
}
