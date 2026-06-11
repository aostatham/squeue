package com.sequencedqueue.postgres;

public class DuplicateIdempotencyKeyException extends QueueException {
    public DuplicateIdempotencyKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
