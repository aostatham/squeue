package com.sequencedqueue.core;

public class DuplicateIdempotencyKeyException extends QueueException {
    public DuplicateIdempotencyKeyException(String message, Throwable cause) {
        super(QueueErrorCode.IDEMPOTENCY_CONFLICT, QueueException.CONFLICT, message, cause);
    }
}
