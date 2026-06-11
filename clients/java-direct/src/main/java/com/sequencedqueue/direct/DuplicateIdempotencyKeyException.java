package com.sequencedqueue.direct;

public class DuplicateIdempotencyKeyException extends QueueException {
    public DuplicateIdempotencyKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
