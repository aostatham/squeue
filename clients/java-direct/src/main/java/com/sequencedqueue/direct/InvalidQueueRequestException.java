package com.sequencedqueue.direct;

/**
 * Direct-client exception raised when a request is invalid before or during core validation.
 */
public class InvalidQueueRequestException extends QueueException {
    /**
     * Creates an invalid request exception.
     */
    public InvalidQueueRequestException(String message) {
        super(message);
    }

    /**
     * Creates an invalid request exception with a cause.
     */
    public InvalidQueueRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
