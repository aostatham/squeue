package com.sequencedqueue.direct;

import java.util.UUID;

/**
 * Direct-client exception raised when a request field exceeds its configured byte limit.
 */
public class QueueFieldTooLargeException extends InvalidQueueRequestException {
    /** Stable field identifier such as payload, headers, result, or errorType. */
    private final String fieldName;
    /** Configured maximum UTF-8 byte length. */
    private final int maxBytes;
    /** Measured UTF-8 byte length. */
    private final int actualBytes;
    /** Logical queue context, when known. */
    private final String queueName;
    /** Source context, when known. */
    private final String sourceId;
    /** Item context, when known. */
    private final UUID itemId;

    /**
     * Creates a structured direct-client oversized-field exception.
     */
    public QueueFieldTooLargeException(String message, String fieldName, int maxBytes, int actualBytes, String queueName, String sourceId, UUID itemId, Throwable cause) {
        super(message, cause);
        this.fieldName = fieldName;
        this.maxBytes = maxBytes;
        this.actualBytes = actualBytes;
        this.queueName = queueName;
        this.sourceId = sourceId;
        this.itemId = itemId;
    }

    /**
     * Returns the stable field identifier.
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Returns the configured byte limit.
     */
    public int maxBytes() {
        return maxBytes;
    }

    /**
     * Returns the measured byte length.
     */
    public int actualBytes() {
        return actualBytes;
    }

    /**
     * Returns the queue context, when known.
     */
    public String queueName() {
        return queueName;
    }

    /**
     * Returns the source context, when known.
     */
    public String sourceId() {
        return sourceId;
    }

    /**
     * Returns the item context, when known.
     */
    public UUID itemId() {
        return itemId;
    }
}
