package com.sequencedqueue.core;

/**
 * Core validation exception raised when a field exceeds its configured byte limit.
 */
public class QueueFieldTooLargeException extends QueueException {
    /** Stable field identifier such as payload, headers, result, or errorType. */
    private final String fieldName;
    /** Configured maximum UTF-8 byte length. */
    private final int maxBytes;
    /** Measured UTF-8 byte length. */
    private final int actualBytes;

    /**
     * Creates a structured oversized-field exception without echoing field content.
     *
     * @param fieldName stable field identifier
     * @param maxBytes configured maximum bytes
     * @param actualBytes measured actual bytes
     */
    public QueueFieldTooLargeException(String fieldName, int maxBytes, int actualBytes) {
        super(QueueErrorCode.FIELD_TOO_LARGE, QueueException.BAD_REQUEST, fieldName + " exceeds configured size limit");
        this.fieldName = fieldName;
        this.maxBytes = maxBytes;
        this.actualBytes = actualBytes;
    }

    /**
     * Returns the stable field identifier.
     *
     * @return field name
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Returns the configured byte limit.
     *
     * @return maximum bytes
     */
    public int maxBytes() {
        return maxBytes;
    }

    /**
     * Returns the measured byte length.
     *
     * @return actual bytes
     */
    public int actualBytes() {
        return actualBytes;
    }
}
