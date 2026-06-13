package com.sequencedqueue.core;

import java.util.UUID;

public class QueueException extends RuntimeException {
    public static final int BAD_REQUEST = 400;
    public static final int NOT_FOUND = 404;
    public static final int CONFLICT = 409;
    public static final int INTERNAL_SERVER_ERROR = 500;

    private final int statusCode;
    private String queueName;
    private String sourceId;
    private UUID itemId;

    public QueueException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public QueueException(int statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }

    public String queueName() {
        return queueName;
    }

    public String sourceId() {
        return sourceId;
    }

    public UUID itemId() {
        return itemId;
    }

    public QueueException withContext(String queueName, String sourceId, UUID itemId) {
        if (this.queueName == null) {
            this.queueName = queueName;
        }
        if (this.sourceId == null) {
            this.sourceId = sourceId;
        }
        if (this.itemId == null) {
            this.itemId = itemId;
        }
        return this;
    }

    public QueueException withQueueName(String queueName) {
        return withContext(queueName, null, null);
    }
}
