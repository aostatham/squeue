package com.sequencedqueue.core;

import java.util.UUID;

public class QueueException extends RuntimeException {
    public static final int BAD_REQUEST = 400;
    public static final int NOT_FOUND = 404;
    public static final int CONFLICT = 409;
    public static final int INTERNAL_SERVER_ERROR = 500;

    private final int statusCode;
    private final QueueErrorCode errorCode;
    private String queueName;
    private String sourceId;
    private UUID itemId;

    public QueueException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
        this.errorCode = defaultErrorCode(statusCode);
    }

    public QueueException(int statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.errorCode = defaultErrorCode(statusCode);
    }

    public QueueException(QueueErrorCode errorCode, int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
    }

    public QueueException(QueueErrorCode errorCode, int statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
    }

    public int statusCode() {
        return statusCode;
    }

    public QueueErrorCode errorCode() {
        return errorCode;
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

    private static QueueErrorCode defaultErrorCode(int statusCode) {
        return switch (statusCode) {
            case BAD_REQUEST -> QueueErrorCode.VALIDATION_ERROR;
            case NOT_FOUND -> QueueErrorCode.ITEM_NOT_FOUND;
            case CONFLICT -> QueueErrorCode.QUEUE_CONFLICT;
            default -> QueueErrorCode.INTERNAL_ERROR;
        };
    }
}
