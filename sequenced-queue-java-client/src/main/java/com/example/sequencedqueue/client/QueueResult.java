package com.example.sequencedqueue.client;

import java.util.Map;

public record QueueResult(boolean retryable, Map<String, Object> result, String errorType, String errorMessage) {
    public static QueueResult success(Map<String, Object> result) {
        return new QueueResult(false, result == null ? Map.of() : result, null, null);
    }

    public static QueueResult retryableFailure(String errorType, String errorMessage) {
        return new QueueResult(true, Map.of(), errorType, errorMessage);
    }

    public static QueueResult failure(String errorType, String errorMessage) {
        return new QueueResult(false, Map.of(), errorType, errorMessage);
    }

    public boolean succeeded() {
        return errorType == null && errorMessage == null;
    }
}
