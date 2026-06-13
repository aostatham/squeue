package com.sequencedqueue.direct;

import java.util.Map;

public record DirectQueueResult(
    boolean succeeded,
    boolean retryable,
    String errorType,
    String errorMessage,
    Map<String, Object> result
) {
    public static DirectQueueResult success(Map<String, Object> result) {
        return new DirectQueueResult(true, false, null, null, result == null ? Map.of() : result);
    }

    public static DirectQueueResult failure(String errorType, String errorMessage) {
        return new DirectQueueResult(false, false, errorType, errorMessage, Map.of());
    }

    public static DirectQueueResult retryableFailure(String errorType, String errorMessage) {
        return new DirectQueueResult(false, true, errorType, errorMessage, Map.of());
    }
}
