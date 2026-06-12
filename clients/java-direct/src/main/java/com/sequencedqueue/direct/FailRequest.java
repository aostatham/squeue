package com.sequencedqueue.direct;

import java.util.UUID;

public record FailRequest(String workerId, UUID leaseId, boolean retryable, String errorType, String errorMessage, Integer backoffSeconds) {
}
