package com.sequencedqueue.direct;

import java.util.Map;
import java.util.UUID;

public record CompleteRequest(String workerId, UUID leaseId, Map<String, Object> result) {
}
