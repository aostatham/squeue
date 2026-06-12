package com.sequencedqueue.direct;

import java.util.Map;
import java.util.UUID;

public record ClaimItem(UUID itemId, long sequenceNo, String itemType, Map<String, Object> payload, Map<String, Object> headers) {
}
