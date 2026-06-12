package com.sequencedqueue.direct;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public record ClaimResponse(UUID leaseId, String queueName, String sourceId, OffsetDateTime leaseUntil, List<ClaimItem> items) {
}
