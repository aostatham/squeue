package com.sequencedqueue.direct;

import java.time.OffsetDateTime;
import java.util.List;

public record RetentionPurgeRequest(OffsetDateTime olderThan, List<String> statuses, Boolean dryRun, String reason) {
}
