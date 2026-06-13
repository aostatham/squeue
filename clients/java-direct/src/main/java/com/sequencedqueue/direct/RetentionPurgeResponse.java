package com.sequencedqueue.direct;

public record RetentionPurgeResponse(String queueName, boolean dryRun, long matched, long deleted) {
}
