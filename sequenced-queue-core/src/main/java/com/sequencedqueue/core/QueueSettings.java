package com.sequencedqueue.core;

public record QueueSettings(
    int defaultLeaseSeconds,
    int maxLeaseSeconds,
    int defaultMaxAttempts,
    int maxPayloadBytes,
    int maxHeadersBytes,
    int maxErrorMessageBytes,
    int maxAdminReasonBytes,
    int maxRetentionPurgeBatchSize
) {
    public static QueueSettings defaults() {
        return new QueueSettings(60, 600, 5, 262_144, 65_536, 8_192, 2_048, 10_000);
    }

    public QueueSettings {
        if (defaultLeaseSeconds <= 0) {
            throw new IllegalArgumentException("defaultLeaseSeconds must be > 0");
        }
        if (maxLeaseSeconds <= 0) {
            throw new IllegalArgumentException("maxLeaseSeconds must be > 0");
        }
        if (defaultLeaseSeconds > maxLeaseSeconds) {
            throw new IllegalArgumentException("defaultLeaseSeconds must be <= maxLeaseSeconds");
        }
        if (defaultMaxAttempts <= 0) {
            throw new IllegalArgumentException("defaultMaxAttempts must be > 0");
        }
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be > 0");
        }
        if (maxHeadersBytes <= 0) {
            throw new IllegalArgumentException("maxHeadersBytes must be > 0");
        }
        if (maxErrorMessageBytes <= 0) {
            throw new IllegalArgumentException("maxErrorMessageBytes must be > 0");
        }
        if (maxAdminReasonBytes <= 0) {
            throw new IllegalArgumentException("maxAdminReasonBytes must be > 0");
        }
        if (maxRetentionPurgeBatchSize <= 0) {
            throw new IllegalArgumentException("maxRetentionPurgeBatchSize must be > 0");
        }
    }
}
