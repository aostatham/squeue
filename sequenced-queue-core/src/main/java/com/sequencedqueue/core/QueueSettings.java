package com.sequencedqueue.core;

/**
 * Global queue runtime settings shared by server and direct Java construction paths.
 *
 * @param defaultLeaseSeconds lease duration used when a claim request omits one
 * @param maxLeaseSeconds maximum allowed claim or heartbeat lease duration
 * @param defaultMaxAttempts default maximum attempts for enqueued items
 * @param limits global queue field byte limits
 * @param maxRetentionPurgeBatchSize maximum rows a retention purge can inspect/delete
 */
public record QueueSettings(
    int defaultLeaseSeconds,
    int maxLeaseSeconds,
    int defaultMaxAttempts,
    QueueLimits limits,
    int maxRetentionPurgeBatchSize
) {
    /**
     * Creates the default settings used by tests, examples, and unconfigured server paths.
     *
     * @return default queue settings
     */
    public static QueueSettings defaults() {
        return new QueueSettings(60, 600, 5, QueueLimits.defaults(), 10_000);
    }

    /**
     * Creates settings from legacy flattened limits, preserving defaults for newer fields.
     */
    public QueueSettings(
        int defaultLeaseSeconds,
        int maxLeaseSeconds,
        int defaultMaxAttempts,
        int maxPayloadBytes,
        int maxHeadersBytes,
        int maxErrorMessageBytes,
        int maxAdminReasonBytes,
        int maxRetentionPurgeBatchSize
    ) {
        this(defaultLeaseSeconds, maxLeaseSeconds, defaultMaxAttempts, new QueueLimits(
            maxPayloadBytes,
            maxHeadersBytes,
            QueueLimits.defaults().maxResultBytes(),
            QueueLimits.defaults().maxErrorTypeBytes(),
            maxErrorMessageBytes,
            maxAdminReasonBytes,
            QueueLimits.defaults().maxAdminMetadataBytes()
        ), maxRetentionPurgeBatchSize);
    }

    /**
     * Creates settings from explicit flattened limits for all limited fields.
     */
    public QueueSettings(
        int defaultLeaseSeconds,
        int maxLeaseSeconds,
        int defaultMaxAttempts,
        int maxPayloadBytes,
        int maxHeadersBytes,
        int maxResultBytes,
        int maxErrorTypeBytes,
        int maxErrorMessageBytes,
        int maxAdminReasonBytes,
        int maxAdminMetadataBytes,
        int maxRetentionPurgeBatchSize
    ) {
        this(defaultLeaseSeconds, maxLeaseSeconds, defaultMaxAttempts, new QueueLimits(
            maxPayloadBytes,
            maxHeadersBytes,
            maxResultBytes,
            maxErrorTypeBytes,
            maxErrorMessageBytes,
            maxAdminReasonBytes,
            maxAdminMetadataBytes
        ), maxRetentionPurgeBatchSize);
    }

    /**
     * Validates settings at construction so invalid configuration fails fast.
     */
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
        if (limits == null) {
            throw new IllegalArgumentException("limits is required");
        }
        if (maxRetentionPurgeBatchSize <= 0) {
            throw new IllegalArgumentException("maxRetentionPurgeBatchSize must be > 0");
        }
    }

    /**
     * Returns the maximum serialized payload bytes.
     */
    public int maxPayloadBytes() {
        return limits.maxPayloadBytes();
    }

    /**
     * Returns the maximum serialized headers bytes.
     */
    public int maxHeadersBytes() {
        return limits.maxHeadersBytes();
    }

    /**
     * Returns the maximum serialized result bytes.
     */
    public int maxResultBytes() {
        return limits.maxResultBytes();
    }

    /**
     * Returns the maximum failure error type bytes.
     */
    public int maxErrorTypeBytes() {
        return limits.maxErrorTypeBytes();
    }

    /**
     * Returns the maximum failure error message bytes.
     */
    public int maxErrorMessageBytes() {
        return limits.maxErrorMessageBytes();
    }

    /**
     * Returns the maximum admin reason bytes.
     */
    public int maxAdminReasonBytes() {
        return limits.maxAdminReasonBytes();
    }

    /**
     * Returns the maximum serialized admin metadata bytes.
     */
    public int maxAdminMetadataBytes() {
        return limits.maxAdminMetadataBytes();
    }
}
