package com.sequencedqueue.core;

/**
 * Global UTF-8 byte limits for queue-controlled request, result, and audit fields.
 *
 * @param maxPayloadBytes maximum bytes for serialized enqueue payload JSON
 * @param maxHeadersBytes maximum bytes for serialized enqueue headers JSON
 * @param maxResultBytes maximum bytes for serialized completion result JSON
 * @param maxErrorTypeBytes maximum bytes for failure error type text
 * @param maxErrorMessageBytes maximum bytes for failure error message text
 * @param maxAdminReasonBytes maximum bytes for admin audit reason text
 * @param maxAdminMetadataBytes maximum bytes for serialized admin audit metadata JSON
 */
public record QueueLimits(
    int maxPayloadBytes,
    int maxHeadersBytes,
    int maxResultBytes,
    int maxErrorTypeBytes,
    int maxErrorMessageBytes,
    int maxAdminReasonBytes,
    int maxAdminMetadataBytes
) {
    /**
     * Returns the default production-safety limits used by server and direct Java paths.
     *
     * @return default byte limits
     */
    public static QueueLimits defaults() {
        return new QueueLimits(262_144, 32_768, 262_144, 256, 8_192, 2_048, 32_768);
    }

    /**
     * Validates configured byte limits at construction time.
     */
    public QueueLimits {
        requirePositive(maxPayloadBytes, "maxPayloadBytes");
        requirePositive(maxHeadersBytes, "maxHeadersBytes");
        requirePositive(maxResultBytes, "maxResultBytes");
        requirePositive(maxErrorTypeBytes, "maxErrorTypeBytes");
        requirePositive(maxErrorMessageBytes, "maxErrorMessageBytes");
        requirePositive(maxAdminReasonBytes, "maxAdminReasonBytes");
        requirePositive(maxAdminMetadataBytes, "maxAdminMetadataBytes");
    }

    /**
     * Rejects zero or negative byte limits.
     */
    private static void requirePositive(int value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be > 0");
        }
    }
}
