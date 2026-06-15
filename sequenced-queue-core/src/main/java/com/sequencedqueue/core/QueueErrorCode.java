package com.sequencedqueue.core;

/**
 * Stable machine-readable error codes shared across REST and direct Java paths.
 */
public enum QueueErrorCode {
    /** Request validation failed before a queue state transition was attempted. */
    VALIDATION_ERROR,
    /** One request field exceeded its configured UTF-8 byte limit. */
    FIELD_TOO_LARGE,
    /** The requested item does not exist in the named queue. */
    ITEM_NOT_FOUND,
    /** The requested source does not exist in the named queue. */
    SOURCE_NOT_FOUND,
    /** The enqueue idempotency key already belongs to a different insert. */
    IDEMPOTENCY_CONFLICT,
    /** The source has a blocking head item and cannot progress normally. */
    SOURCE_BLOCKED,
    /** The worker no longer owns the item/source lease. */
    LEASE_LOST,
    /** The worker-owned lease exists but has expired. */
    LEASE_EXPIRED,
    /** The requested worker operation targeted an item that is not processing. */
    ITEM_NOT_PROCESSING,
    /** The requested operation conflicts with current queue state. */
    QUEUE_CONFLICT,
    /** An unexpected internal failure occurred. */
    INTERNAL_ERROR
}
