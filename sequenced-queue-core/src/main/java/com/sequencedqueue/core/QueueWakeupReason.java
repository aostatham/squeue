package com.sequencedqueue.core;

public enum QueueWakeupReason {
    ENQUEUE,
    ADMIN_RETRY,
    ADMIN_SKIP,
    ADMIN_CANCEL,
    ADMIN_UNBLOCK,
    RECOVERY
}
