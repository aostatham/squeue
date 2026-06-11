package com.sequencedqueue.core;

public enum ItemStatus {
    pending,
    processing,
    succeeded,
    retry_wait,
    failed,
    dead_lettered,
    cancelled,
    skipped
}
