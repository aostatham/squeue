package com.sequencedqueue.core;

public record QueueSchemaInfo(String schemaVersion, boolean queueItemTablePresent, boolean queueSourceStateTablePresent, boolean adminAuditTablePresent) {
    public static final String REQUIRED_SCHEMA_VERSION = "4";

    public QueueSchemaInfo(String schemaVersion) {
        this(schemaVersion, false, false, false);
    }

    public boolean isCurrent() {
        return REQUIRED_SCHEMA_VERSION.equals(schemaVersion);
    }
}
