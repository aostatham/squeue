package com.sequencedqueue.core;

public record QueueSchemaInfo(String schemaVersion) {
    public static final String REQUIRED_SCHEMA_VERSION = "2";

    public boolean isCurrent() {
        return REQUIRED_SCHEMA_VERSION.equals(schemaVersion);
    }
}
