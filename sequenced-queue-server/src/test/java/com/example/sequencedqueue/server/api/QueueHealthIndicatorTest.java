package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sequencedqueue.core.QueueOperations;
import com.sequencedqueue.core.QueueSchemaInfo;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;

class QueueHealthIndicatorTest {
    @Test
    void healthIsUpWhenSchemaIsCurrent() throws Exception {
        QueueHealthIndicator indicator = indicator(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION);

        var health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, health.getDetails().get("schemaVersion"));
        assertEquals(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, health.getDetails().get("requiredSchemaVersion"));
        assertEquals(true, health.getDetails().get("schemaCurrent"));
    }

    @Test
    void healthIsOutOfServiceWhenSchemaIsStale() throws Exception {
        QueueHealthIndicator indicator = indicator("0");

        var health = indicator.health();

        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertEquals("0", health.getDetails().get("schemaVersion"));
        assertEquals(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, health.getDetails().get("requiredSchemaVersion"));
        assertEquals(false, health.getDetails().get("schemaCurrent"));
    }

    @Test
    void healthIsOutOfServiceWhenQueueItemTableIsMissing() throws Exception {
        QueueHealthIndicator indicator = indicator(new QueueSchemaInfo(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, false, true, true), true);

        var health = indicator.health();

        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertEquals(false, health.getDetails().get("queueItemTablePresent"));
    }

    @Test
    void healthIsOutOfServiceWhenAdminAuditTableIsMissing() throws Exception {
        QueueHealthIndicator indicator = indicator(new QueueSchemaInfo(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, true, true, false), true);

        var health = indicator.health();

        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertEquals(false, health.getDetails().get("adminAuditTablePresent"));
    }

    @Test
    void healthIsOutOfServiceWhenSchemaVersionIsMissing() throws Exception {
        QueueHealthIndicator indicator = indicator(new QueueSchemaInfo(null, true, true, true), true);

        var health = indicator.health();

        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertEquals(null, health.getDetails().get("schemaVersion"));
        assertEquals(false, health.getDetails().get("schemaCurrent"));
    }

    @Test
    void healthReportsRecoveryDisabledAsOutOfService() throws Exception {
        QueueHealthIndicator indicator = indicator(new QueueSchemaInfo(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, true, true, true), false);

        var health = indicator.health();

        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertEquals(false, health.getDetails().get("recoveryEnabled"));
    }

    private static QueueHealthIndicator indicator(String schemaVersion) throws Exception {
        return indicator(new QueueSchemaInfo(schemaVersion, true, true, true), true);
    }

    private static QueueHealthIndicator indicator(QueueSchemaInfo schemaInfo, boolean recoveryEnabled) throws Exception {
        QueueOperations operations = mock(QueueOperations.class);

        when(operations.getSchemaInfo()).thenReturn(schemaInfo);

        return new QueueHealthIndicator(operations, recoveryEnabled);
    }
}
