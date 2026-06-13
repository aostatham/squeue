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
        QueueHealthIndicator indicator = indicator("3");

        var health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("3", health.getDetails().get("schemaVersion"));
        assertEquals(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, health.getDetails().get("requiredSchemaVersion"));
        assertEquals(true, health.getDetails().get("schemaCurrent"));
    }

    @Test
    void healthIsOutOfServiceWhenSchemaIsStale() throws Exception {
        QueueHealthIndicator indicator = indicator("1");

        var health = indicator.health();

        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertEquals("1", health.getDetails().get("schemaVersion"));
        assertEquals(QueueSchemaInfo.REQUIRED_SCHEMA_VERSION, health.getDetails().get("requiredSchemaVersion"));
        assertEquals(false, health.getDetails().get("schemaCurrent"));
    }

    private static QueueHealthIndicator indicator(String schemaVersion) throws Exception {
        QueueOperations operations = mock(QueueOperations.class);

        when(operations.getSchemaInfo()).thenReturn(new QueueSchemaInfo(schemaVersion, true, true, true));

        return new QueueHealthIndicator(operations, true);
    }
}
