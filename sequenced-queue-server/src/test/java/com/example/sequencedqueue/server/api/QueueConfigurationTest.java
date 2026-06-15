package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * Test coverage for QueueConfigurationTest.
 */
class QueueConfigurationTest {
    /**
     * Verifies queue operations rejects invalid global settings at creation.
     */
    @Test
    void queueOperationsRejectsInvalidGlobalSettingsAtCreation() {
        QueueConfiguration configuration = new QueueConfiguration();
        DataSource dataSource = mock(DataSource.class);
        ObjectMapper objectMapper = new ObjectMapper();

        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 0, 600, 5, 100, 100, 100, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 0, 5, 100, 100, 100, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 601, 600, 5, 100, 100, 100, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 0, 100, 100, 100, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 0, 100, 100, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 0, 100, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 100, 0, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 100, 100, 0, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 100, 100, 100, 0, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 100, 100, 100, 100, 0, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 100, 100, 100, 100, 100, 0, 100));
        assertThrows(IllegalArgumentException.class, () -> queueOperations(configuration, dataSource, objectMapper, 60, 600, 5, 100, 100, 100, 100, 100, 100, 100, 0));
    }

    private static void queueOperations(QueueConfiguration configuration, DataSource dataSource, ObjectMapper objectMapper, int defaultLeaseSeconds, int maxLeaseSeconds, int defaultMaxAttempts, int maxPayloadBytes, int maxHeadersBytes, int maxResultBytes, int maxErrorTypeBytes, int maxErrorMessageBytes, int maxAdminReasonBytes, int maxAdminMetadataBytes, int maxRetentionPurgeBatchSize) {
        configuration.queueOperations(dataSource, objectMapper, defaultLeaseSeconds, maxLeaseSeconds, defaultMaxAttempts, maxPayloadBytes, maxHeadersBytes, maxResultBytes, maxErrorTypeBytes, maxErrorMessageBytes, maxAdminReasonBytes, maxAdminMetadataBytes, maxRetentionPurgeBatchSize);
    }
}
