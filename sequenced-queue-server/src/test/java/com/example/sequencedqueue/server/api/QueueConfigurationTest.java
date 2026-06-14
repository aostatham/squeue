package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class QueueConfigurationTest {
    @Test
    void queueOperationsRejectsInvalidGlobalSettingsAtCreation() {
        QueueConfiguration configuration = new QueueConfiguration();
        DataSource dataSource = mock(DataSource.class);
        ObjectMapper objectMapper = new ObjectMapper();

        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 0, 600, 5, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 60, 0, 5, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 601, 600, 5, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 60, 600, 0, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 60, 600, 5, 0, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 60, 600, 5, 100, 0, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 60, 600, 5, 100, 100, 0, 100));
        assertThrows(IllegalArgumentException.class, () -> configuration.queueOperations(dataSource, objectMapper, 60, 600, 5, 100, 100, 100, 0));
    }
}
