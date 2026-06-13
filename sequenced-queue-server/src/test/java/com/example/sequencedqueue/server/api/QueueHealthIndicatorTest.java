package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import com.sequencedqueue.core.QueueOperations;
import com.sequencedqueue.core.QueueSchemaInfo;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;

class QueueHealthIndicatorTest {
    @Test
    void healthIsUpWhenSchemaIsCurrent() throws Exception {
        QueueHealthIndicator indicator = indicator("2");

        var health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("2", health.getDetails().get("schemaVersion"));
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
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        QueueOperations operations = mock(QueueOperations.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.isValid(2)).thenReturn(true);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true);
        when(operations.getSchemaInfo()).thenReturn(new QueueSchemaInfo(schemaVersion));

        return new QueueHealthIndicator(dataSource, operations, true);
    }
}
