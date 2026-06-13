package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.SQLException;

import com.sequencedqueue.core.QueueException;
import org.junit.jupiter.api.Test;

class ApiExceptionHandlerTest {
    @Test
    void internalQueueExceptionDoesNotLeakDatabaseDetails() {
        ApiExceptionHandler handler = new ApiExceptionHandler();

        var response = handler.queueException(new QueueException(QueueException.INTERNAL_SERVER_ERROR, "database query failed", new SQLException("syntax error near secret_table")));

        assertEquals(500, response.getStatusCode().value());
        assertEquals("INTERNAL_ERROR", response.getBody().get("errorCode"));
        assertEquals("internal server error", response.getBody().get("message"));
        assertFalse(response.getBody().toString().contains("secret_table"));
        assertFalse(response.getBody().toString().contains("database query failed"));
    }
}
