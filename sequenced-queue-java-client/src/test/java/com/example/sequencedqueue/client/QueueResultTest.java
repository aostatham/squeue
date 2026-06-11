package com.example.sequencedqueue.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class QueueResultTest {
    @Test
    void successHasNoFailureFields() {
        assertTrue(QueueResult.success(Map.of("ok", true)).succeeded());
    }

    @Test
    void failureIsNotSuccessful() {
        assertFalse(QueueResult.retryableFailure("TIMEOUT", "try again").succeeded());
    }
}
