package com.example.sequencedqueue.server.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import com.sequencedqueue.core.RetryPolicy;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {
    private final RetryPolicy policy = new RetryPolicy();

    @Test
    void usesConfiguredBackoffSteps() {
        assertEquals(Duration.ofSeconds(10), policy.nextBackoff(1));
        assertEquals(Duration.ofSeconds(30), policy.nextBackoff(2));
        assertEquals(Duration.ofMinutes(2), policy.nextBackoff(3));
        assertEquals(Duration.ofMinutes(10), policy.nextBackoff(4));
    }
}
