package com.example.sequencedqueue.server.core;

import java.time.Duration;

public class RetryPolicy {
    public Duration nextBackoff(int attemptCount) {
        return switch (attemptCount) {
            case 0, 1 -> Duration.ofSeconds(10);
            case 2 -> Duration.ofSeconds(30);
            case 3 -> Duration.ofMinutes(2);
            default -> Duration.ofMinutes(10);
        };
    }
}
