package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class QueueLoggingSafetyTest {
    @Test
    void explicitQueueLogsDoNotReferenceUnsafeFields() throws Exception {
        Path source = repoRoot().resolve("sequenced-queue-server/src/main/java/com/example/sequencedqueue/server/api/QueueFacade.java");
        for (String line : Files.readAllLines(source)) {
            if (!line.contains("LOG.")) {
                continue;
            }
            String normalized = line.toLowerCase();
            assertFalse(normalized.contains("payload"), line);
            assertFalse(normalized.contains("headers"), line);
            assertFalse(normalized.contains("apikey"), line);
            assertFalse(normalized.contains("api-key"), line);
            assertFalse(normalized.contains("idempotency"), line);
            assertFalse(normalized.contains("sql"), line);
        }
    }

    private static Path repoRoot() {
        Path current = Path.of("").toAbsolutePath();
        return current.getFileName().toString().equals("sequenced-queue-server") ? current.getParent() : current;
    }
}
