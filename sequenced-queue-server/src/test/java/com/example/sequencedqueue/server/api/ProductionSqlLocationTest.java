package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

class ProductionSqlLocationTest {
    private static final List<String> SQL_TOKENS = List.of(
        "SELECT ",
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "CREATE TABLE",
        "ALTER TABLE",
        "FOR UPDATE"
    );
    private static final List<String> LISTENER_SQL_TOKENS = List.of(
        "LISTEN ",
        "UNLISTEN "
    );
    private static final String ALLOWED_DIRECT_NOTIFY_LISTENER = "/clients/java-direct/src/main/java/com/sequencedqueue/direct/PostgresNotifyWorkerWaitStrategy.java";

    @Test
    void productionSqlExistsOnlyInCoreModule() throws Exception {
        Path root = repoRoot();
        List<Path> offenders;
        try (var stream = Files.walk(root)) {
            offenders = stream
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().contains("/src/main/"))
                .filter(path -> !path.toString().contains("/sequenced-queue-core/"))
                .filter(path -> path.toString().endsWith(".java") || path.toString().endsWith(".sql") || path.toString().endsWith(".yaml") || path.toString().endsWith(".yml"))
                .filter(ProductionSqlLocationTest::containsDisallowedSql)
                .toList();
        }

        assertTrue(offenders.isEmpty(), "production queue SQL must stay in sequenced-queue-core; direct Java may only contain LISTEN/UNLISTEN in the notify wait strategy: " + offenders);
    }

    private static boolean containsDisallowedSql(Path path) {
        try {
            String text = Files.readString(path).toUpperCase();
            if (SQL_TOKENS.stream().anyMatch(text::contains)) {
                return true;
            }
            return LISTENER_SQL_TOKENS.stream().anyMatch(text::contains) && !isAllowedDirectNotifyListener(path);
        } catch (Exception e) {
            throw new IllegalStateException("could not read " + path, e);
        }
    }

    private static boolean isAllowedDirectNotifyListener(Path path) {
        return path.toString().replace('\\', '/').endsWith(ALLOWED_DIRECT_NOTIFY_LISTENER);
    }

    private static Path repoRoot() {
        Path current = Path.of("").toAbsolutePath();
        return current.getFileName().toString().equals("sequenced-queue-server") ? current.getParent() : current;
    }
}
