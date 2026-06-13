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
                .filter(ProductionSqlLocationTest::containsSqlToken)
                .toList();
        }

        assertTrue(offenders.isEmpty(), "production SQL must stay in sequenced-queue-core: " + offenders);
    }

    private static boolean containsSqlToken(Path path) {
        try {
            String text = Files.readString(path).toUpperCase();
            return SQL_TOKENS.stream().anyMatch(text::contains);
        } catch (Exception e) {
            throw new IllegalStateException("could not read " + path, e);
        }
    }

    private static Path repoRoot() {
        Path current = Path.of("").toAbsolutePath();
        return current.getFileName().toString().equals("sequenced-queue-server") ? current.getParent() : current;
    }
}
