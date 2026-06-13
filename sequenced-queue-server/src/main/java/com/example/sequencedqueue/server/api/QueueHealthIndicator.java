package com.example.sequencedqueue.server.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.sequencedqueue.core.QueueOperations;
import com.sequencedqueue.core.QueueSchemaInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("queue")
public class QueueHealthIndicator implements HealthIndicator {
    private final DataSource dataSource;
    private final QueueOperations queueOperations;
    private final boolean recoveryEnabled;

    public QueueHealthIndicator(
        DataSource dataSource,
        QueueOperations queueOperations,
        @Value("${sequenced-queue.recovery-enabled:true}") boolean recoveryEnabled
    ) {
        this.dataSource = dataSource;
        this.queueOperations = queueOperations;
        this.recoveryEnabled = recoveryEnabled;
    }

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        try (Connection connection = dataSource.getConnection()) {
            details.put("databaseReachable", connection.isValid(2));
            details.put("queueItemTablePresent", tableExists(connection, "queue_item"));
            details.put("queueSourceStateTablePresent", tableExists(connection, "queue_source_state"));
            details.put("adminAuditTablePresent", tableExists(connection, "queue_admin_audit"));
            QueueSchemaInfo schemaInfo = queueOperations.getSchemaInfo();
            details.put("schemaVersion", schemaInfo.schemaVersion());
            details.put("requiredSchemaVersion", QueueSchemaInfo.REQUIRED_SCHEMA_VERSION);
            details.put("schemaCurrent", schemaInfo.isCurrent());
            details.put("recoveryEnabled", recoveryEnabled);
            boolean ready = Boolean.TRUE.equals(details.get("databaseReachable"))
                && Boolean.TRUE.equals(details.get("queueItemTablePresent"))
                && Boolean.TRUE.equals(details.get("queueSourceStateTablePresent"))
                && Boolean.TRUE.equals(details.get("adminAuditTablePresent"))
                && Boolean.TRUE.equals(details.get("schemaCurrent"))
                && recoveryEnabled;
            return ready ? Health.up().withDetails(details).build() : Health.outOfService().withDetails(details).build();
        } catch (Exception e) {
            details.put("error", "queue health check failed");
            return Health.down().withDetails(details).build();
        }
    }

    private static boolean tableExists(Connection connection, String tableName) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = ?
            """)) {
            statement.setString(1, tableName);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }
}
