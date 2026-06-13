package com.example.sequencedqueue.server.api;

import java.util.LinkedHashMap;
import java.util.Map;

import com.sequencedqueue.core.QueueOperations;
import com.sequencedqueue.core.QueueSchemaInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("queue")
public class QueueHealthIndicator implements HealthIndicator {
    private final QueueOperations queueOperations;
    private final boolean recoveryEnabled;

    public QueueHealthIndicator(
        QueueOperations queueOperations,
        @Value("${sequenced-queue.recovery-enabled:true}") boolean recoveryEnabled
    ) {
        this.queueOperations = queueOperations;
        this.recoveryEnabled = recoveryEnabled;
    }

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        try {
            QueueSchemaInfo schemaInfo = queueOperations.getSchemaInfo();
            details.put("databaseReachable", true);
            details.put("queueItemTablePresent", schemaInfo.queueItemTablePresent());
            details.put("queueSourceStateTablePresent", schemaInfo.queueSourceStateTablePresent());
            details.put("adminAuditTablePresent", schemaInfo.adminAuditTablePresent());
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
}
