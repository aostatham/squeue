package com.sequencedqueue.core;

import java.time.OffsetDateTime;
import java.util.UUID;

public record AdminAuditRow(UUID auditId, OffsetDateTime occurredAt, String actorId, String operation, String queueName, String sourceId, UUID itemId, String previousStatus, String newStatus, String reason, String metadataJson) {
}
