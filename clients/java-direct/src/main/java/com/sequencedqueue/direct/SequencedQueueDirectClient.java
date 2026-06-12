package com.sequencedqueue.direct;

import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sequencedqueue.core.QueueCoreFactory;
import com.sequencedqueue.core.QueueDtos;
import com.sequencedqueue.core.QueueOperations;

public class SequencedQueueDirectClient {
    private static final String DEFAULT_JSON = "{}";
    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final String SUPPORTED_SCHEMA_VERSION = "1";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final String defaultQueueName;
    private final ObjectMapper objectMapper;
    private final QueueOperations queueOperations;

    private SequencedQueueDirectClient(Builder builder) {
        DataSource dataSource = Objects.requireNonNull(builder.dataSource, "dataSource is required");
        this.defaultQueueName = builder.defaultQueueName;
        this.objectMapper = builder.objectMapper == null ? new ObjectMapper() : builder.objectMapper;
        this.queueOperations = QueueCoreFactory.create(dataSource, this.objectMapper, 60, DEFAULT_MAX_ATTEMPTS);
        if (builder.validateSchemaOnBuild) {
            validateSchemaCompatibility();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public EnqueueResponse enqueue(EnqueueRequest request) {
        if (defaultQueueName == null || defaultQueueName.isBlank()) {
            throw new InvalidQueueRequestException("queueName is required because no defaultQueueName was configured");
        }
        return enqueue(defaultQueueName, request);
    }

    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        try {
            QueueDtos.EnqueueResponse response = queueOperations.enqueue(queueName, toCoreRequest(request));
            return new EnqueueResponse(response.itemId(), response.queueName(), response.sourceId(), response.sequenceNo(), response.status());
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public QueueSchemaInfo getSchemaInfo() {
        try {
            com.sequencedqueue.core.QueueSchemaInfo schemaInfo = queueOperations.getSchemaInfo();
            return new QueueSchemaInfo(schemaInfo.schemaVersion());
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    private void validateSchemaCompatibility() {
        QueueSchemaInfo schemaInfo = getSchemaInfo();
        if (!SUPPORTED_SCHEMA_VERSION.equals(schemaInfo.schemaVersion())) {
            throw new QueueUnavailableException("unsupported queue schema version: " + schemaInfo.schemaVersion(), null);
        }
    }

    public ClaimResponse claim(String queueName, ClaimRequest request) {
        try {
            return toDirect(queueOperations.claim(queueName, new QueueDtos.ClaimRequest(request.workerId(), request.supportedItemTypes(), request.leaseSeconds(), request.maxItems())));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        try {
            return toDirect(queueOperations.complete(queueName, itemId, new QueueDtos.CompleteRequest(request.workerId(), request.leaseId(), request.result())));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        try {
            return toDirect(queueOperations.fail(queueName, itemId, new QueueDtos.FailRequest(request.workerId(), request.leaseId(), request.retryable(), request.errorType(), request.errorMessage(), request.backoffSeconds())));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        try {
            queueOperations.heartbeat(queueName, leaseId, new QueueDtos.HeartbeatRequest(request.workerId(), request.extendBySeconds()));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public ItemResponse retry(String queueName, UUID itemId) {
        try {
            return toDirect(queueOperations.retry(queueName, itemId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public ItemResponse skip(String queueName, UUID itemId) {
        try {
            return toDirect(queueOperations.skip(queueName, itemId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public ItemResponse cancel(String queueName, UUID itemId) {
        try {
            return toDirect(queueOperations.cancel(queueName, itemId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public List<SourceResponse> blockedSources(String queueName) {
        try {
            return queueOperations.blockedSources(queueName).stream().map(this::toDirect).toList();
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public SourceResponse unblockSource(String queueName, String sourceId) {
        try {
            return toDirect(queueOperations.unblockSource(queueName, sourceId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    public void recoverExpiredLeases() {
        try {
            queueOperations.recoverExpiredLeases();
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    private QueueDtos.EnqueueRequest toCoreRequest(EnqueueRequest request) {
        validateRequest(request);
        return new QueueDtos.EnqueueRequest(
            request.sourceId(),
            request.itemType(),
            request.idempotencyKey(),
            readJson(request.payloadJson()),
            readJson(request.headersJson()),
            request.availableAt(),
            request.maxAttempts()
        );
    }

    private void validateRequest(EnqueueRequest request) {
        if (request == null) {
            throw new InvalidQueueRequestException("request is required");
        }
        if (request.maxAttempts() != null && request.maxAttempts() < 1) {
            throw new InvalidQueueRequestException("maxAttempts must be >= 1");
        }
    }

    private Map<String, Object> readJson(String json) {
        String normalized = json == null || json.isBlank() ? DEFAULT_JSON : json;
        try {
            return objectMapper.readValue(normalized, MAP_TYPE);
        } catch (Exception e) {
            throw new InvalidQueueRequestException("json value is not valid");
        }
    }

    private QueueException mapCoreException(com.sequencedqueue.core.QueueException e) {
        if (e.statusCode() == com.sequencedqueue.core.QueueException.BAD_REQUEST) {
            return new InvalidQueueRequestException(e.getMessage());
        }
        if (e.statusCode() == com.sequencedqueue.core.QueueException.CONFLICT) {
            return new QueueConflictException(e.getMessage(), e);
        }
        if (e.statusCode() == com.sequencedqueue.core.QueueException.NOT_FOUND) {
            return new ItemNotFoundException(e.getMessage());
        }
        return new QueueUnavailableException(e.getMessage(), e);
    }

    private ClaimResponse toDirect(QueueDtos.ClaimResponse response) {
        return new ClaimResponse(response.leaseId(), response.queueName(), response.sourceId(), response.leaseUntil(), response.items().stream()
            .map(item -> new ClaimItem(item.itemId(), item.sequenceNo(), item.itemType(), item.payload(), item.headers()))
            .toList());
    }

    private ItemResponse toDirect(QueueDtos.ItemResponse item) {
        return new ItemResponse(item.itemId(), item.queueName(), item.sourceId(), item.sequenceNo(), item.itemType(), item.payload(), item.headers(), item.status(), item.availableAt(), item.claimedBy(), item.leaseId(), item.leaseUntil(), item.attemptCount(), item.maxAttempts(), item.idempotencyKey(), item.lastErrorType(), item.lastErrorMessage(), item.result(), item.createdAt(), item.updatedAt());
    }

    private SourceResponse toDirect(QueueDtos.SourceResponse source) {
        return new SourceResponse(source.queueName(), source.sourceId(), source.nextSequenceNo(), source.status(), source.leasedBy(), source.leaseId(), source.leaseUntil(), source.updatedAt());
    }

    public static final class Builder {
        private DataSource dataSource;
        private String defaultQueueName;
        private ObjectMapper objectMapper;
        private boolean validateSchemaOnBuild;

        public Builder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder defaultQueueName(String defaultQueueName) {
            this.defaultQueueName = defaultQueueName;
            return this;
        }

        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public Builder validateSchemaOnBuild(boolean validateSchemaOnBuild) {
            this.validateSchemaOnBuild = validateSchemaOnBuild;
            return this;
        }

        public SequencedQueueDirectClient build() {
            return new SequencedQueueDirectClient(this);
        }
    }
}
