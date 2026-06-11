package com.sequencedqueue.direct;

import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sequencedqueue.core.QueueCoreFactory;
import com.sequencedqueue.core.QueueDtos;
import com.sequencedqueue.core.QueueOperations;

public class SequencedQueueDirectClient {
    private static final String DEFAULT_JSON = "{}";
    private static final int DEFAULT_MAX_ATTEMPTS = 5;
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

    public static final class Builder {
        private DataSource dataSource;
        private String defaultQueueName;
        private ObjectMapper objectMapper;

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

        public SequencedQueueDirectClient build() {
            return new SequencedQueueDirectClient(this);
        }
    }
}
