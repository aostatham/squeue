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

/**
 * Trusted direct Java/PostgreSQL client backed by the shared core queue implementation.
 */
public class SequencedQueueDirectClient {
    /** Empty JSON object used for omitted direct JSON strings. */
    private static final String DEFAULT_JSON = "{}";
    /** Default maximum attempts used when constructing core settings. */
    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    /** Required schema baseline supported by this client build. */
    private static final String SUPPORTED_SCHEMA_VERSION = com.sequencedqueue.core.QueueSchemaInfo.REQUIRED_SCHEMA_VERSION;
    /** Jackson type token for direct JSON request parsing. */
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    /** Optional queue name used by single-argument enqueue. */
    private final String defaultQueueName;
    /** JSON mapper used to parse direct request JSON strings. */
    private final ObjectMapper objectMapper;
    /** Shared core implementation used for all queue semantics. */
    private final QueueOperations queueOperations;

    /**
     * Creates a direct client from builder settings.
     */
    private SequencedQueueDirectClient(Builder builder) {
        DataSource dataSource = Objects.requireNonNull(builder.dataSource, "dataSource is required");
        this.defaultQueueName = builder.defaultQueueName;
        this.objectMapper = builder.objectMapper == null ? new ObjectMapper() : builder.objectMapper;
        this.queueOperations = QueueCoreFactory.create(dataSource, this.objectMapper, 60, DEFAULT_MAX_ATTEMPTS);
        if (builder.validateSchemaOnBuild) {
            validateSchemaCompatibility();
        }
    }

    /**
     * Creates a direct client builder.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Enqueues using the configured default queue name.
     *
     * @param request enqueue request
     * @return enqueue response
     */
    public EnqueueResponse enqueue(EnqueueRequest request) {
        if (defaultQueueName == null || defaultQueueName.isBlank()) {
            throw new InvalidQueueRequestException("queueName is required because no defaultQueueName was configured");
        }
        return enqueue(defaultQueueName, request);
    }

    /**
     * Enqueues into an explicit queue.
     *
     * @param queueName logical queue name
     * @param request enqueue request
     * @return enqueue response
     */
    public EnqueueResponse enqueue(String queueName, EnqueueRequest request) {
        try {
            QueueDtos.EnqueueResponse response = queueOperations.enqueue(queueName, toCoreRequest(request));
            return new EnqueueResponse(response.itemId(), response.queueName(), response.sourceId(), response.sequenceNo(), response.status());
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Reads current schema compatibility information.
     *
     * @return schema information
     */
    public QueueSchemaInfo getSchemaInfo() {
        try {
            com.sequencedqueue.core.QueueSchemaInfo schemaInfo = queueOperations.getSchemaInfo();
            return new QueueSchemaInfo(schemaInfo.schemaVersion());
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Fails client construction when the database schema does not match this build.
     */
    private void validateSchemaCompatibility() {
        QueueSchemaInfo schemaInfo = getSchemaInfo();
        if (!SUPPORTED_SCHEMA_VERSION.equals(schemaInfo.schemaVersion())) {
            throw new QueueUnavailableException("unsupported queue schema version: " + schemaInfo.schemaVersion(), null);
        }
    }

    /**
     * Claims work from an explicit queue.
     */
    public ClaimResponse claim(String queueName, ClaimRequest request) {
        try {
            return toDirect(queueOperations.claim(queueName, new QueueDtos.ClaimRequest(request.workerId(), request.supportedItemTypes(), request.leaseSeconds(), request.maxItems())));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Completes a claimed item.
     */
    public ItemResponse complete(String queueName, UUID itemId, CompleteRequest request) {
        try {
            return toDirect(queueOperations.complete(queueName, itemId, new QueueDtos.CompleteRequest(request.workerId(), request.leaseId(), request.result())));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Fails a claimed item.
     */
    public ItemResponse fail(String queueName, UUID itemId, FailRequest request) {
        try {
            return toDirect(queueOperations.fail(queueName, itemId, new QueueDtos.FailRequest(request.workerId(), request.leaseId(), request.retryable(), request.errorType(), request.errorMessage(), request.backoffSeconds())));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Extends a worker lease.
     */
    public void heartbeat(String queueName, UUID leaseId, HeartbeatRequest request) {
        try {
            queueOperations.heartbeat(queueName, leaseId, new QueueDtos.HeartbeatRequest(request.workerId(), request.extendBySeconds()));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Retries a dead-lettered head item.
     */
    public ItemResponse retry(String queueName, UUID itemId) {
        try {
            return toDirect(queueOperations.retry(queueName, itemId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Skips a blocking head item.
     */
    public ItemResponse skip(String queueName, UUID itemId) {
        try {
            return toDirect(queueOperations.skip(queueName, itemId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Cancels an allowed head item.
     */
    public ItemResponse cancel(String queueName, UUID itemId) {
        try {
            return toDirect(queueOperations.cancel(queueName, itemId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Lists blocked sources.
     */
    public List<SourceResponse> blockedSources(String queueName) {
        try {
            return queueOperations.blockedSources(queueName).stream().map(this::toDirect).toList();
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Unblocks a source when no blocking head remains.
     */
    public SourceResponse unblockSource(String queueName, String sourceId) {
        try {
            return toDirect(queueOperations.unblockSource(queueName, sourceId));
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Runs manual retention purge.
     */
    public RetentionPurgeResponse purgeRetention(String queueName, RetentionPurgeRequest request) {
        try {
            QueueDtos.RetentionPurgeRequest coreRequest = request == null
                ? null
                : new QueueDtos.RetentionPurgeRequest(request.olderThan(), request.statuses(), request.dryRun(), request.reason(), request.limit());
            QueueDtos.RetentionPurgeResponse response = queueOperations.purgeRetention(queueName,
                coreRequest);
            return new RetentionPurgeResponse(response.queueName(), response.dryRun(), response.matched(), response.deleted());
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Runs lease recovery through the shared core service.
     */
    public void recoverExpiredLeases() {
        try {
            queueOperations.recoverExpiredLeases();
        } catch (com.sequencedqueue.core.QueueException e) {
            throw mapCoreException(e);
        }
    }

    /**
     * Creates a direct worker builder using this client.
     */
    public SequencedQueueDirectWorker.Builder worker(String queueName) {
        return SequencedQueueDirectWorker.builder(this, queueName);
    }

    /**
     * Converts direct enqueue JSON strings into core map DTOs.
     */
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

    /**
     * Performs direct-client request validation before core delegation.
     */
    private void validateRequest(EnqueueRequest request) {
        if (request == null) {
            throw new InvalidQueueRequestException("request is required");
        }
        if (request.maxAttempts() != null && request.maxAttempts() < 1) {
            throw new InvalidQueueRequestException("maxAttempts must be >= 1");
        }
    }

    /**
     * Parses a nullable direct JSON string into a map.
     */
    private Map<String, Object> readJson(String json) {
        String normalized = json == null || json.isBlank() ? DEFAULT_JSON : json;
        try {
            return objectMapper.readValue(normalized, MAP_TYPE);
        } catch (Exception e) {
            throw new InvalidQueueRequestException("json value is not valid");
        }
    }

    /**
     * Maps stable core errors into direct-client typed exceptions.
     */
    private QueueException mapCoreException(com.sequencedqueue.core.QueueException e) {
        return switch (e.errorCode()) {
            case FIELD_TOO_LARGE -> mapFieldTooLargeException(e);
            case VALIDATION_ERROR -> new InvalidQueueRequestException(e.getMessage());
            case ITEM_NOT_FOUND, SOURCE_NOT_FOUND -> new ItemNotFoundException(e.getMessage());
            case LEASE_LOST, LEASE_EXPIRED -> new LeaseLostException(e.getMessage());
            case ITEM_NOT_PROCESSING -> new ItemNotClaimedException(e.getMessage());
            case SOURCE_BLOCKED -> new SourceBlockedException(e.getMessage());
            case IDEMPOTENCY_CONFLICT -> new DuplicateIdempotencyKeyException(e.getMessage(), e);
            case QUEUE_CONFLICT -> new QueueConflictException(e.getMessage(), e);
            case INTERNAL_ERROR -> new QueueUnavailableException(e.getMessage(), e);
        };
    }

    /**
     * Maps structured core oversized-field validation to the direct-client typed exception.
     */
    private QueueException mapFieldTooLargeException(com.sequencedqueue.core.QueueException e) {
        if (e instanceof com.sequencedqueue.core.QueueFieldTooLargeException tooLarge) {
            return new com.sequencedqueue.direct.QueueFieldTooLargeException(
                e.getMessage(),
                tooLarge.fieldName(),
                tooLarge.maxBytes(),
                tooLarge.actualBytes(),
                e.queueName(),
                e.sourceId(),
                e.itemId(),
                e
            );
        }
        return new InvalidQueueRequestException(e.getMessage(), e);
    }

    /**
     * Converts a core claim response into direct-client DTOs.
     */
    private ClaimResponse toDirect(QueueDtos.ClaimResponse response) {
        return new ClaimResponse(response.leaseId(), response.queueName(), response.sourceId(), response.leaseUntil(), response.items().stream()
            .map(item -> new ClaimItem(item.itemId(), item.sequenceNo(), item.itemType(), item.payload(), item.headers()))
            .toList());
    }

    /**
     * Converts a core item response into direct-client DTOs.
     */
    private ItemResponse toDirect(QueueDtos.ItemResponse item) {
        return new ItemResponse(item.itemId(), item.queueName(), item.sourceId(), item.sequenceNo(), item.itemType(), item.payload(), item.headers(), item.status(), item.availableAt(), item.claimedBy(), item.leaseId(), item.leaseUntil(), item.attemptCount(), item.maxAttempts(), item.idempotencyKey(), item.lastErrorType(), item.lastErrorMessage(), item.result(), item.createdAt(), item.updatedAt());
    }

    /**
     * Converts a core source response into direct-client DTOs.
     */
    private SourceResponse toDirect(QueueDtos.SourceResponse source) {
        return new SourceResponse(source.queueName(), source.sourceId(), source.nextSequenceNo(), source.status(), source.leasedBy(), source.leaseId(), source.leaseUntil(), source.updatedAt());
    }

    /**
     * Builder for trusted direct Java clients.
     */
    public static final class Builder {
        /** PostgreSQL data source used by core. */
        private DataSource dataSource;
        /** Optional default queue name for enqueue convenience. */
        private String defaultQueueName;
        /** Optional JSON mapper for direct JSON parsing and core persistence. */
        private ObjectMapper objectMapper;
        /** Whether to validate Flyway schema compatibility during build. */
        private boolean validateSchemaOnBuild;

        /**
         * Sets the PostgreSQL data source.
         */
        public Builder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets the optional default queue name.
         */
        public Builder defaultQueueName(String defaultQueueName) {
            this.defaultQueueName = defaultQueueName;
            return this;
        }

        /**
         * Sets the JSON mapper.
         */
        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        /**
         * Enables or disables schema validation at build time.
         */
        public Builder validateSchemaOnBuild(boolean validateSchemaOnBuild) {
            this.validateSchemaOnBuild = validateSchemaOnBuild;
            return this;
        }

        /**
         * Builds the direct client.
         *
         * @return direct client
         */
        public SequencedQueueDirectClient build() {
            return new SequencedQueueDirectClient(this);
        }
    }
}
