package com.sequencedqueue.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SequencedQueuePostgresClient {
    private static final String DEFAULT_JSON = "{}";
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    private final DataSource dataSource;
    private final String defaultQueueName;
    private final ObjectMapper objectMapper;

    private SequencedQueuePostgresClient(Builder builder) {
        this.dataSource = Objects.requireNonNull(builder.dataSource, "dataSource is required");
        this.defaultQueueName = builder.defaultQueueName;
        this.objectMapper = builder.objectMapper == null ? new ObjectMapper() : builder.objectMapper;
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
        validateQueueName(queueName);
        validateRequest(request);

        String payloadJson = normalizeJson(request.payloadJson());
        String headersJson = normalizeJson(request.headersJson());
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime availableAt = request.availableAt() == null ? now : request.availableAt();
        int maxAttempts = request.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : request.maxAttempts();

        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                ensureSource(connection, queueName, request.sourceId(), now);
                SourceState source = lockSource(connection, queueName, request.sourceId());

                EnqueueResponse existing = findByIdempotencyKey(connection, queueName, request.idempotencyKey());
                if (existing != null) {
                    connection.commit();
                    return existing;
                }

                long sequenceNo = source.nextSequenceNo();
                incrementNextSequence(connection, queueName, request.sourceId(), now);
                EnqueueResponse response = insertItem(
                    connection,
                    UUID.randomUUID(),
                    queueName,
                    request.sourceId(),
                    sequenceNo,
                    request.itemType(),
                    payloadJson,
                    headersJson,
                    availableAt,
                    maxAttempts,
                    blankToNull(request.idempotencyKey()),
                    now
                );
                connection.commit();
                return response;
            } catch (SQLException e) {
                rollbackQuietly(connection);
                if ("23505".equals(e.getSQLState()) && request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
                    return readExistingAfterIdempotencyRace(queueName, request.idempotencyKey(), e);
                }
                throw sqlException("enqueue", queueName, request.sourceId(), e);
            } catch (RuntimeException e) {
                rollbackQuietly(connection);
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            throw sqlException("enqueue", queueName, request.sourceId(), e);
        }
    }

    public QueueSchemaInfo getSchemaInfo() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT version
                 FROM flyway_schema_history
                 WHERE success = true
                 ORDER BY installed_rank DESC
                 LIMIT 1
                 """)) {
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new QueueSchemaInfo(rs.getString("version"));
                }
                return new QueueSchemaInfo(null);
            }
        } catch (SQLException e) {
            throw sqlException("getSchemaInfo", null, null, e);
        }
    }

    private void ensureSource(Connection connection, String queueName, String sourceId, OffsetDateTime now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO queue_source_state (queue_name, source_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (queue_name, source_id) DO NOTHING
            """)) {
            statement.setString(1, queueName);
            statement.setString(2, sourceId);
            statement.setObject(3, now);
            statement.setObject(4, now);
            statement.executeUpdate();
        }
    }

    private SourceState lockSource(Connection connection, String queueName, String sourceId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT next_sequence_no
            FROM queue_source_state
            WHERE queue_name = ? AND source_id = ?
            FOR UPDATE
            """)) {
            statement.setString(1, queueName);
            statement.setString(2, sourceId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new InvalidQueueRequestException("source state was not created for queueName=" + queueName + ", sourceId=" + sourceId);
                }
                return new SourceState(rs.getLong("next_sequence_no"));
            }
        }
    }

    private EnqueueResponse findByIdempotencyKey(Connection connection, String queueName, String idempotencyKey) throws SQLException {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return null;
        }
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT item_id, queue_name, source_id, sequence_no, status
            FROM queue_item
            WHERE queue_name = ? AND idempotency_key = ?
            """)) {
            statement.setString(1, queueName);
            statement.setString(2, idempotencyKey);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? mapEnqueueResponse(rs) : null;
            }
        }
    }

    private void incrementNextSequence(Connection connection, String queueName, String sourceId, OffsetDateTime now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            UPDATE queue_source_state
            SET next_sequence_no = next_sequence_no + 1,
                updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """)) {
            statement.setObject(1, now);
            statement.setString(2, queueName);
            statement.setString(3, sourceId);
            statement.executeUpdate();
        }
    }

    private EnqueueResponse insertItem(
        Connection connection,
        UUID itemId,
        String queueName,
        String sourceId,
        long sequenceNo,
        String itemType,
        String payloadJson,
        String headersJson,
        OffsetDateTime availableAt,
        int maxAttempts,
        String idempotencyKey,
        OffsetDateTime now
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO queue_item (
                item_id, queue_name, source_id, sequence_no, item_type, payload_json, headers_json,
                status, available_at, max_attempts, idempotency_key, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), 'pending', ?, ?, ?, ?, ?)
            RETURNING item_id, queue_name, source_id, sequence_no, status
            """)) {
            statement.setObject(1, itemId);
            statement.setString(2, queueName);
            statement.setString(3, sourceId);
            statement.setLong(4, sequenceNo);
            statement.setString(5, itemType);
            statement.setString(6, payloadJson);
            statement.setString(7, headersJson);
            statement.setObject(8, availableAt);
            statement.setInt(9, maxAttempts);
            statement.setString(10, idempotencyKey);
            statement.setObject(11, now);
            statement.setObject(12, now);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return mapEnqueueResponse(rs);
            }
        }
    }

    private EnqueueResponse readExistingAfterIdempotencyRace(String queueName, String idempotencyKey, SQLException duplicate) {
        try (Connection connection = dataSource.getConnection()) {
            EnqueueResponse existing = findByIdempotencyKey(connection, queueName, idempotencyKey);
            if (existing != null) {
                return existing;
            }
            throw new DuplicateIdempotencyKeyException("duplicate idempotency key but existing item could not be read for queueName=" + queueName, duplicate);
        } catch (SQLException e) {
            throw new DuplicateIdempotencyKeyException("duplicate idempotency key lookup failed for queueName=" + queueName, e);
        }
    }

    private EnqueueResponse mapEnqueueResponse(ResultSet rs) throws SQLException {
        return new EnqueueResponse(
            rs.getObject("item_id", UUID.class),
            rs.getString("queue_name"),
            rs.getString("source_id"),
            rs.getLong("sequence_no"),
            rs.getString("status")
        );
    }

    private void validateQueueName(String queueName) {
        if (queueName == null || queueName.isBlank()) {
            throw new InvalidQueueRequestException("queueName is required");
        }
    }

    private void validateRequest(EnqueueRequest request) {
        if (request == null) {
            throw new InvalidQueueRequestException("request is required");
        }
        if (request.sourceId() == null || request.sourceId().isBlank()) {
            throw new InvalidQueueRequestException("sourceId is required");
        }
        if (request.itemType() == null || request.itemType().isBlank()) {
            throw new InvalidQueueRequestException("itemType is required");
        }
        if (request.maxAttempts() != null && request.maxAttempts() < 1) {
            throw new InvalidQueueRequestException("maxAttempts must be >= 1");
        }
    }

    private String normalizeJson(String json) {
        String normalized = json == null || json.isBlank() ? DEFAULT_JSON : json;
        try {
            objectMapper.readTree(normalized);
            return normalized;
        } catch (Exception e) {
            throw new InvalidQueueRequestException("json value is not valid");
        }
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    private QueueException sqlException(String operation, String queueName, String sourceId, SQLException e) {
        String message = "PostgreSQL queue operation failed"
            + " operation=" + operation
            + (queueName == null ? "" : " queueName=" + queueName)
            + (sourceId == null ? "" : " sourceId=" + sourceId);
        return new QueueUnavailableException(message, e);
    }

    private record SourceState(long nextSequenceNo) {
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

        public SequencedQueuePostgresClient build() {
            return new SequencedQueuePostgresClient(this);
        }
    }
}
