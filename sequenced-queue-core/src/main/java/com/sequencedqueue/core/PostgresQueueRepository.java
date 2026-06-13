package com.sequencedqueue.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PostgresQueueRepository implements QueueRepository {
    private final SqlConnectionProvider connections;

    public PostgresQueueRepository(SqlConnectionProvider connections) {
        this.connections = connections;
    }

    @Override
    public Optional<QueueItemRow> findByIdempotencyKey(String queueName, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Optional.empty();
        }
        return query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND idempotency_key = ?
            """, this::mapItem, queueName, idempotencyKey).stream().findFirst();
    }

    @Override
    public void ensureSource(String queueName, String sourceId, OffsetDateTime now) {
        update("""
            INSERT INTO queue_source_state (queue_name, source_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (queue_name, source_id) DO NOTHING
            """, queueName, sourceId, now, now);
    }

    @Override
    public SourceStateRow lockSource(String queueName, String sourceId) {
        return queryOne("""
            SELECT * FROM queue_source_state
            WHERE queue_name = ? AND source_id = ?
            FOR UPDATE
            """, this::mapSource, queueName, sourceId);
    }

    @Override
    public QueueItemRow insertItem(UUID itemId, String queueName, String sourceId, long sequenceNo, String itemType, String payloadJson, String headersJson, OffsetDateTime availableAt, int maxAttempts, String idempotencyKey, OffsetDateTime now) {
        String sql = """
            INSERT INTO queue_item (
                item_id, queue_name, source_id, sequence_no, item_type, payload_json, headers_json,
                status, available_at, max_attempts, idempotency_key, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), 'pending', ?, ?, ?, ?, ?)
            RETURNING *
            """;
        try (PreparedStatement statement = connection().prepareStatement(sql)) {
            bind(statement, itemId, queueName, sourceId, sequenceNo, itemType, payloadJson, headersJson, availableAt, maxAttempts, idempotencyKey, now, now);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "insert did not return queue item");
                }
                return mapItem(rs);
            }
        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState()) && idempotencyKey != null) {
                throw new DuplicateIdempotencyKeyException("idempotency key already exists for queueName=" + queueName, e);
            }
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "database insert failed", e);
        }
    }

    @Override
    public void incrementNextSequence(String queueName, String sourceId, OffsetDateTime now) {
        update("""
            UPDATE queue_source_state
            SET next_sequence_no = next_sequence_no + 1, updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """, now, queueName, sourceId);
    }

    @Override
    public Optional<QueueItemRow> claimHeadItem(String queueName, List<String> itemTypes, String workerId, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime now) {
        if (itemTypes == null || itemTypes.isEmpty()) {
            return Optional.empty();
        }
        String placeholders = String.join(",", itemTypes.stream().map(t -> "?").toList());
        List<Object> args = new ArrayList<>();
        args.add(queueName);
        args.addAll(itemTypes);
        args.add(now);
        args.add(workerId);
        args.add(leaseId);
        args.add(leaseUntil);
        args.add(now);
        args.add(workerId);
        args.add(leaseId);
        args.add(now);
        args.add(leaseUntil);
        args.add(now);
        args.add(now);

        String sql = """
            WITH candidate_source AS (
                SELECT s.queue_name, s.source_id
                FROM queue_source_state s
                JOIN LATERAL (
                    SELECT i.item_id, i.item_type
                    FROM queue_item i
                    WHERE i.queue_name = s.queue_name
                      AND i.source_id = s.source_id
                      AND i.status NOT IN ('succeeded', 'cancelled', 'skipped', 'failed')
                    ORDER BY i.sequence_no
                    LIMIT 1
                ) head ON true
                WHERE s.queue_name = ?
                  AND s.status = 'idle'
                  AND head.item_type IN (%s)
                  AND EXISTS (
                    SELECT 1
                    FROM queue_item ready
                    WHERE ready.item_id = head.item_id
                      AND ready.status IN ('pending', 'retry_wait')
                      AND ready.available_at <= ?
                  )
                ORDER BY s.updated_at, s.source_id
                LIMIT 1
                FOR UPDATE OF s SKIP LOCKED
            ), leased_source AS (
                UPDATE queue_source_state s
                SET status = 'leased',
                    leased_by = ?,
                    lease_id = ?,
                    lease_until = ?,
                    updated_at = ?
                FROM candidate_source c
                WHERE s.queue_name = c.queue_name AND s.source_id = c.source_id
                RETURNING s.queue_name, s.source_id
            )
            UPDATE queue_item i
            SET status = 'processing',
                claimed_by = ?,
                lease_id = ?,
                claimed_at = ?,
                lease_until = ?,
                attempt_count = attempt_count + 1,
                updated_at = ?
            FROM leased_source ls
            WHERE i.queue_name = ls.queue_name
              AND i.source_id = ls.source_id
              AND i.item_id = (
                SELECT head.item_id
                FROM queue_item head
                WHERE head.queue_name = ls.queue_name
                  AND head.source_id = ls.source_id
                  AND head.status IN ('pending', 'retry_wait')
                  AND head.available_at <= ?
                ORDER BY head.sequence_no
                LIMIT 1
              )
            RETURNING i.*
            """.formatted(placeholders);

        return query(sql, this::mapItem, args.toArray()).stream().findFirst();
    }

    @Override
    public Optional<QueueItemRow> findItem(String queueName, UUID itemId) {
        return query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND item_id = ?
            """, this::mapItem, queueName, itemId).stream().findFirst();
    }

    @Override
    public QueueItemRow lockItem(String queueName, UUID itemId) {
        return queryOne("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND item_id = ?
            FOR UPDATE
            """, this::mapItem, queueName, itemId);
    }

    @Override
    public List<QueueItemRow> listSourceItems(String queueName, String sourceId) {
        return query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND source_id = ?
            ORDER BY sequence_no
            """, this::mapItem, queueName, sourceId);
    }

    @Override
    public List<QueueItemRow> listDeadLettered(String queueName, int limit, int offset) {
        return query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND status = 'dead_lettered'
            ORDER BY updated_at DESC, sequence_no
            LIMIT ? OFFSET ?
            """, this::mapItem, queueName, limit, offset);
    }

    @Override
    public QueueItemRow complete(UUID itemId, String resultJson, OffsetDateTime now) {
        return queryOne("""
            UPDATE queue_item
            SET status = 'succeeded',
                result_json = CAST(? AS jsonb),
                claimed_by = NULL,
                lease_id = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                updated_at = ?
            WHERE item_id = ?
            RETURNING *
            """, this::mapItem, resultJson, now, itemId);
    }

    @Override
    public QueueItemRow fail(UUID itemId, ItemStatus status, OffsetDateTime availableAt, String errorType, String errorMessage, OffsetDateTime now) {
        return queryOne("""
            UPDATE queue_item
            SET status = ?,
                available_at = ?,
                claimed_by = NULL,
                lease_id = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                last_error_type = ?,
                last_error_message = ?,
                updated_at = ?
            WHERE item_id = ?
            RETURNING *
            """, this::mapItem, status.name(), availableAt, errorType, errorMessage, now, itemId);
    }

    @Override
    public void releaseSource(String queueName, String sourceId, OffsetDateTime now) {
        update("""
            UPDATE queue_source_state
            SET status = 'idle', leased_by = NULL, lease_id = NULL, lease_until = NULL, updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """, now, queueName, sourceId);
    }

    @Override
    public void blockSource(String queueName, String sourceId, OffsetDateTime now) {
        update("""
            UPDATE queue_source_state
            SET status = 'blocked', leased_by = NULL, lease_id = NULL, lease_until = NULL, updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """, now, queueName, sourceId);
    }

    @Override
    public int releaseSourceIfLeaseMatches(String queueName, String sourceId, UUID leaseId, String leasedBy, OffsetDateTime now) {
        return update("""
            UPDATE queue_source_state
            SET status = 'idle', leased_by = NULL, lease_id = NULL, lease_until = NULL, updated_at = ?
            WHERE queue_name = ? AND source_id = ? AND lease_id = ? AND leased_by = ?
            """, now, queueName, sourceId, leaseId, leasedBy);
    }

    @Override
    public int blockSourceIfLeaseMatches(String queueName, String sourceId, UUID leaseId, String leasedBy, OffsetDateTime now) {
        return update("""
            UPDATE queue_source_state
            SET status = 'blocked', leased_by = NULL, lease_id = NULL, lease_until = NULL, updated_at = ?
            WHERE queue_name = ? AND source_id = ? AND lease_id = ? AND leased_by = ?
            """, now, queueName, sourceId, leaseId, leasedBy);
    }

    @Override
    public int heartbeat(String queueName, UUID leaseId, String workerId, OffsetDateTime leaseUntil, OffsetDateTime now) {
        int sources = update("""
            UPDATE queue_source_state
            SET lease_until = ?, updated_at = ?
            WHERE queue_name = ? AND lease_id = ? AND leased_by = ? AND status = 'leased' AND lease_until > ?
            """, leaseUntil, now, queueName, leaseId, workerId, now);
        int items = update("""
            UPDATE queue_item
            SET lease_until = ?, updated_at = ?
            WHERE queue_name = ? AND lease_id = ? AND claimed_by = ? AND status = 'processing' AND lease_until > ?
            """, leaseUntil, now, queueName, leaseId, workerId, now);
        return Math.min(sources, items);
    }

    @Override
    public List<SourceStateRow> blockedSources(String queueName) {
        return query("""
            SELECT * FROM queue_source_state
            WHERE queue_name = ? AND status = 'blocked'
            ORDER BY updated_at, source_id
            """, this::mapSource, queueName);
    }

    @Override
    public List<BlockedSourceRow> inspectBlockedSources(String queueName, int limit, int offset) {
        return query("""
            SELECT s.queue_name,
                   s.source_id,
                   s.status,
                   s.leased_by,
                   s.lease_until,
                   s.updated_at,
                   head.item_id AS head_item_id,
                   head.status AS head_item_status
            FROM queue_source_state s
            LEFT JOIN LATERAL (
                SELECT i.item_id, i.status
                FROM queue_item i
                WHERE i.queue_name = s.queue_name
                  AND i.source_id = s.source_id
                  AND i.status NOT IN ('succeeded', 'cancelled', 'skipped', 'failed')
                ORDER BY i.sequence_no
                LIMIT 1
            ) head ON true
            WHERE s.queue_name = ? AND s.status = 'blocked'
            ORDER BY s.updated_at, s.source_id
            LIMIT ? OFFSET ?
            """, this::mapBlockedSource, queueName, limit, offset);
    }

    @Override
    public SourceStateRow findSource(String queueName, String sourceId) {
        return queryOne("""
            SELECT * FROM queue_source_state
            WHERE queue_name = ? AND source_id = ?
            """, this::mapSource, queueName, sourceId);
    }

    @Override
    public QueueItemRow adminStatus(UUID itemId, ItemStatus status, OffsetDateTime availableAt, OffsetDateTime now) {
        return queryOne("""
            UPDATE queue_item
            SET status = ?,
                available_at = ?,
                claimed_by = NULL,
                lease_id = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                updated_at = ?
            WHERE item_id = ?
            RETURNING *
            """, this::mapItem, status.name(), availableAt, now, itemId);
    }

    @Override
    public Optional<QueueItemRow> skipDeadLetteredHead(String queueName, String sourceId, OffsetDateTime now) {
        return query("""
            UPDATE queue_item i
            SET status = 'skipped',
                claimed_by = NULL,
                lease_id = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                updated_at = ?
            WHERE i.item_id = (
                SELECT head.item_id
                FROM queue_item head
                WHERE head.queue_name = ?
                  AND head.source_id = ?
                  AND head.status NOT IN ('succeeded', 'cancelled', 'skipped', 'failed')
                ORDER BY head.sequence_no
                LIMIT 1
            )
            AND i.status = 'dead_lettered'
            RETURNING i.*
            """, this::mapItem, now, queueName, sourceId).stream().findFirst();
    }

    @Override
    public Optional<QueueItemRow> findHeadBlockingItem(String queueName, String sourceId) {
        return query("""
            SELECT *
            FROM queue_item
            WHERE queue_name = ?
              AND source_id = ?
              AND status NOT IN ('succeeded', 'cancelled', 'skipped', 'failed')
            ORDER BY sequence_no
            LIMIT 1
            FOR UPDATE
            """, this::mapItem, queueName, sourceId).stream().findFirst();
    }

    @Override
    public int blockDeadLetteredHeadSources(String queueName, OffsetDateTime now) {
        return update("""
            UPDATE queue_source_state s
            SET status = 'blocked',
                leased_by = NULL,
                lease_id = NULL,
                lease_until = NULL,
                updated_at = ?
            WHERE s.queue_name = ?
              AND s.status = 'idle'
              AND EXISTS (
                SELECT 1
                FROM queue_item head
                WHERE head.item_id = (
                  SELECT i.item_id
                  FROM queue_item i
                  WHERE i.queue_name = s.queue_name
                    AND i.source_id = s.source_id
                    AND i.status NOT IN ('succeeded', 'cancelled', 'skipped', 'failed')
                  ORDER BY i.sequence_no
                  LIMIT 1
                )
                AND head.status = 'dead_lettered'
              )
            """, now, queueName);
    }

    @Override
    public List<QueueItemRow> expiredProcessing(OffsetDateTime now, int limit) {
        return query("""
            SELECT * FROM queue_item
            WHERE status = 'processing' AND lease_until < ?
            ORDER BY lease_until
            LIMIT ?
            """, this::mapItem, now, limit);
    }

    @Override
    public void insertAdminAudit(UUID auditId, OffsetDateTime occurredAt, String actorId, String operation, String queueName, String sourceId, UUID itemId, String previousStatus, String newStatus, String reason, String metadataJson) {
        update("""
            INSERT INTO queue_admin_audit (
                audit_id, occurred_at, actor_id, operation, queue_name, source_id,
                item_id, previous_status, new_status, reason, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
            """, auditId, occurredAt, actorId, operation, queueName, sourceId, itemId, previousStatus, newStatus, reason, metadataJson);
    }

    @Override
    public List<AdminAuditRow> listAdminAudit(String queueName, int limit, int offset) {
        return query("""
            SELECT *
            FROM queue_admin_audit
            WHERE queue_name = ?
            ORDER BY occurred_at DESC
            LIMIT ? OFFSET ?
            """, this::mapAdminAudit, queueName, limit, offset);
    }

    @Override
    public long countRetentionEligible(String queueName, OffsetDateTime olderThan, List<ItemStatus> statuses) {
        String placeholders = statusPlaceholders(statuses);
        List<Object> args = retentionArgs(queueName, olderThan, statuses);
        return query("""
            SELECT count(*) AS row_count
            FROM queue_item
            WHERE queue_name = ?
              AND updated_at < ?
              AND status IN (%s)
            """.formatted(placeholders), rs -> rs.getLong("row_count"), args.toArray()).getFirst();
    }

    @Override
    public long deleteRetentionEligible(String queueName, OffsetDateTime olderThan, List<ItemStatus> statuses) {
        String placeholders = statusPlaceholders(statuses);
        List<Object> args = retentionArgs(queueName, olderThan, statuses);
        return update("""
            DELETE FROM queue_item
            WHERE queue_name = ?
              AND updated_at < ?
              AND status IN (%s)
            """.formatted(placeholders), args.toArray());
    }

    @Override
    public QueueDtos.QueueMetricsSnapshot metricsSnapshot() {
        return query("""
            SELECT
                COUNT(*) FILTER (WHERE i.status = 'pending') AS pending_items,
                COUNT(*) FILTER (WHERE i.status = 'processing') AS processing_items,
                COUNT(*) FILTER (WHERE i.status = 'retry_wait') AS retry_wait_items,
                COUNT(*) FILTER (WHERE i.status = 'dead_lettered') AS dead_lettered_items,
                (SELECT COUNT(*) FROM queue_source_state s WHERE s.status = 'idle') AS idle_sources,
                (SELECT COUNT(*) FROM queue_source_state s WHERE s.status = 'leased') AS leased_sources,
                (SELECT COUNT(*) FROM queue_source_state s WHERE s.status = 'blocked') AS blocked_sources
            FROM queue_item i
            """, rs -> new QueueDtos.QueueMetricsSnapshot(
                rs.getLong("pending_items"),
                rs.getLong("processing_items"),
                rs.getLong("retry_wait_items"),
                rs.getLong("dead_lettered_items"),
                rs.getLong("idle_sources"),
                rs.getLong("leased_sources"),
                rs.getLong("blocked_sources")
            )).getFirst();
    }

    @Override
    public QueueSchemaInfo getSchemaInfo() {
        return query("""
            SELECT
                (
                    SELECT version
                    FROM flyway_schema_history
                    WHERE success = true
                    ORDER BY installed_rank DESC
                    LIMIT 1
                ) AS schema_version,
                EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name = 'queue_item'
                ) AS queue_item_table_present,
                EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name = 'queue_source_state'
                ) AS queue_source_state_table_present,
                EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name = 'queue_admin_audit'
                ) AS admin_audit_table_present
            """, rs -> new QueueSchemaInfo(
                rs.getString("schema_version"),
                rs.getBoolean("queue_item_table_present"),
                rs.getBoolean("queue_source_state_table_present"),
                rs.getBoolean("admin_audit_table_present")
            )).getFirst();
    }

    private <T> T queryOne(String sql, RowMapper<T> mapper, Object... args) {
        List<T> results = query(sql, mapper, args);
        if (results.isEmpty()) {
            throw new QueueException(QueueException.NOT_FOUND, "database row not found");
        }
        return results.getFirst();
    }

    private <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
        try (PreparedStatement statement = connection().prepareStatement(sql)) {
            bind(statement, args);
            try (ResultSet rs = statement.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapper.map(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "database query failed", e);
        }
    }

    private int update(String sql, Object... args) {
        try (PreparedStatement statement = connection().prepareStatement(sql)) {
            bind(statement, args);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new QueueException(QueueException.INTERNAL_SERVER_ERROR, "database update failed", e);
        }
    }

    private void bind(PreparedStatement statement, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            statement.setObject(i + 1, args[i]);
        }
    }

    private String statusPlaceholders(List<ItemStatus> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            throw new QueueException(QueueException.BAD_REQUEST, "statuses is required");
        }
        return String.join(",", statuses.stream().map(status -> "?").toList());
    }

    private List<Object> retentionArgs(String queueName, OffsetDateTime olderThan, List<ItemStatus> statuses) {
        List<Object> args = new ArrayList<>();
        args.add(queueName);
        args.add(olderThan);
        args.addAll(statuses.stream().map(Enum::name).toList());
        return args;
    }

    private Connection connection() {
        return connections.currentConnection();
    }

    private QueueItemRow mapItem(ResultSet rs) throws SQLException {
        return new QueueItemRow(rs.getObject("item_id", UUID.class), rs.getString("queue_name"), rs.getString("source_id"), rs.getLong("sequence_no"), rs.getString("item_type"), rs.getString("payload_json"), rs.getString("headers_json"), ItemStatus.valueOf(rs.getString("status")), toOffset(rs.getTimestamp("available_at")), rs.getString("claimed_by"), rs.getObject("lease_id", UUID.class), toOffset(rs.getTimestamp("lease_until")), rs.getInt("attempt_count"), rs.getInt("max_attempts"), rs.getString("idempotency_key"), rs.getString("last_error_type"), rs.getString("last_error_message"), rs.getString("result_json"), toOffset(rs.getTimestamp("created_at")), toOffset(rs.getTimestamp("updated_at")));
    }

    private SourceStateRow mapSource(ResultSet rs) throws SQLException {
        return new SourceStateRow(rs.getString("queue_name"), rs.getString("source_id"), rs.getLong("next_sequence_no"), SourceStatus.valueOf(rs.getString("status")), rs.getString("leased_by"), rs.getObject("lease_id", UUID.class), toOffset(rs.getTimestamp("lease_until")), toOffset(rs.getTimestamp("created_at")), toOffset(rs.getTimestamp("updated_at")));
    }

    private BlockedSourceRow mapBlockedSource(ResultSet rs) throws SQLException {
        String headItemStatus = rs.getString("head_item_status");
        return new BlockedSourceRow(
            rs.getString("queue_name"),
            rs.getString("source_id"),
            SourceStatus.valueOf(rs.getString("status")),
            rs.getString("leased_by"),
            toOffset(rs.getTimestamp("lease_until")),
            rs.getObject("head_item_id", UUID.class),
            headItemStatus == null ? null : ItemStatus.valueOf(headItemStatus),
            toOffset(rs.getTimestamp("updated_at"))
        );
    }

    private AdminAuditRow mapAdminAudit(ResultSet rs) throws SQLException {
        return new AdminAuditRow(
            rs.getObject("audit_id", UUID.class),
            toOffset(rs.getTimestamp("occurred_at")),
            rs.getString("actor_id"),
            rs.getString("operation"),
            rs.getString("queue_name"),
            rs.getString("source_id"),
            rs.getObject("item_id", UUID.class),
            rs.getString("previous_status"),
            rs.getString("new_status"),
            rs.getString("reason"),
            rs.getString("metadata_json")
        );
    }

    private OffsetDateTime toOffset(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant().atOffset(ZoneOffset.UTC);
    }

    @FunctionalInterface
    private interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }
}
