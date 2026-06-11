package com.example.sequencedqueue.server.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.example.sequencedqueue.server.core.ItemStatus;
import com.example.sequencedqueue.server.core.QueueRepository;
import com.example.sequencedqueue.server.core.SourceStatus;
import com.example.sequencedqueue.server.store.QueueItemRow;
import com.example.sequencedqueue.server.store.SourceStateRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class SpringJdbcQueueRepository implements QueueRepository {
    private final JdbcTemplate jdbc;

    private final RowMapper<QueueItemRow> itemMapper = this::mapItem;
    private final RowMapper<SourceStateRow> sourceMapper = this::mapSource;

    public SpringJdbcQueueRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<QueueItemRow> findByIdempotencyKey(String queueName, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Optional.empty();
        }
        return jdbc.query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND idempotency_key = ?
            """, itemMapper, queueName, idempotencyKey).stream().findFirst();
    }

    public void ensureSource(String queueName, String sourceId, OffsetDateTime now) {
        jdbc.update("""
            INSERT INTO queue_source_state (queue_name, source_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (queue_name, source_id) DO NOTHING
            """, queueName, sourceId, now, now);
    }

    public SourceStateRow lockSource(String queueName, String sourceId) {
        return jdbc.queryForObject("""
            SELECT * FROM queue_source_state
            WHERE queue_name = ? AND source_id = ?
            FOR UPDATE
            """, sourceMapper, queueName, sourceId);
    }

    public QueueItemRow insertItem(
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
    ) {
        return jdbc.queryForObject("""
            INSERT INTO queue_item (
                item_id, queue_name, source_id, sequence_no, item_type, payload_json, headers_json,
                status, available_at, max_attempts, idempotency_key, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), 'pending', ?, ?, ?, ?, ?)
            RETURNING *
            """, itemMapper, itemId, queueName, sourceId, sequenceNo, itemType, payloadJson, headersJson,
            availableAt, maxAttempts, idempotencyKey, now, now);
    }

    public void incrementNextSequence(String queueName, String sourceId, OffsetDateTime now) {
        jdbc.update("""
            UPDATE queue_source_state
            SET next_sequence_no = next_sequence_no + 1, updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """, now, queueName, sourceId);
    }

    public Optional<QueueItemRow> claimHeadItem(String queueName, List<String> itemTypes, String workerId, UUID leaseId, OffsetDateTime leaseUntil, OffsetDateTime now) {
        if (itemTypes == null || itemTypes.isEmpty()) {
            return Optional.empty();
        }
        String placeholders = String.join(",", itemTypes.stream().map(t -> "?").toList());
        Object[] args = new Object[6 + itemTypes.size()];
        int index = 0;
        args[index++] = queueName;
        for (String itemType : itemTypes) {
            args[index++] = itemType;
        }
        args[index++] = now;
        args[index++] = workerId;
        args[index++] = leaseId;
        args[index++] = leaseUntil;
        args[index] = now;

        String sql = """
            WITH candidate_source AS (
                SELECT s.queue_name, s.source_id
                FROM queue_source_state s
                JOIN LATERAL (
                    SELECT i.item_id, i.item_type
                    FROM queue_item i
                    WHERE i.queue_name = s.queue_name
                      AND i.source_id = s.source_id
                      AND i.status NOT IN ('succeeded', 'cancelled', 'skipped')
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

        Object[] finalArgs = new Object[args.length + 6];
        System.arraycopy(args, 0, finalArgs, 0, args.length);
        int p = args.length;
        finalArgs[p++] = workerId;
        finalArgs[p++] = leaseId;
        finalArgs[p++] = now;
        finalArgs[p++] = leaseUntil;
        finalArgs[p++] = now;
        finalArgs[p] = now;

        return jdbc.query(sql, itemMapper, finalArgs).stream().findFirst();
    }

    public Optional<QueueItemRow> findItem(String queueName, UUID itemId) {
        return jdbc.query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND item_id = ?
            """, itemMapper, queueName, itemId).stream().findFirst();
    }

    public QueueItemRow lockItem(String queueName, UUID itemId) {
        return jdbc.queryForObject("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND item_id = ?
            FOR UPDATE
            """, itemMapper, queueName, itemId);
    }

    public List<QueueItemRow> listSourceItems(String queueName, String sourceId) {
        return jdbc.query("""
            SELECT * FROM queue_item
            WHERE queue_name = ? AND source_id = ?
            ORDER BY sequence_no
            """, itemMapper, queueName, sourceId);
    }

    public QueueItemRow complete(UUID itemId, String resultJson, OffsetDateTime now) {
        return jdbc.queryForObject("""
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
            """, itemMapper, resultJson, now, itemId);
    }

    public QueueItemRow fail(UUID itemId, ItemStatus status, OffsetDateTime availableAt, String errorType, String errorMessage, OffsetDateTime now) {
        return jdbc.queryForObject("""
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
            """, itemMapper, status.name(), availableAt, errorType, errorMessage, now, itemId);
    }

    public void releaseSource(String queueName, String sourceId, OffsetDateTime now) {
        jdbc.update("""
            UPDATE queue_source_state
            SET status = 'idle', leased_by = NULL, lease_id = NULL, lease_until = NULL, updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """, now, queueName, sourceId);
    }

    public void blockSource(String queueName, String sourceId, OffsetDateTime now) {
        jdbc.update("""
            UPDATE queue_source_state
            SET status = 'blocked', leased_by = NULL, lease_id = NULL, lease_until = NULL, updated_at = ?
            WHERE queue_name = ? AND source_id = ?
            """, now, queueName, sourceId);
    }

    public int heartbeat(String queueName, UUID leaseId, String workerId, OffsetDateTime leaseUntil, OffsetDateTime now) {
        int sources = jdbc.update("""
            UPDATE queue_source_state
            SET lease_until = ?, updated_at = ?
            WHERE queue_name = ? AND lease_id = ? AND leased_by = ? AND status = 'leased'
            """, leaseUntil, now, queueName, leaseId, workerId);
        int items = jdbc.update("""
            UPDATE queue_item
            SET lease_until = ?, updated_at = ?
            WHERE queue_name = ? AND lease_id = ? AND claimed_by = ? AND status = 'processing'
            """, leaseUntil, now, queueName, leaseId, workerId);
        return Math.min(sources, items);
    }

    public List<SourceStateRow> blockedSources(String queueName) {
        return jdbc.query("""
            SELECT * FROM queue_source_state
            WHERE queue_name = ? AND status = 'blocked'
            ORDER BY updated_at, source_id
            """, sourceMapper, queueName);
    }

    public SourceStateRow findSource(String queueName, String sourceId) {
        return jdbc.queryForObject("""
            SELECT * FROM queue_source_state
            WHERE queue_name = ? AND source_id = ?
            """, sourceMapper, queueName, sourceId);
    }

    public QueueItemRow adminStatus(UUID itemId, ItemStatus status, OffsetDateTime availableAt, OffsetDateTime now) {
        return jdbc.queryForObject("""
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
            """, itemMapper, status.name(), availableAt, now, itemId);
    }

    public Optional<QueueItemRow> skipDeadLetteredHead(String queueName, String sourceId, OffsetDateTime now) {
        return jdbc.query("""
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
                  AND head.status NOT IN ('succeeded', 'cancelled', 'skipped')
                ORDER BY head.sequence_no
                LIMIT 1
            )
            AND i.status = 'dead_lettered'
            RETURNING i.*
            """, itemMapper, now, queueName, sourceId).stream().findFirst();
    }

    public List<QueueItemRow> expiredProcessing(OffsetDateTime now, int limit) {
        return jdbc.query("""
            SELECT * FROM queue_item
            WHERE status = 'processing' AND lease_until < ?
            ORDER BY lease_until
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """, itemMapper, now, limit);
    }

    private QueueItemRow mapItem(ResultSet rs, int rowNum) throws SQLException {
        return new QueueItemRow(
            rs.getObject("item_id", UUID.class),
            rs.getString("queue_name"),
            rs.getString("source_id"),
            rs.getLong("sequence_no"),
            rs.getString("item_type"),
            rs.getString("payload_json"),
            rs.getString("headers_json"),
            ItemStatus.valueOf(rs.getString("status")),
            toOffset(rs.getTimestamp("available_at")),
            rs.getString("claimed_by"),
            rs.getObject("lease_id", UUID.class),
            toOffset(rs.getTimestamp("lease_until")),
            rs.getInt("attempt_count"),
            rs.getInt("max_attempts"),
            rs.getString("idempotency_key"),
            rs.getString("last_error_type"),
            rs.getString("last_error_message"),
            rs.getString("result_json"),
            toOffset(rs.getTimestamp("created_at")),
            toOffset(rs.getTimestamp("updated_at"))
        );
    }

    private SourceStateRow mapSource(ResultSet rs, int rowNum) throws SQLException {
        return new SourceStateRow(
            rs.getString("queue_name"),
            rs.getString("source_id"),
            rs.getLong("next_sequence_no"),
            SourceStatus.valueOf(rs.getString("status")),
            rs.getString("leased_by"),
            rs.getObject("lease_id", UUID.class),
            toOffset(rs.getTimestamp("lease_until")),
            toOffset(rs.getTimestamp("created_at")),
            toOffset(rs.getTimestamp("updated_at"))
        );
    }

    private OffsetDateTime toOffset(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant().atOffset(ZoneOffset.UTC);
    }
}
