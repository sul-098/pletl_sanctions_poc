package etl.mashreq.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import etl.mashreq.domain.ChangeType;
import etl.mashreq.domain.DeltaEntry;
import etl.mashreq.domain.DeltaFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabasePersistenceService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Transactional
    public void persistDelta(DeltaFile deltaFile) {
        if (deltaFile == null) return;

        Long runId = insertRun(deltaFile);

        if (deltaFile.getChanges() == null) return;

        for (DeltaEntry change : deltaFile.getChanges()) {
            insertDeltaChange(runId, deltaFile.getSourceId(), change);
            updateCurrentEntry(deltaFile, change);
        }

        log.info("Persisted delta to MySQL source={} runId={}", deltaFile.getSourceId(), runId);
    }

    private Long insertRun(DeltaFile deltaFile) {
        Map<String, Object> summary = deltaFile.getSummary();

        jdbcTemplate.update("""
            INSERT INTO sanctions_file_run
            (source_code, current_file_name, previous_file_name, generated_at,
             added_count, removed_count, updated_count, total_current_entries)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            deltaFile.getSourceId(),
            deltaFile.getSourceFile(),
            deltaFile.getPreviousFile(),
            Timestamp.from(deltaFile.getGeneratedAt() == null ? Instant.now() : deltaFile.getGeneratedAt()),
            getInt(summary, "added"),
            getInt(summary, "removed"),
            getInt(summary, "updated"),
            getNullableInt(summary, "totalCurrentEntries")
        );

        return jdbcTemplate.queryForObject("SELECT LAST_INSERT_ID()", Long.class);
    }

    private void insertDeltaChange(Long runId, String sourceCode, DeltaEntry change) {
        jdbcTemplate.update("""
            INSERT INTO sanctions_delta_change
            (run_id, source_code, change_type, business_key,
             previous_payload_json, current_payload_json, field_changes_json)
            VALUES (?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON))
            """,
            runId,
            sourceCode,
            change.getChangeType() == null ? null : change.getChangeType().name(),
            change.getBusinessKey(),
            toJson(change.getPrevious()),
            toJson(change.getCurrent()),
            toJson(change.getFieldChanges())
        );
    }

    private void updateCurrentEntry(DeltaFile deltaFile, DeltaEntry change) {
        if (change.getChangeType() == null) return;

        if (change.getChangeType() == ChangeType.REMOVED) {
            jdbcTemplate.update("""
                UPDATE sanctions_entry_current
                SET is_active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE source_code = ? AND business_key = ?
                """,
                deltaFile.getSourceId(),
                change.getBusinessKey()
            );
            return;
        }

        Map<String, Object> current = change.getCurrent();
        if (current == null) return;

        String sourceRef = value(current.get("sourceRef"));
        String primaryName = value(current.get("primaryName"));
        String entityType = value(current.get("entityType"));
        String payloadJson = toJson(current);
        String canonicalHash = Integer.toHexString(payloadJson.hashCode());

        jdbcTemplate.update("""
            INSERT INTO sanctions_entry_current
            (source_code, business_key, source_ref, primary_name, entity_type,
             canonical_hash, payload_json, is_active, last_seen_file, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), TRUE, ?, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                source_ref = VALUES(source_ref),
                primary_name = VALUES(primary_name),
                entity_type = VALUES(entity_type),
                canonical_hash = VALUES(canonical_hash),
                payload_json = VALUES(payload_json),
                is_active = TRUE,
                last_seen_file = VALUES(last_seen_file),
                last_seen_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            """,
            deltaFile.getSourceId(),
            change.getBusinessKey(),
            sourceRef,
            primaryName,
            entityType,
            canonicalHash,
            payloadJson,
            deltaFile.getSourceFile()
        );
    }

    private String toJson(Object value) {
        if (value == null) return null;
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to convert value to JSON", e);
        }
    }

    private int getInt(Map<String, Object> map, String key) {
        Integer value = getNullableInt(map, key);
        return value == null ? 0 : value;
    }

    private Integer getNullableInt(Map<String, Object> map, String key) {
        if (map == null || map.get(key) == null) return null;
        Object value = map.get(key);
        if (value instanceof Number number) return number.intValue();
        return Integer.parseInt(value.toString());
    }

    private String value(Object value) {
        if (value == null) return null;
        String s = value.toString().trim();
        return s.isEmpty() ? null : s;
    }
}