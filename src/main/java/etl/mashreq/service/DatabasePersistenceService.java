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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabasePersistenceService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Transactional
    public Long startRun(String sourceCode,
                         String currentFileName,
                         String previousFileName,
                         String runType,
                         String triggeredBy,
                         String parserUsed,
                         String storageMode) {

        String mode = storageMode == null ? "LOCAL" : storageMode;
        String type = runType == null ? "SCHEDULED" : runType;

        Long runId = jdbcTemplate.queryForObject(
            "INSERT INTO SANCTIONS_RUN" +
            "    (source_code, current_file_name, previous_file_name," +
            "     parser_used, storage_mode," +
            "     run_type, triggered_by, run_status, started_at)" +
            " OUTPUT INSERTED.id" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'IN_PROGRESS', GETUTCDATE())",
            Long.class,
            sourceCode, currentFileName, previousFileName,
            parserUsed, mode,
            type, triggeredBy
        );

        log.info("Run started: runId={} source={} type={} parser={} file={}",
                runId, sourceCode, type, parserUsed, currentFileName);

        return runId;
    }

    @Transactional
    public void completeRun(Long runId, DeltaFile deltaFile) {
        List<DeltaEntry> changes = deltaFile.getChanges();

        if (changes != null && !changes.isEmpty()) {
            List<Object[]> deltaLogRows = new ArrayList<>(changes.size());

            for (DeltaEntry change : changes) {
                Long entityId = upsertEntity(runId, deltaFile, change);

                deltaLogRows.add(new Object[]{
                    runId,
                    deltaFile.getSourceId(),
                    change.getChangeType() == null ? null : change.getChangeType().name(),
                    change.getBusinessKey(),
                    entityId,
                    toJson(change.getPrevious()),
                    toJson(change.getCurrent()),
                    toJson(change.getFieldChanges())
                });
            }

            jdbcTemplate.batchUpdate(
                "INSERT INTO SANCTIONS_DELTA_LOG" +
                "    (run_id, source_code, change_type, business_key," +
                "     entity_id, previous_payload_json, current_payload_json, field_changes_json)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                deltaLogRows);
        }

        Map<String, Object> summary = deltaFile.getSummary();
        int skipped = deltaFile.getSkippedCount();
        String finalStatus = skipped > 0 ? "PARTIAL_SUCCESS" : "COMPLETED";

        jdbcTemplate.update(
            "UPDATE SANCTIONS_RUN SET" +
            "    run_status            = ?," +
            "    completed_at          = GETUTCDATE()," +
            "    added_count           = ?," +
            "    removed_count         = ?," +
            "    updated_count         = ?," +
            "    total_current_entries = ?," +
            "    skipped_count         = ?" +
            " WHERE id = ?",
            finalStatus,
            getInt(summary, "added"),
            getInt(summary, "removed"),
            getInt(summary, "updated"),
            getNullableInt(summary, "totalCurrentEntries"),
            skipped,
            runId
        );

        log.info("Run completed: runId={} status={} added={} removed={} updated={} skipped={}",
                runId, finalStatus,
                getInt(summary, "added"),
                getInt(summary, "removed"),
                getInt(summary, "updated"),
                skipped);
    }

    @Transactional
    public void failRun(Long runId, String errorMessage) {
        jdbcTemplate.update(
            "UPDATE SANCTIONS_RUN SET" +
            "    run_status    = 'FAILED'," +
            "    completed_at  = GETUTCDATE()," +
            "    error_message = ?" +
            " WHERE id = ?",
            truncate(errorMessage, 2000),
            runId
        );
        log.error("Run failed: runId={} error={}", runId, errorMessage);
    }

    private Long upsertEntity(Long runId, DeltaFile deltaFile, DeltaEntry change) {
        if (change.getChangeType() == null) return null;

        if (change.getChangeType() == ChangeType.REMOVED) {
            jdbcTemplate.update(
                "UPDATE SANCTIONS_ENTITY" +
                "   SET is_active   = 0," +
                "       change_type = 'REMOVED'," +
                "       updated_at  = GETUTCDATE()" +
                " WHERE source_code = ? AND business_key = ?",
                deltaFile.getSourceId(),
                change.getBusinessKey()
            );
            return lookupEntityId(deltaFile.getSourceId(), change.getBusinessKey());
        }

        Map<String, Object> current = change.getCurrent();
        if (current == null) return null;

        String sourceRef   = strVal(current.get("sourceRef"));
        String primaryName = strVal(current.get("primaryName"));
        String entityType  = strVal(current.get("entityType"));
        String remarks     = strVal(current.get("remarks"));

        String nationality = strVal(current.get("nationality"));
        if (nationality == null) {
            nationality = extractFirstFromList(current.get("nationalities"),
                    "country", "countryDescription", "value");
        }

        String dateOfBirth = strVal(current.get("dateOfBirth"));
        if (dateOfBirth == null) {
            dateOfBirth = extractFirstFromList(current.get("datesOfBirth"),
                    "date", "dateOfBirth", "birthdate", "value");
        }

        String payloadJson    = toJson(current);
        String canonicalHash  = Integer.toHexString(payloadJson == null ? 0 : payloadJson.hashCode());
        String changeTypeName = change.getChangeType().name();

        jdbcTemplate.update(
            "MERGE INTO SANCTIONS_ENTITY WITH (HOLDLOCK) AS target" +
            "  USING (SELECT ? AS source_code, ? AS business_key) AS src" +
            "     ON target.source_code = src.source_code" +
            "    AND target.business_key = src.business_key" +
            "  WHEN MATCHED THEN UPDATE SET" +
            "    source_ref       = ?," +
            "    primary_name     = ?," +
            "    entity_type      = ?," +
            "    nationality      = ?," +
            "    date_of_birth    = ?," +
            "    remarks          = ?," +
            "    canonical_hash   = ?," +
            "    change_type      = ?," +
            "    is_active        = 1," +
            "    last_seen_run_id = ?," +
            "    last_seen_at     = GETUTCDATE()," +
            "    updated_at       = GETUTCDATE()" +
            "  WHEN NOT MATCHED THEN INSERT" +
            "    (source_code, business_key, source_ref, primary_name," +
            "     entity_type, nationality, date_of_birth, remarks," +
            "     canonical_hash, change_type, is_active, last_seen_run_id, last_seen_at)" +
            "  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, GETUTCDATE());",
            deltaFile.getSourceId(), change.getBusinessKey(),
            sourceRef, primaryName, entityType, nationality, dateOfBirth, remarks,
            canonicalHash, changeTypeName, runId,
            deltaFile.getSourceId(), change.getBusinessKey(),
            sourceRef, primaryName, entityType, nationality, dateOfBirth, remarks,
            canonicalHash, changeTypeName, runId
        );

        return lookupEntityId(deltaFile.getSourceId(), change.getBusinessKey());
    }

    private Long lookupEntityId(String sourceCode, String businessKey) {
        List<Long> ids = jdbcTemplate.queryForList(
            "SELECT id FROM SANCTIONS_ENTITY WHERE source_code = ? AND business_key = ?",
            Long.class, sourceCode, businessKey);
        return ids.isEmpty() ? null : ids.get(0);
    }

    private String toJson(Object value) {
        if (value == null) return null;
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("JSON serialization failed", e);
        }
    }

    private int getInt(Map<String, Object> map, String key) {
        Integer v = getNullableInt(map, key);
        return v == null ? 0 : v;
    }

    private Integer getNullableInt(Map<String, Object> map, String key) {
        if (map == null || map.get(key) == null) return null;
        Object v = map.get(key);
        return v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
    }

    private String strVal(Object value) {
        if (value == null) return null;
        String s = value.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private String extractFirstFromList(Object listObj, String... keys) {
        if (!(listObj instanceof List<?> list) || list.isEmpty()) return null;
        Object first = list.get(0);
        if (!(first instanceof Map<?, ?> map)) return null;
        for (String key : keys) {
            Object val = map.get(key);
            String s = val == null ? null : val.toString().trim();
            if (s != null && !s.isEmpty()) return s;
        }
        return null;
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen);
    }
}
