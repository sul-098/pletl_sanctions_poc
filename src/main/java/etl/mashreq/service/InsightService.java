package etl.mashreq.service;

import etl.mashreq.dto.DailyInsightDto;
import etl.mashreq.dto.InsightSummaryDto;
import etl.mashreq.dto.LatestRunDto;
import etl.mashreq.dto.SourceTrendDto;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
@RequiredArgsConstructor
@SuppressWarnings("null")
public class InsightService {

    private final JdbcTemplate jdbcTemplate;

    // ── Daily ────────────────────────────────────────────────────────────────

    public List<DailyInsightDto> getDailyInsights() {
        return jdbcTemplate.query("""
            SELECT
                CAST(started_at AS DATE)           AS processing_date,
                source_code,
                COUNT(*)                           AS total_comparisons,
                SUM(added_count)                   AS total_added,
                SUM(removed_count)                 AS total_removed,
                SUM(updated_count)                 AS total_updated,
                MAX(total_current_entries)         AS total_current_entries
            FROM sanctions_run
            WHERE run_status IN ('COMPLETED', 'PARTIAL_SUCCESS')
            GROUP BY CAST(started_at AS DATE), source_code
            ORDER BY processing_date DESC, source_code
            """,
            dailyRowMapper());
    }

    // ── Rolling-window helpers ───────────────────────────────────────────────

    public List<InsightSummaryDto> getMonthlyInsights() {
        return getSummaryForRange(LocalDate.now().minusMonths(1), LocalDate.now());
    }

    public List<InsightSummaryDto> getQuarterlyInsights() {
        return getSummaryForRange(LocalDate.now().minusMonths(3), LocalDate.now());
    }

    public List<InsightSummaryDto> getSixMonthInsights() {
        return getSummaryForRange(LocalDate.now().minusMonths(6), LocalDate.now());
    }

    public List<InsightSummaryDto> getYearlyInsights() {
        return getSummaryForRange(LocalDate.now().minusYears(1), LocalDate.now());
    }

    public List<InsightSummaryDto> getCustomRangeInsights(LocalDate from, LocalDate to) {
        return getSummaryForRange(from, to);
    }

    private List<InsightSummaryDto> getSummaryForRange(LocalDate from, LocalDate to) {
        return jdbcTemplate.query("""
            SELECT
                CAST(? AS DATE)                    AS period_start,
                CAST(? AS DATE)                    AS period_end,
                source_code,
                COUNT(*)                           AS total_runs,
                SUM(added_count)                   AS total_added,
                SUM(removed_count)                 AS total_removed,
                SUM(updated_count)                 AS total_updated,
                MAX(total_current_entries)         AS latest_current_entries
            FROM sanctions_run
            WHERE run_status IN ('COMPLETED', 'PARTIAL_SUCCESS')
              AND CAST(started_at AS DATE) BETWEEN ? AND ?
            GROUP BY source_code
            ORDER BY source_code
            """,
            summaryRowMapper(),
            from, to, from, to);
    }

    // ── Per-source ───────────────────────────────────────────────────────────

    public List<DailyInsightDto> getInsightsBySource(String sourceCode) {
        return jdbcTemplate.query("""
            SELECT
                CAST(started_at AS DATE)           AS processing_date,
                source_code,
                COUNT(*)                           AS total_comparisons,
                SUM(added_count)                   AS total_added,
                SUM(removed_count)                 AS total_removed,
                SUM(updated_count)                 AS total_updated,
                MAX(total_current_entries)         AS total_current_entries
            FROM sanctions_run
            WHERE run_status IN ('COMPLETED', 'PARTIAL_SUCCESS')
              AND source_code = ?
            GROUP BY CAST(started_at AS DATE), source_code
            ORDER BY processing_date DESC
            """,
            dailyRowMapper(),
            sourceCode);
    }

    // ── Weekly trends (all sources) ──────────────────────────────────────────

    public List<SourceTrendDto> getWeeklyTrends(int weeks) {
        LocalDate from = LocalDate.now().minusWeeks(weeks);
        return jdbcTemplate.query("""
            SELECT
                CAST(DATEADD(DAY,
                    -(DATEPART(WEEKDAY, started_at) - 1),
                    CAST(started_at AS DATE)) AS DATE)  AS week_start,
                source_code,
                SUM(added_count)                        AS total_added,
                SUM(removed_count)                      AS total_removed,
                SUM(updated_count)                      AS total_updated
            FROM sanctions_run
            WHERE run_status IN ('COMPLETED', 'PARTIAL_SUCCESS')
              AND CAST(started_at AS DATE) >= ?
            GROUP BY
                DATEADD(DAY,
                    -(DATEPART(WEEKDAY, started_at) - 1),
                    CAST(started_at AS DATE)),
                source_code
            ORDER BY week_start DESC, source_code
            """,
            (rs, rowNum) -> new SourceTrendDto(
                    rs.getDate("week_start").toLocalDate(),
                    rs.getString("source_code"),
                    rs.getInt("total_added"),
                    rs.getInt("total_removed"),
                    rs.getInt("total_updated")
            ),
            from);
    }

    // ── Latest runs per source ───────────────────────────────────────────────

    public List<LatestRunDto> getLatestRunPerSource() {
        return jdbcTemplate.query("""
            SELECT r.id, r.source_code, r.current_file_name, r.previous_file_name,
                   r.run_type, r.triggered_by, r.run_status,
                   r.started_at, r.completed_at, r.duration_ms,
                   r.added_count, r.removed_count, r.updated_count, r.total_current_entries
            FROM sanctions_run r
            INNER JOIN (
                SELECT source_code, MAX(id) AS max_id
                FROM sanctions_run
                GROUP BY source_code
            ) latest ON r.source_code = latest.source_code AND r.id = latest.max_id
            ORDER BY r.source_code
            """,
            latestRunRowMapper());
    }

    public List<LatestRunDto> getLatestRuns(int limit) {
        return jdbcTemplate.query("""
            SELECT TOP (?) id, source_code, current_file_name, previous_file_name,
                   run_type, triggered_by, run_status,
                   started_at, completed_at, duration_ms,
                   added_count, removed_count, updated_count, total_current_entries
            FROM sanctions_run
            ORDER BY started_at DESC
            """,
            latestRunRowMapper(),
            limit);
    }

    // ── Row mappers ──────────────────────────────────────────────────────────

    private RowMapper<DailyInsightDto> dailyRowMapper() {
        return (rs, rowNum) -> new DailyInsightDto(
                rs.getDate("processing_date").toLocalDate(),
                rs.getString("source_code"),
                rs.getInt("total_comparisons"),
                rs.getInt("total_added"),
                rs.getInt("total_removed"),
                rs.getInt("total_updated"),
                rs.getInt("total_current_entries")
        );
    }

    private RowMapper<InsightSummaryDto> summaryRowMapper() {
        return (rs, rowNum) -> new InsightSummaryDto(
                rs.getDate("period_start").toLocalDate(),
                rs.getDate("period_end").toLocalDate(),
                rs.getString("source_code"),
                rs.getInt("total_runs"),
                rs.getInt("total_added"),
                rs.getInt("total_removed"),
                rs.getInt("total_updated"),
                rs.getInt("latest_current_entries")
        );
    }

    private RowMapper<LatestRunDto> latestRunRowMapper() {
        return (rs, rowNum) -> new LatestRunDto(
                rs.getLong("id"),
                rs.getString("source_code"),
                rs.getString("current_file_name"),
                rs.getString("previous_file_name"),
                rs.getString("run_type"),
                rs.getString("triggered_by"),
                rs.getString("run_status"),
                rs.getTimestamp("started_at").toLocalDateTime(),
                rs.getTimestamp("completed_at") != null
                        ? rs.getTimestamp("completed_at").toLocalDateTime() : null,
                rs.getObject("duration_ms") != null ? rs.getLong("duration_ms") : null,
                (Integer) rs.getObject("added_count"),
                (Integer) rs.getObject("removed_count"),
                (Integer) rs.getObject("updated_count"),
                (Integer) rs.getObject("total_current_entries")
        );
    }
}
