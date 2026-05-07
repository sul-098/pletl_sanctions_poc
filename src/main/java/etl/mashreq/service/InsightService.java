package etl.mashreq.service;

import etl.mashreq.dto.DailyInsightDto;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class InsightService {

    private final JdbcTemplate jdbcTemplate;

    public List<DailyInsightDto> getDailyInsights() {
        return jdbcTemplate.query("""
            SELECT
                processing_date,
                source_code,
                total_comparisons,
                total_added,
                total_removed,
                total_updated,
                total_current_entries
            FROM dbo.VW_SANCTIONS_DAILY_INSIGHTS
            ORDER BY processing_date DESC, source_code
            """,
            (rs, rowNum) -> new DailyInsightDto(
                    rs.getDate("processing_date").toLocalDate(),
                    rs.getString("source_code"),
                    rs.getInt("total_comparisons"),
                    rs.getInt("total_added"),
                    rs.getInt("total_removed"),
                    rs.getInt("total_updated"),
                    rs.getInt("total_current_entries")
            ));
    }
}