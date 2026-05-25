package etl.mashreq.controller;

import etl.mashreq.dto.DailyInsightDto;
import etl.mashreq.dto.InsightSummaryDto;
import etl.mashreq.dto.LatestRunDto;
import etl.mashreq.dto.SourceTrendDto;
import etl.mashreq.service.InsightService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/insights")
@RequiredArgsConstructor
public class InsightController {

    private final InsightService insightService;

    // ── Time-bucketed stats ──────────────────────────────────────────────────

    @GetMapping("/daily")
    public List<DailyInsightDto> daily() {
        return insightService.getDailyInsights();
    }

    @GetMapping("/monthly")
    public List<InsightSummaryDto> monthly() {
        return insightService.getMonthlyInsights();
    }

    @GetMapping("/quarterly")
    public List<InsightSummaryDto> quarterly() {
        return insightService.getQuarterlyInsights();
    }

    @GetMapping("/six-months")
    public List<InsightSummaryDto> sixMonths() {
        return insightService.getSixMonthInsights();
    }

    @GetMapping("/yearly")
    public List<InsightSummaryDto> yearly() {
        return insightService.getYearlyInsights();
    }

    /**
     * Custom date range: /api/insights/range?from=2024-01-01&to=2024-06-30
     */
    @GetMapping("/range")
    public List<InsightSummaryDto> range(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        return insightService.getCustomRangeInsights(from, to);
    }

    // ── Per-source ───────────────────────────────────────────────────────────

    @GetMapping("/source/{sourceCode}")
    public List<DailyInsightDto> bySource(@PathVariable String sourceCode) {
        return insightService.getInsightsBySource(sourceCode);
    }

    // ── Trends ───────────────────────────────────────────────────────────────

    /**
     * Weekly trend buckets: /api/insights/trends?weeks=12
     */
    @GetMapping("/trends")
    public List<SourceTrendDto> trends(@RequestParam(defaultValue = "12") int weeks) {
        return insightService.getWeeklyTrends(weeks);
    }

    // ── Latest comparisons ───────────────────────────────────────────────────

    @GetMapping("/latest")
    public List<LatestRunDto> latestPerSource() {
        return insightService.getLatestRunPerSource();
    }

    @GetMapping("/latest/all")
    public List<LatestRunDto> latestAll(@RequestParam(defaultValue = "20") int limit) {
        return insightService.getLatestRuns(limit);
    }
}
