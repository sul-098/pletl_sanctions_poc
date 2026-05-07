package etl.mashreq.controller;

import etl.mashreq.dto.DailyInsightDto;
import etl.mashreq.service.InsightService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/insights")
@RequiredArgsConstructor
public class InsightController {

    private final InsightService insightService;

    @GetMapping("/daily")
    public List<DailyInsightDto> dailyInsights() {
        return insightService.getDailyInsights();
    }
}