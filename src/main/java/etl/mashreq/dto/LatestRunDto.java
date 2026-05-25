package etl.mashreq.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class LatestRunDto {
    private Long runId;
    private String sourceCode;
    private String currentFileName;
    private String previousFileName;
    private String runType;
    private String triggeredBy;
    private String runStatus;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private Long durationMs;
    private Integer addedCount;
    private Integer removedCount;
    private Integer updatedCount;
    private Integer totalCurrentEntries;
}
