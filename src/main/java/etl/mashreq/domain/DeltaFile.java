package etl.mashreq.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaFile {
    private String sourceId;
    private String sourceFile;
    private String previousFile;
    private Instant generatedAt;
    private String comparisonMode;

    // Run lifecycle fields — set by orchestrators before persistence
    private String runType;       // SCHEDULED | MANUAL
    private String triggeredBy;   // null for scheduled; client IP for manual
    private String parserUsed;    // parser class name e.g. OfacStaxParser
    private String storageMode;   // LOCAL | MANUAL_UPLOAD | AZURE_BLOB (default: LOCAL)

    // Records skipped by the parser (missing key, malformed row, etc.)
    // Non-zero → run recorded as PARTIAL_SUCCESS instead of COMPLETED
    private int skippedCount;

    private Map<String, Object> summary;
    private List<DeltaEntry> changes;
}