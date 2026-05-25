package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Builder
public class NormalizedSanctionsFile {
    private String sourceId;
    private String fileName;
    private Instant loadedAt;
    private Map<String, NormalizedEntry> entries;

    /**
     * Number of records in the source file that were skipped during parsing
     * (e.g. missing business key, malformed structure).
     * A non-zero value causes the run to be recorded as PARTIAL_SUCCESS.
     */
    @Builder.Default
    private int skippedCount = 0;

    public static NormalizedSanctionsFile empty(String sourceId, String fileName) {
        return NormalizedSanctionsFile.builder()
                .sourceId(sourceId)
                .fileName(fileName)
                .loadedAt(Instant.now())
                .entries(new LinkedHashMap<>())
                .build();
    }
}