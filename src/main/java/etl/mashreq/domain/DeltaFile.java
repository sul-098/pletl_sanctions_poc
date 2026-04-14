package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class DeltaFile {
    private String sourceId;
    private String sourceFile;
    private String previousFile;
    private Instant generatedAt;
    private String comparisonMode;
    private Map<String, Object> summary;
    private List<DeltaEntry> changes;
}
