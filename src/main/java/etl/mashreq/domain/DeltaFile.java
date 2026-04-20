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
    private Map<String, Object> summary;
    private List<DeltaEntry> changes;
}