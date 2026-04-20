package etl.mashreq.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaEntry {
    private ChangeType changeType;
    private String businessKey;
    private Map<String, Object> previous;
    private Map<String, Object> current;
    private List<FieldChange> fieldChanges;
}