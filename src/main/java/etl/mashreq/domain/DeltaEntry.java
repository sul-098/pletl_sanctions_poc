package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class DeltaEntry {
    private ChangeType changeType;
    private String businessKey;
    private Map<String, Object> previous;
    private Map<String, Object> current;
    private List<FieldChange> fieldChanges;
}
