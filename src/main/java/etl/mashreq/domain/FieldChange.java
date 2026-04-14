package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FieldChange {
    private String fieldPath;
    private Object oldValue;
    private Object newValue;
}
