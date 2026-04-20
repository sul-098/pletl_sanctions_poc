package etl.mashreq.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldChange {
    private String fieldPath;
    private Object oldValue;
    private Object newValue;
}