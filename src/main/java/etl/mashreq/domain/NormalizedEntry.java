package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Builder
public class NormalizedEntry {
    private String businessKey;
    private String sourceId;
    private Map<String, Object> attributes;
    private String canonicalHash;

    public static NormalizedEntry empty() {
        return NormalizedEntry.builder()
                .attributes(new LinkedHashMap<>())
                .build();
    }
}