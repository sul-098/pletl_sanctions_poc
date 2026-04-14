package etl.mashreq.domain;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ProcessingState {
    /**
     * sourceId -> last processed file name
     */
    private Map<String, String> lastProcessedFiles = new HashMap<>();
}
