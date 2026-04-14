package etl.mashreq.config;

import lombok.Data;

@Data
public class UniqueKeyProperties {

    /**
     * Supported values:
     * xpath, composite
     */
    private String type = "xpath";

    /**
     * Used when type = xpath
     */
    private String xpath;
}