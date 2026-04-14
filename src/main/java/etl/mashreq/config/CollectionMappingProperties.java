package etl.mashreq.config;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class CollectionMappingProperties {

    /**
     * XPath relative to the entry node, returning the repeated child nodes.
     */
    private String itemXpath;

    /**
     * Field name -> XPath relative to each child node.
     */
    private Map<String, String> itemFields = new LinkedHashMap<>();
}
