package etl.mashreq.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class SourceProperties {

    @NotBlank
    private String id;

    private boolean enabled = true;

    @NotBlank
    private String subDirectory;

    @NotBlank
    private String fileRegex;

    @NotBlank
    private String entryXpath;

    @Valid
    private UniqueKeyProperties uniqueKey = new UniqueKeyProperties();

    private Map<String, String> fields = new LinkedHashMap<>();

    @Valid
    private Map<String, CollectionMappingProperties> collections = new LinkedHashMap<>();
}