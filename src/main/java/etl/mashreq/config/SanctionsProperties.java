package etl.mashreq.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@Data
@ConfigurationProperties(prefix = "sanctions")
public class SanctionsProperties {

    @NotBlank
    private String sharedRootDirectory;

    @NotBlank
    private String deltaOutputDirectory;

    @NotBlank
    private String stateFile;

    @NotBlank
    private String errorDirectory;

    @NotBlank
    private String archiveDirectory;

    private boolean schedulerEnabled = true;
    private String pollCron = "0 */5 * * * *";
    private boolean prettyJson = true;

    @Valid
    private List<SourceProperties> sources = new ArrayList<>();
}