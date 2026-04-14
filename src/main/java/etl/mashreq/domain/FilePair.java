package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

import java.nio.file.Path;

@Data
@Builder
public class FilePair {
    private String sourceId;
    private Path currentFile;
    private Path previousFile; // can be null for first-time load
}
