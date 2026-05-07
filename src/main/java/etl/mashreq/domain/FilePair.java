package etl.mashreq.domain;

import lombok.Builder;
import lombok.Data;

import java.nio.file.Path;

@Data
@Builder
public class FilePair {
    private String sourceId;
    private Path currentFile;   // file from new/
    private Path previousFile;  // file from old/
}