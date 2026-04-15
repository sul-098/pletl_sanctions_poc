package etl.mashreq.parser;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedSanctionsFile;

import java.nio.file.Path;

public interface SanctionsSourceParser {
    boolean supports(String sourceId);
    NormalizedSanctionsFile parse(Path file, SourceProperties source);
}