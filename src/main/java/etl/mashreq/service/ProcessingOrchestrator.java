package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.DeltaFile;
import etl.mashreq.domain.FilePair;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.parser.SanctionsParserFactory;
import etl.mashreq.parser.SanctionsSourceParser;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Path;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProcessingOrchestrator {

    private final SanctionsProperties properties;
    private final FolderScannerService folderScannerService;
    private final SanctionsParserFactory parserFactory;
    private final DiffService diffService;
    private final DeltaWriterService deltaWriterService;
    private final DatabasePersistenceService databasePersistenceService;
    private final FileLifecycleService fileLifecycleService;

    // StateStoreService removed — last-processed state is now derived
    // exclusively from sanctions_run (DB is the single source of truth).

    public void processAll() {
        for (FilePair pair : folderScannerService.findWork()) {
            try {
                processSingle(pair);
            } catch (Exception e) {
                log.error("Processing failed for source={} currentFile={}",
                        pair.getSourceId(), pair.getCurrentFile(), e);
            }
        }
    }

    public void processSingle(FilePair pair) {
        SourceProperties source = properties.getSources().stream()
                .filter(s -> s.getId().equals(pair.getSourceId()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown source: " + pair.getSourceId()));

        SanctionsSourceParser parser = parserFactory.getParser(pair.getSourceId());

        String currentFileName  = pair.getCurrentFile().getFileName().toString();
        String previousFileName = pair.getPreviousFile() == null
                ? null : pair.getPreviousFile().getFileName().toString();

        Long runId = databasePersistenceService.startRun(
                pair.getSourceId(),
                currentFileName,
                previousFileName,
                "SCHEDULED",
                null,
                parser.getClass().getSimpleName(),
                "LOCAL"
        );

        try {
            log.info("Parsing current file: source={} parser={} file={}",
                    pair.getSourceId(), parser.getClass().getSimpleName(), currentFileName);
            NormalizedSanctionsFile current = parser.parse(pair.getCurrentFile(), source);

            DeltaFile deltaFile;

            if (pair.getPreviousFile() == null) {
                log.info("No previous file — generating initial load delta: source={}",
                        pair.getSourceId());
                deltaFile = diffService.initialLoad(current);
            } else {
                log.info("Parsing previous file: source={} file={}", pair.getSourceId(), previousFileName);
                NormalizedSanctionsFile previous = parser.parse(pair.getPreviousFile(), source);
                deltaFile = diffService.compare(previous, current);
            }

            deltaFile.setRunType("SCHEDULED");
            deltaFile.setTriggeredBy(null);
            deltaFile.setParserUsed(parser.getClass().getSimpleName());
            deltaFile.setStorageMode("LOCAL");
            deltaFile.setSkippedCount(current.getSkippedCount());

            log.info("Writing delta file: source={}", pair.getSourceId());
            Path output = deltaWriterService.write(deltaFile);
            log.info("Delta file written: {}", output);

            // Updates run to COMPLETED, or PARTIAL_SUCCESS if skippedCount > 0
            databasePersistenceService.completeRun(runId, deltaFile);

            log.info("Rotating lifecycle files: source={}", pair.getSourceId());
            fileLifecycleService.rotateAfterSuccessfulProcessing(pair);

            log.info("Processing complete: source={} runId={} added={} removed={} updated={} skipped={}",
                    pair.getSourceId(), runId,
                    deltaFile.getSummary().get("added"),
                    deltaFile.getSummary().get("removed"),
                    deltaFile.getSummary().get("updated"),
                    current.getSkippedCount());

        } catch (Exception e) {
            databasePersistenceService.failRun(runId, e.getMessage());
            throw new IllegalStateException(
                    "Processing failed for source=" + pair.getSourceId(), e);
        }
    }
}
