package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.DeltaFile;
import etl.mashreq.domain.FilePair;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.domain.ProcessingState;
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
    private final GenericXPathXmlParser parser;
    private final DiffService diffService;
    private final DeltaWriterService deltaWriterService;
    private final StateStoreService stateStoreService;

    public void processAll() {
        for (FilePair pair : folderScannerService.findWork()) {
            try {
                processSingle(pair);
            } catch (Exception e) {
                log.error("Processing failed for source={} currentFile={}",
                        pair.getSourceId(),
                        pair.getCurrentFile(),
                        e);
            }
        }
    }

    public void processSingle(FilePair pair) {
        SourceProperties source = properties.getSources().stream()
                .filter(s -> s.getId().equals(pair.getSourceId()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown source: " + pair.getSourceId()));

        log.info("Processing source={} current={} previous={}",
                pair.getSourceId(),
                pair.getCurrentFile().getFileName(),
                pair.getPreviousFile() == null ? "null" : pair.getPreviousFile().getFileName());

        log.info("Parsing current file: {}", pair.getCurrentFile());
        NormalizedSanctionsFile current = parser.parse(pair.getCurrentFile(), source);

        DeltaFile deltaFile;

        if (pair.getPreviousFile() == null) {
            log.info("No previous file found. Generating initial load delta.");
            deltaFile = diffService.initialLoad(current);
        } else {
            log.info("Parsing previous file: {}", pair.getPreviousFile());
            NormalizedSanctionsFile previous = parser.parse(pair.getPreviousFile(), source);

            log.info("Comparing previous vs current for source={}", pair.getSourceId());
            deltaFile = diffService.compare(previous, current);
        }

        log.info("Writing delta file for source={}", pair.getSourceId());
        Path output = deltaWriterService.write(deltaFile);

        log.info("Delta file written successfully: {}", output);

        ProcessingState state = stateStoreService.load();
        state.getLastProcessedFiles().put(pair.getSourceId(), pair.getCurrentFile().getFileName().toString());
        stateStoreService.save(state);

        log.info("State updated for source={} lastProcessed={}",
                pair.getSourceId(),
                pair.getCurrentFile().getFileName().toString());
    }
}