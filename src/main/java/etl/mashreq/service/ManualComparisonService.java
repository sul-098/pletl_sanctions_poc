package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.DeltaFile;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.parser.SanctionsParserFactory;
import etl.mashreq.parser.SanctionsSourceParser;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Files;
import java.nio.file.Path;

@Service
@RequiredArgsConstructor
@Slf4j
public class ManualComparisonService {

    private final SanctionsProperties properties;
    private final SanctionsParserFactory parserFactory;
    private final DiffService diffService;
    private final DeltaWriterService deltaWriterService;
    private final DatabasePersistenceService databasePersistenceService;

    /**
     * Accepts two uploaded files for the given source, compares them,
     * persists the delta to the database, writes a delta JSON file, and
     * returns the DeltaFile for the API response.
     *
     * Run is inserted as IN_PROGRESS at the start. If an exception is thrown,
     * the run is marked FAILED before rethrowing.
     */
    public DeltaFile compare(String sourceId,
                             MultipartFile previousFile,
                             MultipartFile currentFile,
                             String triggeredBy) {

        SourceProperties source = resolveSource(sourceId);
        SanctionsSourceParser parser = parserFactory.getParser(sourceId);

        Path tempDir = null;
        Long runId   = null;

        try {
            tempDir = Files.createTempDirectory("manual-compare-" + sourceId + "-");

            Path prevPath = stageUpload(tempDir, "previous-", previousFile);
            Path currPath = stageUpload(tempDir, "current-",  currentFile);

            String originalCurrentName  = currentFile.getOriginalFilename();
            String originalPreviousName = previousFile.getOriginalFilename();

            log.info("Manual comparison: source={} previous={} current={} triggeredBy={}",
                    sourceId, originalPreviousName, originalCurrentName, triggeredBy);

            runId = databasePersistenceService.startRun(
                    sourceId,
                    originalCurrentName  != null ? originalCurrentName  : currPath.getFileName().toString(),
                    originalPreviousName != null ? originalPreviousName : prevPath.getFileName().toString(),
                    "MANUAL",
                    triggeredBy,
                    parser.getClass().getSimpleName(),
                    "MANUAL_UPLOAD"
            );

            NormalizedSanctionsFile previous = parser.parse(prevPath, source);
            NormalizedSanctionsFile current  = parser.parse(currPath,  source);

            DeltaFile delta = diffService.compare(previous, current);
            delta.setRunType("MANUAL");
            delta.setTriggeredBy(triggeredBy);
            delta.setParserUsed(parser.getClass().getSimpleName());
            delta.setStorageMode("MANUAL_UPLOAD");
            delta.setSkippedCount(current.getSkippedCount());

            deltaWriterService.write(delta);
            databasePersistenceService.completeRun(runId, delta);

            log.info("Manual comparison complete: source={} runId={} added={} removed={} updated={} skipped={}",
                    sourceId, runId,
                    delta.getSummary().get("added"),
                    delta.getSummary().get("removed"),
                    delta.getSummary().get("updated"),
                    current.getSkippedCount());

            return delta;

        } catch (Exception e) {
            if (runId != null) {
                databasePersistenceService.failRun(runId, e.getMessage());
            }
            throw new IllegalStateException(
                    "Manual comparison failed for source=" + sourceId, e);
        } finally {
            deleteTempDir(tempDir);
        }
    }

    /**
     * Compare two files that already exist on the server filesystem.
     * Useful for re-running comparisons on archived files during POC validation.
     */
    public DeltaFile compareByPath(String sourceId, Path previousPath, Path currentPath) {
        SourceProperties source = resolveSource(sourceId);
        SanctionsSourceParser parser = parserFactory.getParser(sourceId);

        Long runId = databasePersistenceService.startRun(
                sourceId,
                currentPath.getFileName().toString(),
                previousPath.getFileName().toString(),
                "MANUAL",
                null,
                parser.getClass().getSimpleName(),
                "LOCAL"
        );

        try {
            NormalizedSanctionsFile previous = parser.parse(previousPath, source);
            NormalizedSanctionsFile current  = parser.parse(currentPath,  source);

            DeltaFile delta = diffService.compare(previous, current);
            delta.setRunType("MANUAL");
            delta.setParserUsed(parser.getClass().getSimpleName());
            delta.setStorageMode("LOCAL");
            delta.setSkippedCount(current.getSkippedCount());

            deltaWriterService.write(delta);
            databasePersistenceService.completeRun(runId, delta);

            return delta;

        } catch (Exception e) {
            databasePersistenceService.failRun(runId, e.getMessage());
            throw new IllegalStateException(
                    "Path-based comparison failed for source=" + sourceId, e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private SourceProperties resolveSource(String sourceId) {
        return properties.getSources().stream()
                .filter(s -> s.getId().equalsIgnoreCase(sourceId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown source: " + sourceId));
    }

    private Path stageUpload(Path dir, String prefix, MultipartFile upload) throws Exception {
        String originalName = upload.getOriginalFilename();
        String ext = (originalName != null && originalName.contains("."))
                ? originalName.substring(originalName.lastIndexOf('.'))
                : "";
        Path staged = dir.resolve(prefix + System.currentTimeMillis() + ext);
        upload.transferTo(staged);
        return staged;
    }

    private void deleteTempDir(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        } catch (Exception e) {
            log.warn("Could not clean up temp directory: {}", dir, e);
        }
    }
}
