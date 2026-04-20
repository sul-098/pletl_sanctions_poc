package etl.mashreq.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.domain.DeltaFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeltaDashboardService {

    private final SanctionsProperties properties;
    private final ObjectMapper objectMapper;

    public Optional<DeltaFile> loadLatestDelta(String sourceId) {
        Path sourceDeltaDir = Path.of(properties.getDeltaOutputDirectory(), sourceId);

        if (!Files.exists(sourceDeltaDir) || !Files.isDirectory(sourceDeltaDir)) {
            log.warn("Delta directory not found for source={} path={}", sourceId, sourceDeltaDir);
            return Optional.empty();
        }

        try (Stream<Path> stream = Files.list(sourceDeltaDir)) {
            Optional<Path> latestFile = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .max(Comparator.comparing(path -> path.getFileName().toString()));

            if (latestFile.isEmpty()) {
                log.warn("No delta file found for source={} path={}", sourceId, sourceDeltaDir);
                return Optional.empty();
            }

            log.info("Loading delta file for source={} file={}", sourceId, latestFile.get().getFileName());

            DeltaFile deltaFile = objectMapper.readValue(latestFile.get().toFile(), DeltaFile.class);

            log.info("Loaded delta for source={} changesCount={}",
                    sourceId,
                    deltaFile.getChanges() == null ? 0 : deltaFile.getChanges().size());

            return Optional.of(deltaFile);

        } catch (IOException e) {
            log.error("Failed loading delta for source={} path={}", sourceId, sourceDeltaDir, e);
            throw new IllegalStateException("Failed to load latest delta for source: " + sourceId, e);
        }
    }
}