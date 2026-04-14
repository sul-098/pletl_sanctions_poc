package etl.mashreq.service;

import  etl.mashreq.config.SanctionsProperties;
import  etl.mashreq.config.SourceProperties;
import  etl.mashreq.domain.FilePair;
import  etl.mashreq.domain.ProcessingState;
import  etl.mashreq.util.FileNameUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@Slf4j
public class FolderScannerService {

    private final SanctionsProperties properties;
    private final StateStoreService stateStoreService;

    public List<FilePair> findWork() {
        ProcessingState state = stateStoreService.load();

        return properties.getSources().stream()
                .filter(SourceProperties::isEnabled)
                .map(source -> buildPairForSource(source, state))
                .filter(pair -> pair != null)
                .toList();
    }

    private FilePair buildPairForSource(SourceProperties source, ProcessingState state) {
        Path sourceDir = Path.of(properties.getSharedRootDirectory(), source.getSubDirectory());

        try {
            if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
                log.warn("Source directory missing for source={} path={}", source.getId(), sourceDir);
                return null;
            }

            Pattern pattern = Pattern.compile(source.getFileRegex());

            List<Path> files = Files.list(sourceDir)
                    .filter(Files::isRegularFile)
                    .filter(path -> pattern.matcher(path.getFileName().toString()).matches())
                    .sorted(FileNameUtils.comparatorByEmbeddedTimestamp(source.getFileRegex()))
                    .toList();

            if (files.isEmpty()) {
                log.info("No files found for source={} in path={}", source.getId(), sourceDir);
                return null;
            }

            Path current = files.get(files.size() - 1);
            Path previous = files.size() > 1 ? files.get(files.size() - 2) : null;

            String lastProcessed = state.getLastProcessedFiles().get(source.getId());
            if (current.getFileName().toString().equals(lastProcessed)) {
                log.debug("Latest file already processed for source={} file={}", source.getId(), current.getFileName());
                return null;
            }

            return FilePair.builder()
                    .sourceId(source.getId())
                    .currentFile(current)
                    .previousFile(previous)
                    .build();

        } catch (IOException e) {
            throw new IllegalStateException("Failed scanning source directory for source=" + source.getId(), e);
        }
    }
}