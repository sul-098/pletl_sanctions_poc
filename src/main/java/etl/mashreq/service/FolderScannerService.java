package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.FilePair;
import etl.mashreq.util.FileNameUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@Slf4j
public class FolderScannerService {

    private final SanctionsProperties properties;

    public List<FilePair> findWork() {
        return properties.getSources().stream()
                .filter(SourceProperties::isEnabled)
                .map(this::buildPairForSource)
                .filter(pair -> pair != null)
                .toList();
    }

    private FilePair buildPairForSource(SourceProperties source) {
        try {
            Path sourceRoot = Path.of(properties.getSharedRootDirectory(), source.getSubDirectory());
            Path newDir = sourceRoot.resolve("new");
            Path oldDir = sourceRoot.resolve("old");

            Files.createDirectories(newDir);
            Files.createDirectories(oldDir);
            Files.createDirectories(sourceRoot.resolve("archive"));

            Pattern pattern = Pattern.compile(source.getFileRegex());

            Path currentFile = latestMatchingFile(newDir, pattern, source);
            if (currentFile == null) {
                log.info("No new file found for source={} path={}", source.getId(), newDir);
                return null;
            }

            Path previousFile = latestMatchingFile(oldDir, pattern, source);

            return FilePair.builder()
                    .sourceId(source.getId())
                    .currentFile(currentFile)
                    .previousFile(previousFile)
                    .build();

        } catch (Exception e) {
            throw new IllegalStateException("Failed scanning lifecycle folders for source=" + source.getId(), e);
        }
    }

    private Path latestMatchingFile(Path dir, Pattern pattern, SourceProperties source) throws Exception {
        if (!Files.exists(dir)) {
            return null;
        }

        List<Path> files;
        try (var stream = Files.list(dir)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> pattern.matcher(path.getFileName().toString()).matches())
                    .sorted(FileNameUtils.comparatorByEmbeddedTimestamp(source.getFileRegex()))
                    .toList();
        }

        if (files.isEmpty()) {
            return null;
        }

        return files.get(files.size() - 1);
    }
}