package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.domain.FilePair;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileLifecycleService {

    private final SanctionsProperties properties;

    private static final DateTimeFormatter ARCHIVE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public void rotateAfterSuccessfulProcessing(FilePair pair) {
        try {
            Path sourceRoot = Path.of(properties.getSharedRootDirectory(), pair.getSourceId());
            Path oldDir = sourceRoot.resolve("old");
            Path archiveDir = sourceRoot.resolve("archive");

            Files.createDirectories(oldDir);
            Files.createDirectories(archiveDir);

            if (pair.getPreviousFile() != null && Files.exists(pair.getPreviousFile())) {
                Path archivedOld = archiveDir.resolve(archiveName(pair.getPreviousFile()));
                Files.move(pair.getPreviousFile(), archivedOld, StandardCopyOption.REPLACE_EXISTING);
                log.info("Moved old baseline to archive: {}", archivedOld);
            }

            Path newBaseline = oldDir.resolve(pair.getCurrentFile().getFileName());
            Files.move(pair.getCurrentFile(), newBaseline, StandardCopyOption.REPLACE_EXISTING);

            log.info("Moved new file to old baseline: {}", newBaseline);

        } catch (Exception e) {
            throw new IllegalStateException("Failed rotating files after processing for source=" + pair.getSourceId(), e);
        }
    }

    private String archiveName(Path file) {
        String timestamp = LocalDateTime.now().format(ARCHIVE_FORMAT);
        return timestamp + "-" + file.getFileName();
    }
}