package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.config.SourceProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
@RequiredArgsConstructor
@Slf4j
public class DownloadOrchestrator {

    private final SanctionsProperties properties;
    private final HttpDownloadService httpDownloadService;
    private final DownloadNamingService namingService;
    private final ProcessingOrchestrator processingOrchestrator;

    public void downloadAll() {
        properties.getSources().stream()
                .filter(SourceProperties::isDownloadEnabled)
                .forEach(this::downloadSingle);
    }

    public void downloadSingle(SourceProperties source) {
        try {
            log.info("Downloading source={}", source.getId());

            InputStream stream = httpDownloadService.download(source.getDownloadUrl());

            Path targetDir = Path.of(properties.getSharedRootDirectory(), source.getSubDirectory());
            Files.createDirectories(targetDir);

            String fileName = namingService.generateFileName(source.getTargetFilePrefix());

            Path tempFile = targetDir.resolve(fileName + ".part");
            Path finalFile = targetDir.resolve(fileName);

            Files.copy(stream, tempFile);

            Files.move(tempFile, finalFile);

            log.info("Download completed: {}", finalFile);

            // Immediately trigger processing
            processingOrchestrator.processAll();

        } catch (Exception e) {
            log.error("Download failed for source={}", source.getId(), e);
        }
    }
}