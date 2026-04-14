package etl.mashreq.service;

import  etl.mashreq.config.SanctionsProperties;
import  etl.mashreq.domain.DeltaFile;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
public class DeltaWriterService {

    private final SanctionsProperties properties;
    private final ObjectMapper objectMapper;

    public Path write(DeltaFile deltaFile) {
        try {
            Path outputDir = Path.of(properties.getDeltaOutputDirectory(), deltaFile.getSourceId());
            Files.createDirectories(outputDir);

            String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    .withZone(java.time.ZoneOffset.UTC)
                    .format(deltaFile.getGeneratedAt());

            String fileName = String.format("%s-delta-%s.json", deltaFile.getSourceId(), timestamp);
            Path outputFile = outputDir.resolve(fileName);

            if (properties.isPrettyJson()) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile.toFile(), deltaFile);
            } else {
                objectMapper.writeValue(outputFile.toFile(), deltaFile);
            }

            return outputFile;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write delta file", e);
        }
    }
}
