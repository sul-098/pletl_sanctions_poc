package etl.mashreq.service;

import etl.mashreq.config.SanctionsProperties;
import etl.mashreq.domain.ProcessingState;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
@RequiredArgsConstructor
public class StateStoreService {

    private final SanctionsProperties properties;
    private final ObjectMapper objectMapper;

    public synchronized ProcessingState load() {
        Path statePath = Path.of(properties.getStateFile());
        if (!Files.exists(statePath)) {
            return new ProcessingState();
        }

        try {
            return objectMapper.readValue(Files.readString(statePath), ProcessingState.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load state file: " + statePath, e);
        }
    }

    public synchronized void save(ProcessingState state) {
        Path statePath = Path.of(properties.getStateFile());
        try {
            Files.createDirectories(statePath.getParent());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(statePath.toFile(), state);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to save state file: " + statePath, e);
        }
    }
}
