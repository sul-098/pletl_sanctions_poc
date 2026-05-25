package etl.mashreq.controller;

import etl.mashreq.domain.DeltaFile;
import etl.mashreq.service.ManualComparisonService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/compare")
@RequiredArgsConstructor
public class ManualComparisonController {

    private final ManualComparisonService manualComparisonService;

    /**
     * Upload two files and compare them.
     *
     * POST /api/compare/{sourceId}
     * Content-Type: multipart/form-data
     *   previousFile – the baseline (older) file
     *   currentFile  – the newer file
     *
     * The run is recorded as type=MANUAL with the client IP as triggered_by.
     * The delta is persisted to the database and returned as JSON.
     */
    @PostMapping(value = "/{sourceId}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public DeltaFile compare(
            @PathVariable String sourceId,
            @RequestPart("previousFile") MultipartFile previousFile,
            @RequestPart("currentFile")  MultipartFile currentFile,
            HttpServletRequest request) {

        if (previousFile.isEmpty()) {
            throw new IllegalArgumentException("previousFile must not be empty");
        }
        if (currentFile.isEmpty()) {
            throw new IllegalArgumentException("currentFile must not be empty");
        }

        String clientIp = resolveClientIp(request);
        return manualComparisonService.compare(sourceId, previousFile, currentFile, clientIp);
    }

    private String resolveClientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
