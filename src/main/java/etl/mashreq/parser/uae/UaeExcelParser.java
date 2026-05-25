package etl.mashreq.parser.uae;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedEntry;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.service.CanonicalizationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses UAE sanctions files in XLSX / XLS format (Apache POI).
 * Not a @Component — instantiated and owned by UaeSourceParser.
 * Produces the canonical normalized model via UaeNormalizationHelper.
 */
@Slf4j
public class UaeExcelParser {

    private final CanonicalizationService canonicalizationService;

    public UaeExcelParser(CanonicalizationService canonicalizationService) {
        this.canonicalizationService = canonicalizationService;
    }

    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();
        int skipped  = 0;
        int rowsRead = 0;

        try (InputStream in = Files.newInputStream(file);
             Workbook workbook = WorkbookFactory.create(in)) {

            for (Sheet sheet : workbook) {
                String sheetName = sheet.getSheetName();

                Row headerRow = findHeaderRow(sheet);
                if (headerRow == null) {
                    log.warn("UAE Excel: no header row found in sheet='{}' — skipping sheet", sheetName);
                    continue;
                }

                Map<Integer, String> headers = readHeaders(headerRow);
                log.debug("UAE Excel: sheet='{}' headers={}", sheetName, headers.values());

                for (int i = headerRow.getRowNum() + 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
                    if (row == null || isEmptyRow(row)) continue;

                    rowsRead++;
                    Map<String, Object> raw = new LinkedHashMap<>();
                    raw.put("sheetName", sheetName);

                    for (Map.Entry<Integer, String> h : headers.entrySet()) {
                        String val = cellValue(row.getCell(h.getKey()));
                        if (val != null && !val.isBlank()) {
                            raw.put(normalizeColumnName(h.getValue()), val.trim());
                        }
                    }

                    Map<String, Object> normalized = UaeNormalizationHelper.normalize(raw);
                    String businessKey = UaeNormalizationHelper.buildBusinessKey(normalized);

                    if (businessKey == null || businessKey.isBlank()) {
                        log.warn("UAE Excel: skipping row {} in sheet='{}' — missing business key",
                                i, sheetName);
                        skipped++;
                        continue;
                    }

                    Map<String, Object> canonical = canonicalizationService.canonicalizeMap(normalized);
                    String hash = canonicalizationService.computeHash(canonical);

                    entries.put(businessKey, NormalizedEntry.builder()
                            .businessKey(businessKey)
                            .sourceId(source.getId())
                            .attributes(canonical)
                            .canonicalHash(hash)
                            .build());
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE Excel file: " + file, e);
        }

        log.info("UAE Excel parse complete: file={} rowsRead={} entries={} skipped={}",
                file.getFileName(), rowsRead, entries.size(), skipped);

        return NormalizedSanctionsFile.builder()
                .sourceId(source.getId())
                .fileName(file.getFileName().toString())
                .loadedAt(Instant.now())
                .entries(entries)
                .skippedCount(skipped)
                .build();
    }

    // ── Excel helpers ─────────────────────────────────────────────────────────

    private Row findHeaderRow(Sheet sheet) {
        for (int i = 0; i <= Math.min(sheet.getLastRowNum(), 20); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;
            int nonEmpty = 0;
            for (Cell cell : row) {
                String v = cellValue(cell);
                if (v != null && !v.isBlank()) nonEmpty++;
            }
            if (nonEmpty >= 2) return row;
        }
        return null;
    }

    private Map<Integer, String> readHeaders(Row headerRow) {
        Map<Integer, String> headers = new LinkedHashMap<>();
        for (Cell cell : headerRow) {
            String header = cellValue(cell);
            if (header != null && !header.isBlank()) {
                headers.put(cell.getColumnIndex(), header);
            }
        }
        return headers;
    }

    private boolean isEmptyRow(Row row) {
        for (Cell cell : row) {
            String v = cellValue(cell);
            if (v != null && !v.isBlank()) return false;
        }
        return true;
    }

    private String cellValue(Cell cell) {
        if (cell == null) return null;
        return new DataFormatter().formatCellValue(cell).trim();
    }

    private String normalizeColumnName(String column) {
        if (column == null) return null;
        return column.trim().toLowerCase()
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
    }
}
