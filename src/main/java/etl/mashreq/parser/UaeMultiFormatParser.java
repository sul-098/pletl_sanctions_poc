package etl.mashreq.parser;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedEntry;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.service.CanonicalizationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class UaeMultiFormatParser extends AbstractStaxParser {

    private final CanonicalizationService canonicalizationService;

    @Override
    public boolean supports(String sourceId) {
        return "uae".equalsIgnoreCase(sourceId);
    }

    @Override
    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        String fileName = file.getFileName().toString().toLowerCase();

        List<Map<String, Object>> rawRecords;

        if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
            rawRecords = parseExcel(file);
        } else if (fileName.endsWith(".pdf")) {
            rawRecords = parsePdf(file);
        } else if (fileName.endsWith(".xml")) {
            throw new UnsupportedOperationException("UAE XML can be handled separately if required.");
        } else {
            throw new IllegalArgumentException("Unsupported UAE file format: " + fileName);
        }

        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

        for (Map<String, Object> raw : rawRecords) {
            Map<String, Object> attributes = normalizeUaeRecord(raw);

            String businessKey = buildBusinessKey(attributes);

            if (businessKey == null || businessKey.isBlank()) {
                log.warn("Skipping UAE record because business key is missing. record={}", attributes);
                continue;
            }

            Map<String, Object> canonical = canonicalizationService.canonicalizeMap(attributes);
            String hash = canonicalizationService.computeHash(canonical);

            NormalizedEntry entry = NormalizedEntry.builder()
                    .businessKey(businessKey)
                    .sourceId(source.getId())
                    .attributes(canonical)
                    .canonicalHash(hash)
                    .build();

            entries.put(businessKey, entry);
        }

        log.info("Parsed {} UAE entries from {}", entries.size(), file.getFileName());

        return NormalizedSanctionsFile.builder()
                .sourceId(source.getId())
                .fileName(file.getFileName().toString())
                .loadedAt(Instant.now())
                .entries(entries)
                .build();
    }

    private List<Map<String, Object>> parseExcel(Path file) {
        List<Map<String, Object>> records = new ArrayList<>();

        try (InputStream in = Files.newInputStream(file);
             Workbook workbook = WorkbookFactory.create(in)) {

            for (Sheet sheet : workbook) {
                String sheetName = sheet.getSheetName();

                Row headerRow = findHeaderRow(sheet);
                if (headerRow == null) {
                    log.warn("No header row found in UAE Excel sheet={}", sheetName);
                    continue;
                }

                Map<Integer, String> headers = readHeaders(headerRow);

                for (int rowIndex = headerRow.getRowNum() + 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                    Row row = sheet.getRow(rowIndex);
                    if (row == null || isEmptyRow(row)) {
                        continue;
                    }

                    Map<String, Object> record = new LinkedHashMap<>();
                    record.put("sheetName", sheetName);

                    for (Map.Entry<Integer, String> header : headers.entrySet()) {
                        String value = cellValue(row.getCell(header.getKey()));
                        if (value != null && !value.isBlank()) {
                            record.put(normalizeColumnName(header.getValue()), value.trim());
                        }
                    }

                    records.add(record);
                }
            }

            return records;

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE Excel file=" + file, e);
        }
    }

    private List<Map<String, Object>> parsePdf(Path file) {
        List<Map<String, Object>> records = new ArrayList<>();

        try (PDDocument document = PDDocument.load(file.toFile())) {
            PDFTextStripper stripper = new PDFTextStripper();
            String text = stripper.getText(document);

            String[] lines = text.split("\\R");

            for (String line : lines) {
                String clean = line.trim();
                if (clean.isBlank()) {
                    continue;
                }

                /*
                 * PDF parsing is intentionally conservative.
                 * UAE PDFs may not have stable tables.
                 * Here we capture line-based records first.
                 * Later, once you share actual UAE PDF sample layout,
                 * we can improve this into proper column extraction.
                 */
                if (looksLikeSanctionRecord(clean)) {
                    Map<String, Object> record = new LinkedHashMap<>();
                    record.put("rawLine", clean);
                    record.put("name", extractNameFromPdfLine(clean));
                    record.put("referenceNumber", extractReferenceFromPdfLine(clean));
                    record.put("entityType", detectEntityType(clean));
                    records.add(record);
                }
            }

            return records;

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE PDF file=" + file, e);
        }
    }

    private Map<String, Object> normalizeUaeRecord(Map<String, Object> raw) {
        Map<String, Object> normalized = new LinkedHashMap<>();

        String sheetName = value(raw.get("sheetName"));

        String referenceNumber = firstNonBlank(
                raw, "reference_number", "reference", "ref_no", "listing_id", "id", "serial_no", "uid"
        );

        String name = firstNonBlank(
                raw, "name", "full_name", "english_name", "listed_name", "individual_name", "entity_name"
        );

        String entityType = firstNonBlank(
                raw, "entity_type", "type", "category"
        );

        if (entityType == null) {
            entityType = inferEntityTypeFromSheet(sheetName);
        }

        String nationality = firstNonBlank(
                raw, "nationality", "citizenship", "country"
        );

        String dob = firstNonBlank(
                raw, "date_of_birth", "dob", "birth_date"
        );

        normalized.put("sourceRef", referenceNumber);
        normalized.put("listType", "UAE");
        normalized.put("entityType", entityType);
        normalized.put("primaryName", name);
        normalized.put("nationality", nationality);
        normalized.put("dateOfBirth", dob);
        normalized.put("sheetName", sheetName);

        normalized.put("raw", raw);

        return normalized;
    }

    private String buildBusinessKey(Map<String, Object> attributes) {
        String sourceRef = value(attributes.get("sourceRef"));
        if (sourceRef != null) {
            return sourceRef;
        }

        /*
         * Fallback key for UAE because PDF/Excel sometimes has no stable ID.
         * This allows comparison across formats if name + type are stable.
         */
        String name = value(attributes.get("primaryName"));
        String type = value(attributes.get("entityType"));
        String dob = value(attributes.get("dateOfBirth"));
        String nationality = value(attributes.get("nationality"));

        String composite = String.join("|",
                safeKey(type),
                safeKey(name),
                safeKey(dob),
                safeKey(nationality)
        );

        return composite.isBlank() ? null : composite.toLowerCase();
    }

    private Row findHeaderRow(Sheet sheet) {
        for (int i = 0; i <= Math.min(sheet.getLastRowNum(), 20); i++) {
            Row row = sheet.getRow(i);
            if (row == null) {
                continue;
            }

            int nonEmpty = 0;
            for (Cell cell : row) {
                String value = cellValue(cell);
                if (value != null && !value.isBlank()) {
                    nonEmpty++;
                }
            }

            if (nonEmpty >= 2) {
                return row;
            }
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
            String value = cellValue(cell);
            if (value != null && !value.isBlank()) {
                return false;
            }
        }
        return true;
    }

    private String cellValue(Cell cell) {
        if (cell == null) {
            return null;
        }

        DataFormatter formatter = new DataFormatter();
        return formatter.formatCellValue(cell).trim();
    }

    private String normalizeColumnName(String column) {
        return column == null ? null :
                column.trim()
                        .toLowerCase()
                        .replaceAll("[^a-z0-9]+", "_")
                        .replaceAll("^_+|_+$", "");
    }

    private String firstNonBlank(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value != null && !value.toString().trim().isBlank()) {
                return value.toString().trim();
            }
        }
        return null;
    }

    private String inferEntityTypeFromSheet(String sheetName) {
        if (sheetName == null) {
            return "UNKNOWN";
        }

        String s = sheetName.toLowerCase();

        if (s.contains("individual") || s.contains("person")) {
            return "INDIVIDUAL";
        }

        if (s.contains("entity") || s.contains("organization") || s.contains("organisation")) {
            return "ENTITY";
        }

        return sheetName;
    }

    private boolean looksLikeSanctionRecord(String line) {
        return line.length() > 10 &&
                !line.toLowerCase().contains("page ") &&
                !line.toLowerCase().contains("united arab emirates");
    }

    private String extractNameFromPdfLine(String line) {
        /*
         * Temporary generic rule:
         * If line contains reference separators, take text before them.
         */
        String[] separators = {" Ref ", " Reference ", " ID ", " No. "};

        for (String sep : separators) {
            int idx = line.indexOf(sep);
            if (idx > 0) {
                return line.substring(0, idx).trim();
            }
        }

        return line;
    }

    private String extractReferenceFromPdfLine(String line) {
        /*
         * Temporary generic extraction.
         * Once real PDF sample is known, replace with exact pattern.
         */
        java.util.regex.Matcher matcher =
                java.util.regex.Pattern.compile("(UAE[-_ ]?\\d+|\\b\\d{4,}\\b)").matcher(line);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    private String detectEntityType(String text) {
        String s = text.toLowerCase();

        if (s.contains("company") || s.contains("llc") || s.contains("entity") || s.contains("organization")) {
            return "ENTITY";
        }

        return "INDIVIDUAL";
    }

    private String value(Object value) {
        if (value == null) {
            return null;
        }

        String s = value.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private String safeKey(String value) {
        return value == null ? "" : value.trim().replaceAll("\\s+", " ");
    }
}