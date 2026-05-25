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

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

// Replaced by UaeSourceParser + uae strategy package.
// @Component removed so Spring no longer registers this bean.
// Safe to delete once UaeSourceParser has been verified in POC testing.
@RequiredArgsConstructor
@Slf4j
public class UaeMultiFormatParser extends AbstractStaxParser {

    private final CanonicalizationService canonicalizationService;

    @Override
    public boolean supports(String sourceId) {
        return false; // disabled — UaeSourceParser handles uae
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
            rawRecords = parseXml(file);
        } else {
            throw new IllegalArgumentException("Unsupported UAE file format: " + fileName);
        }

        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

        for (Map<String, Object> raw : rawRecords) {
            Map<String, Object> attributes = normalizeUaeRecord(raw);

            String businessKey = buildBusinessKey(attributes);
            if (businessKey == null || businessKey.isBlank()) {
                log.warn("Skipping UAE record with missing business key: sourceRef={} primaryName={}",
                        attributes.get("sourceRef"), attributes.get("primaryName"));
                continue;
            }

            Map<String, Object> canonical = canonicalizationService.canonicalizeMap(attributes);
            String hash = canonicalizationService.computeHash(canonical);

            entries.put(businessKey, NormalizedEntry.builder()
                    .businessKey(businessKey)
                    .sourceId(source.getId())
                    .attributes(canonical)
                    .canonicalHash(hash)
                    .build());
        }

        log.info("Parsed {} UAE entries from {}", entries.size(), file.getFileName());

        return NormalizedSanctionsFile.builder()
                .sourceId(source.getId())
                .fileName(file.getFileName().toString())
                .loadedAt(Instant.now())
                .entries(entries)
                .build();
    }

    // ── XML parsing (StAX) ───────────────────────────────────────────────────

    private List<Map<String, Object>> parseXml(Path file) {
        List<Map<String, Object>> records = new ArrayList<>();

        try (InputStream in = Files.newInputStream(file)) {
            XMLStreamReader reader = newFactory().createXMLStreamReader(in);

            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    String name = reader.getLocalName();
                    if ("Person".equals(name) || "Individual".equals(name)) {
                        records.add(parseXmlRecord(reader, name, "INDIVIDUAL"));
                    } else if ("Entity".equals(name) || "Organisation".equals(name)) {
                        records.add(parseXmlRecord(reader, name, "ENTITY"));
                    } else if ("Record".equals(name)) {
                        records.add(parseXmlRecord(reader, name, null));
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE XML file=" + file, e);
        }

        return records;
    }

    private Map<String, Object> parseXmlRecord(XMLStreamReader reader, String closingTag,
                                                String defaultEntityType) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        if (defaultEntityType != null) {
            record.put("entityType", defaultEntityType);
        }

        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                String el = reader.getLocalName();
                switch (el) {
                    case "ReferenceNumber", "Ref", "ID", "Id" ->
                            record.put("reference_number", safe(reader.getElementText()));
                    case "Name", "FullName", "EnglishName", "ListedName" ->
                            record.put("name", safe(reader.getElementText()));
                    case "EntityType", "Type", "Category" ->
                            record.put("entity_type", safe(reader.getElementText()));
                    case "Nationality", "Citizenship" ->
                            record.put("nationality", safe(reader.getElementText()));
                    case "DateOfBirth", "DOB", "BirthDate" ->
                            record.put("date_of_birth", safe(reader.getElementText()));
                    case "Remarks", "OtherInfo", "Comments" ->
                            record.put("remarks", safe(reader.getElementText()));
                    default -> { /* skip unrecognised elements */ }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        return record;
    }

    // ── Excel parsing (Apache POI) ───────────────────────────────────────────

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
                        String cellVal = cellValue(row.getCell(header.getKey()));
                        if (cellVal != null && !cellVal.isBlank()) {
                            record.put(normalizeColumnName(header.getValue()), cellVal.trim());
                        }
                    }

                    records.add(record);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE Excel file=" + file, e);
        }

        return records;
    }

    // ── PDF parsing (PDFBox) ─────────────────────────────────────────────────

    private List<Map<String, Object>> parsePdf(Path file) {
        List<Map<String, Object>> records = new ArrayList<>();

        try (PDDocument document = PDDocument.load(file.toFile())) {
            PDFTextStripper stripper = new PDFTextStripper();
            String text = stripper.getText(document);

            for (String line : text.split("\\R")) {
                String clean = line.trim();
                if (clean.isBlank() || !looksLikeSanctionRecord(clean)) {
                    continue;
                }

                Map<String, Object> record = new LinkedHashMap<>();
                record.put("name", extractNameFromPdfLine(clean));
                record.put("reference_number", extractReferenceFromPdfLine(clean));
                record.put("entity_type", detectEntityType(clean));
                records.add(record);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE PDF file=" + file, e);
        }

        return records;
    }

    // ── Canonical normalization ──────────────────────────────────────────────

    private Map<String, Object> normalizeUaeRecord(Map<String, Object> raw) {
        Map<String, Object> out = new LinkedHashMap<>();

        String sheetName = value(raw.get("sheetName"));

        String referenceNumber = firstNonBlank(raw,
                "reference_number", "reference", "ref_no", "listing_id", "id", "serial_no", "uid");

        String name = firstNonBlank(raw,
                "name", "full_name", "english_name", "listed_name", "individual_name", "entity_name");

        String entityType = firstNonBlank(raw, "entity_type", "type", "category");
        if (entityType == null) {
            entityType = inferEntityTypeFromSheet(sheetName);
        }

        String nationality = firstNonBlank(raw, "nationality", "citizenship", "country");
        String dob         = firstNonBlank(raw, "date_of_birth", "dob", "birth_date");
        String remarks     = firstNonBlank(raw, "remarks", "other_info", "comments");

        out.put("sourceRef",    referenceNumber);
        out.put("listType",     "UAE");
        out.put("entityType",   entityType);
        out.put("primaryName",  name);
        out.put("remarks",      remarks);

        // Scalar convenience fields (used by DB persistence directly)
        out.put("nationality",  nationality);
        out.put("dateOfBirth",  dob);

        // Canonical collection fields — always present as empty lists if no data
        out.put("aliases",      new ArrayList<>());
        out.put("addresses",    new ArrayList<>());
        out.put("documents",    new ArrayList<>());
        out.put("nationalities", nationality != null
                ? List.of(Map.of("country", nationality))
                : new ArrayList<>());
        out.put("datesOfBirth", dob != null
                ? List.of(Map.of("date", dob))
                : new ArrayList<>());

        return out;
    }

    private String buildBusinessKey(Map<String, Object> attributes) {
        String sourceRef = value(attributes.get("sourceRef"));
        if (sourceRef != null) {
            return sourceRef;
        }

        // Composite fallback for PDF/Excel rows with no stable ID
        String name        = value(attributes.get("primaryName"));
        String type        = value(attributes.get("entityType"));
        String dob         = value(attributes.get("dateOfBirth"));
        String nationality = value(attributes.get("nationality"));

        String composite = String.join("|",
                safeKey(type), safeKey(name), safeKey(dob), safeKey(nationality));

        return composite.isBlank() ? null : composite.toLowerCase();
    }

    // ── Excel helpers ────────────────────────────────────────────────────────

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

    // ── PDF helpers ──────────────────────────────────────────────────────────

    private boolean looksLikeSanctionRecord(String line) {
        return line.length() > 10
                && !line.toLowerCase().contains("page ")
                && !line.toLowerCase().contains("united arab emirates");
    }

    private String extractNameFromPdfLine(String line) {
        for (String sep : new String[]{" Ref ", " Reference ", " ID ", " No. "}) {
            int idx = line.indexOf(sep);
            if (idx > 0) return line.substring(0, idx).trim();
        }
        return line;
    }

    private String extractReferenceFromPdfLine(String line) {
        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile("(UAE[-_ ]?\\d+|\\b\\d{4,}\\b)").matcher(line);
        return m.find() ? m.group(1) : null;
    }

    private String detectEntityType(String text) {
        String s = text.toLowerCase();
        if (s.contains("company") || s.contains("llc") || s.contains("entity")
                || s.contains("organization") || s.contains("organisation")) {
            return "ENTITY";
        }
        return "INDIVIDUAL";
    }

    // ── Shared utilities ─────────────────────────────────────────────────────

    private String firstNonBlank(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object v = map.get(key);
            if (v != null && !v.toString().trim().isBlank()) return v.toString().trim();
        }
        return null;
    }

    private String inferEntityTypeFromSheet(String sheetName) {
        if (sheetName == null) return "UNKNOWN";
        String s = sheetName.toLowerCase();
        if (s.contains("individual") || s.contains("person")) return "INDIVIDUAL";
        if (s.contains("entity") || s.contains("organization") || s.contains("organisation")) return "ENTITY";
        return sheetName;
    }

    private String value(Object v) {
        if (v == null) return null;
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private String safeKey(String v) {
        return v == null ? "" : v.trim().replaceAll("\\s+", " ");
    }
}
