package etl.mashreq.parser.uae;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedEntry;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.service.CanonicalizationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses UAE sanctions files in PDF format (Apache PDFBox text extraction).
 * Not a @Component — instantiated and owned by UaeSourceParser.
 *
 * PDF is an unstructured format. Extraction is layout-dependent and may produce
 * partial results if the UAE publishes a redesigned PDF. The parser logs the
 * extracted count so regressions are visible immediately.
 *
 * Produces the canonical normalized model via UaeNormalizationHelper.
 */
@Slf4j
public class UaePdfParser {

    private static final Pattern REFERENCE_PATTERN =
            Pattern.compile("(UAE[-_]?\\d+|\\b\\d{4,}\\b)");

    private final CanonicalizationService canonicalizationService;

    public UaePdfParser(CanonicalizationService canonicalizationService) {
        this.canonicalizationService = canonicalizationService;
    }

    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();
        int skipped  = 0;
        int linesRead = 0;

        try (PDDocument document = PDDocument.load(file.toFile())) {
            PDFTextStripper stripper = new PDFTextStripper();
            String text = stripper.getText(document);

            for (String line : text.split("\\R")) {
                String clean = line.trim();
                if (clean.isBlank() || !looksLikeSanctionRecord(clean)) continue;

                linesRead++;
                Map<String, Object> raw = new LinkedHashMap<>();
                raw.put("name",             extractName(clean));
                raw.put("reference_number", extractReference(clean));
                raw.put("entity_type",      detectEntityType(clean));

                Map<String, Object> normalized = UaeNormalizationHelper.normalize(raw);
                String businessKey = UaeNormalizationHelper.buildBusinessKey(normalized);

                if (businessKey == null || businessKey.isBlank()) {
                    log.warn("UAE PDF: skipping line — missing business key: line='{}'",
                            truncate(clean, 80));
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

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UAE PDF file: " + file, e);
        }

        // Log extraction quality for POC validation — if entries << linesRead,
        // the PDF layout likely changed and the parser needs updating.
        log.info("UAE PDF parse complete: file={} linesRead={} entries={} skipped={}",
                file.getFileName(), linesRead, entries.size(), skipped);

        if (linesRead > 0 && entries.isEmpty()) {
            log.warn("UAE PDF: zero entries extracted from {} candidate lines in '{}'. " +
                     "PDF layout may have changed — manual review recommended.",
                    linesRead, file.getFileName());
        }

        return NormalizedSanctionsFile.builder()
                .sourceId(source.getId())
                .fileName(file.getFileName().toString())
                .loadedAt(Instant.now())
                .entries(entries)
                .skippedCount(skipped)
                .build();
    }

    // ── PDF extraction helpers ─────────────────────────────────────────────────

    private boolean looksLikeSanctionRecord(String line) {
        return line.length() > 10
                && !line.toLowerCase().contains("page ")
                && !line.toLowerCase().contains("united arab emirates")
                && !line.toLowerCase().startsWith("no.")
                && !line.toLowerCase().startsWith("serial");
    }

    private String extractName(String line) {
        for (String sep : new String[]{" Ref ", " Reference ", " ID ", " No. "}) {
            int idx = line.indexOf(sep);
            if (idx > 0) return line.substring(0, idx).trim();
        }
        return line;
    }

    private String extractReference(String line) {
        Matcher m = REFERENCE_PATTERN.matcher(line);
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

    private String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
