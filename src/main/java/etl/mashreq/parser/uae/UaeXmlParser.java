package etl.mashreq.parser.uae;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedEntry;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.service.CanonicalizationService;
import lombok.extern.slf4j.Slf4j;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses UAE sanctions files in XML format.
 * Not a @Component — instantiated and owned by UaeSourceParser.
 * Produces the canonical normalized model via UaeNormalizationHelper.
 */
@Slf4j
public class UaeXmlParser {

    private final CanonicalizationService canonicalizationService;

    public UaeXmlParser(CanonicalizationService canonicalizationService) {
        this.canonicalizationService = canonicalizationService;
    }

    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();
        int skipped = 0;

        try (InputStream in = Files.newInputStream(file)) {
            XMLStreamReader reader = newFactory().createXMLStreamReader(in);

            while (reader.hasNext()) {
                int event = reader.next();
                if (event != XMLStreamConstants.START_ELEMENT) continue;

                String localName = reader.getLocalName();
                String defaultEntityType;

                if ("Person".equals(localName) || "Individual".equals(localName)) {
                    defaultEntityType = "INDIVIDUAL";
                } else if ("Entity".equals(localName) || "Organisation".equals(localName)) {
                    defaultEntityType = "ENTITY";
                } else if ("Record".equals(localName)) {
                    defaultEntityType = null;
                } else {
                    continue;
                }

                Map<String, Object> raw = parseXmlRecord(reader, localName, defaultEntityType);
                Map<String, Object> normalized = UaeNormalizationHelper.normalize(raw);
                String businessKey = UaeNormalizationHelper.buildBusinessKey(normalized);

                if (businessKey == null || businessKey.isBlank()) {
                    log.warn("UAE XML: skipping record — missing business key: primaryName={}",
                            normalized.get("primaryName"));
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
            throw new RuntimeException("Failed to parse UAE XML file: " + file, e);
        }

        log.info("UAE XML parse complete: file={} entries={} skipped={}",
                file.getFileName(), entries.size(), skipped);

        return NormalizedSanctionsFile.builder()
                .sourceId(source.getId())
                .fileName(file.getFileName().toString())
                .loadedAt(Instant.now())
                .entries(entries)
                .skippedCount(skipped)
                .build();
    }

    private Map<String, Object> parseXmlRecord(XMLStreamReader reader,
                                                String closingTag,
                                                String defaultEntityType) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        if (defaultEntityType != null) {
            record.put("entity_type", defaultEntityType);
        }

        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
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
                    default -> log.debug("UAE XML: unrecognised element <{}>", reader.getLocalName());
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        return record;
    }

    /** XXE-safe StAX factory — same settings as AbstractStaxParser. */
    private XMLInputFactory newFactory() {
        XMLInputFactory f = XMLInputFactory.newFactory();
        f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
        f.setProperty(XMLInputFactory.IS_COALESCING, true);
        f.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        return f;
    }

    private String safe(String value) {
        if (value == null) return null;
        String t = value.trim();
        return t.isEmpty() ? null : t;
    }
}
