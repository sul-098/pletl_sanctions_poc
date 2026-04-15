package etl.mashreq.parser;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedEntry;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.service.CanonicalizationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class HmtStaxParser extends AbstractStaxParser {

    private final CanonicalizationService canonicalizationService;

    @Override
    public boolean supports(String sourceId) {
        return "hmt".equalsIgnoreCase(sourceId);
    }

    @Override
    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

        try (InputStream in = Files.newInputStream(file)) {
            XMLStreamReader reader = newFactory().createXMLStreamReader(in);

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT
                        && "Designation".equals(reader.getLocalName())) {

                    Map<String, Object> attributes = parseDesignation(reader);

                    String businessKey = value(attributes.get("sourceRef"));
                    if (businessKey == null || businessKey.isBlank()) {
                        log.warn("Skipping HMT entry with missing UniqueID in file={}", file.getFileName());
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
            }

            log.info("Parsed {} HMT entries from {}", entries.size(), file.getFileName());

            return NormalizedSanctionsFile.builder()
                    .sourceId(source.getId())
                    .fileName(file.getFileName().toString())
                    .loadedAt(Instant.now())
                    .entries(entries)
                    .build();

        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse HMT XML file: " + file, e);
        }
    }

    private Map<String, Object> parseDesignation(XMLStreamReader reader) throws Exception {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("listType", "HMT");

        String sourceRef = null;
        String entityType = null;
        String regimeName = null;
        String remarks = null;

        List<Map<String, Object>> aliases = new ArrayList<>();
        List<Map<String, Object>> addresses = new ArrayList<>();
        List<Map<String, Object>> datesOfBirth = new ArrayList<>();
        List<Map<String, Object>> nationalities = new ArrayList<>();

        Map<String, Object> primaryName = null;

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();

                switch (name) {
                    case "UniqueID" -> sourceRef = readSimpleText(reader);
                    case "IndividualEntityShip" -> entityType = readSimpleText(reader);
                    case "RegimeName" -> regimeName = readSimpleText(reader);
                    case "OtherInformation" -> remarks = readSimpleText(reader);

                    case "Name" -> {
                        Map<String, Object> parsedName = parseName(reader);
                        String nameType = value(parsedName.get("nameType"));

                        if (nameType != null && nameType.equalsIgnoreCase("Primary Name")) {
                            primaryName = parsedName;
                        } else {
                            aliases.add(parsedName);
                        }
                    }

                    case "Address" -> addresses.add(parseAddress(reader));
                    case "DateOfBirth" -> datesOfBirth.add(parseDateOfBirth(reader));
                    case "Nationality" -> nationalities.add(parseNationality(reader));

                    default -> {
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "Designation".equals(reader.getLocalName())) {
                break;
            }
        }

        attributes.put("sourceRef", safe(sourceRef));
        attributes.put("entityType", safe(entityType));
        attributes.put("regimeName", safe(regimeName));
        attributes.put("remarks", safe(remarks));
        attributes.put("primaryName", buildPrimaryName(primaryName));
        attributes.put("aliases", aliases);
        attributes.put("addresses", addresses);
        attributes.put("datesOfBirth", datesOfBirth);
        attributes.put("nationalities", nationalities);

        return attributes;
    }

    private Map<String, Object> parseName(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "Name1" -> map.put("name1", readSimpleText(reader));
                    case "Name2" -> map.put("name2", readSimpleText(reader));
                    case "Name3" -> map.put("name3", readSimpleText(reader));
                    case "Name4" -> map.put("name4", readSimpleText(reader));
                    case "Name5" -> map.put("name5", readSimpleText(reader));
                    case "Name6" -> map.put("name6", readSimpleText(reader));
                    case "NameType" -> map.put("nameType", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "Name".equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseAddress(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "AddressLine1" -> map.put("addressLine1", readSimpleText(reader));
                    case "AddressLine2" -> map.put("addressLine2", readSimpleText(reader));
                    case "AddressLine3" -> map.put("addressLine3", readSimpleText(reader));
                    case "AddressCity" -> map.put("city", readSimpleText(reader));
                    case "AddressCountry" -> map.put("country", readSimpleText(reader));
                    case "PostCode" -> map.put("postalCode", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "Address".equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseDateOfBirth(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        String text = readElementTextManually(reader, "DateOfBirth");
        if (text != null) {
            map.put("value", text);
        }

        return map;
    }

    private Map<String, Object> parseNationality(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        String text = readElementTextManually(reader, "Nationality");
        if (text != null) {
            map.put("value", text);
        }

        return map;
    }

    private String buildPrimaryName(Map<String, Object> primaryNameMap) {
        if (primaryNameMap == null || primaryNameMap.isEmpty()) {
            return null;
        }

        List<String> parts = new ArrayList<>();
        for (String key : List.of("name1", "name2", "name3", "name4", "name5", "name6")) {
            Object value = primaryNameMap.get(key);
            if (value != null) {
                String s = value.toString().trim();
                if (!s.isEmpty()) {
                    parts.add(s);
                }
            }
        }

        if (parts.isEmpty()) {
            return null;
        }

        return String.join(" ", parts).replaceAll("\\s+", " ").trim();
    }

    private String readSimpleText(XMLStreamReader reader) throws Exception {
        return safe(reader.getElementText());
    }

    private String readElementTextManually(XMLStreamReader reader, String closingTag) throws Exception {
        StringBuilder sb = new StringBuilder();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA) {
                sb.append(reader.getText());
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        String value = sb.toString().trim();
        return value.isEmpty() ? null : value;
    }

    private String value(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }
}