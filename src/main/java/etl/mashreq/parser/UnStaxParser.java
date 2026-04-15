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
public class UnStaxParser extends AbstractStaxParser {

    private final CanonicalizationService canonicalizationService;

    @Override
    public boolean supports(String sourceId) {
        return "un".equalsIgnoreCase(sourceId);
    }

    @Override
    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

        try (InputStream in = Files.newInputStream(file)) {
            XMLStreamReader reader = newFactory().createXMLStreamReader(in);

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT) {
                    String localName = reader.getLocalName();

                    if ("INDIVIDUAL".equals(localName) || "ENTITY".equals(localName)) {
                        Map<String, Object> attributes = parseRecord(reader, localName);

                        String businessKey = value(attributes.get("sourceRef"));
                        if (businessKey == null || businessKey.isBlank()) {
                            log.warn("Skipping UN entry with missing DATAID in file={}", file.getFileName());
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
            }

            log.info("Parsed {} UN entries from {}", entries.size(), file.getFileName());

            return NormalizedSanctionsFile.builder()
                    .sourceId(source.getId())
                    .fileName(file.getFileName().toString())
                    .loadedAt(Instant.now())
                    .entries(entries)
                    .build();

        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse UN XML file: " + file, e);
        }
    }

    private Map<String, Object> parseRecord(XMLStreamReader reader, String recordType) throws Exception {
        Map<String, Object> attributes = new LinkedHashMap<>();

        String sourceRef = null;
        String firstName = null;
        String secondName = null;
        String thirdName = null;
        String fourthName = null;
        String referenceNumber = null;
        String unListType = null;
        String listedOn = null;
        String gender = null;
        String comments = null;

        List<Map<String, Object>> designations = new ArrayList<>();
        List<Map<String, Object>> titles = new ArrayList<>();
        List<Map<String, Object>> nationalities = new ArrayList<>();
        List<Map<String, Object>> aliases = new ArrayList<>();
        List<Map<String, Object>> addresses = new ArrayList<>();
        List<Map<String, Object>> datesOfBirth = new ArrayList<>();
        List<Map<String, Object>> placesOfBirth = new ArrayList<>();
        List<Map<String, Object>> documents = new ArrayList<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();

                switch (name) {
                    case "DATAID" -> sourceRef = readSimpleText(reader);
                    case "FIRST_NAME" -> firstName = readSimpleText(reader);
                    case "SECOND_NAME" -> secondName = readSimpleText(reader);
                    case "THIRD_NAME" -> thirdName = readSimpleText(reader);
                    case "FOURTH_NAME" -> fourthName = readSimpleText(reader);
                    case "REFERENCE_NUMBER" -> referenceNumber = readSimpleText(reader);
                    case "UN_LIST_TYPE" -> unListType = readSimpleText(reader);
                    case "LISTED_ON" -> listedOn = readSimpleText(reader);
                    case "GENDER" -> gender = readSimpleText(reader);
                    case "COMMENTS1" -> comments = readSimpleText(reader);

                    case "DESIGNATION" -> designations.add(parseValueContainer(reader, "DESIGNATION"));
                    case "TITLE" -> titles.add(parseValueContainer(reader, "TITLE"));
                    case "NATIONALITY" -> nationalities.add(parseNationality(reader));
                    case "INDIVIDUAL_ALIAS", "ENTITY_ALIAS" -> aliases.add(parseAlias(reader, name));
                    case "INDIVIDUAL_ADDRESS", "ENTITY_ADDRESS" -> addresses.add(parseAddress(reader, name));
                    case "INDIVIDUAL_DATE_OF_BIRTH" -> datesOfBirth.add(parseDateOfBirth(reader));
                    case "INDIVIDUAL_PLACE_OF_BIRTH" -> placesOfBirth.add(parsePlaceOfBirth(reader));
                    case "INDIVIDUAL_DOCUMENT", "ENTITY_DOCUMENT" -> documents.add(parseDocument(reader, name));

                    default -> {
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && recordType.equals(reader.getLocalName())) {
                break;
            }
        }

        attributes.put("sourceRef", safe(sourceRef));
        attributes.put("listType", "UN");
        attributes.put("entityType", recordType);
        attributes.put("primaryName", joinName(firstName, secondName, thirdName, fourthName));
        attributes.put("referenceNumber", safe(referenceNumber));
        attributes.put("unListType", safe(unListType));
        attributes.put("listedOn", safe(listedOn));
        attributes.put("gender", safe(gender));
        attributes.put("comments", safe(comments));
        attributes.put("designations", designations);
        attributes.put("titles", titles);
        attributes.put("nationalities", nationalities);
        attributes.put("aliases", aliases);
        attributes.put("addresses", addresses);
        attributes.put("datesOfBirth", datesOfBirth);
        attributes.put("placesOfBirth", placesOfBirth);
        attributes.put("documents", documents);

        return attributes;
    }

    private Map<String, Object> parseValueContainer(XMLStreamReader reader, String closingTag) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT && "VALUE".equals(reader.getLocalName())) {
                map.put("value", readSimpleText(reader));
            } else if (event == XMLStreamConstants.END_ELEMENT && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseNationality(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT && "VALUE".equals(reader.getLocalName())) {
                map.put("value", readSimpleText(reader));
            } else if (event == XMLStreamConstants.END_ELEMENT && "NATIONALITY".equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseAlias(XMLStreamReader reader, String closingTag) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "ALIAS_NAME" -> map.put("aliasName", readSimpleText(reader));
                    case "QUALITY" -> map.put("quality", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseAddress(XMLStreamReader reader, String closingTag) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "STREET" -> map.put("street", readSimpleText(reader));
                    case "CITY" -> map.put("city", readSimpleText(reader));
                    case "STATE_PROVINCE" -> map.put("stateProvince", readSimpleText(reader));
                    case "COUNTRY" -> map.put("country", readSimpleText(reader));
                    case "NOTE" -> map.put("note", readSimpleText(reader));
                    case "ZIP_CODE" -> map.put("zipCode", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseDateOfBirth(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "TYPE_OF_DATE" -> map.put("typeOfDate", readSimpleText(reader));
                    case "DATE" -> map.put("date", readSimpleText(reader));
                    case "YEAR" -> map.put("year", readSimpleText(reader));
                    case "FROM_YEAR" -> map.put("fromYear", readSimpleText(reader));
                    case "TO_YEAR" -> map.put("toYear", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "INDIVIDUAL_DATE_OF_BIRTH".equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parsePlaceOfBirth(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "CITY" -> map.put("city", readSimpleText(reader));
                    case "STATE_PROVINCE" -> map.put("stateProvince", readSimpleText(reader));
                    case "COUNTRY" -> map.put("country", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "INDIVIDUAL_PLACE_OF_BIRTH".equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseDocument(XMLStreamReader reader, String closingTag) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "TYPE_OF_DOCUMENT" -> map.put("typeOfDocument", readSimpleText(reader));
                    case "NUMBER" -> map.put("number", readSimpleText(reader));
                    case "ISSUING_COUNTRY" -> map.put("issuingCountry", readSimpleText(reader));
                    case "DATE_OF_ISSUE" -> map.put("dateOfIssue", readSimpleText(reader));
                    case "CITY_OF_ISSUE" -> map.put("cityOfIssue", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private String readSimpleText(XMLStreamReader reader) throws Exception {
        return safe(reader.getElementText());
    }

    private String joinName(String... parts) {
        List<String> values = new ArrayList<>();
        for (String part : parts) {
            if (part != null) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    values.add(trimmed);
                }
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        return String.join(" ", values).replaceAll("\\s+", " ").trim();
    }

    private String value(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }
}