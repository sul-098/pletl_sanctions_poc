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
public class OfacStaxParser extends AbstractStaxParser {

    private final CanonicalizationService canonicalizationService;

    @Override
    public boolean supports(String sourceId) {
        return "ofac".equalsIgnoreCase(sourceId);
    }

    @Override
    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

        try (InputStream in = Files.newInputStream(file)) {
            XMLStreamReader reader = newFactory().createXMLStreamReader(in);

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT
                        && "sdnEntry".equals(reader.getLocalName())) {

                    Map<String, Object> attributes = parseSdnEntry(reader);

                    String businessKey = value(attributes.get("sourceRef"));
                    if (businessKey == null) {
                        log.warn("Skipping OFAC entry with missing uid in file={}", file.getFileName());
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

            log.info("Parsed {} OFAC entries from {}", entries.size(), file.getFileName());

            return NormalizedSanctionsFile.builder()
                    .sourceId(source.getId())
                    .fileName(file.getFileName().toString())
                    .loadedAt(Instant.now())
                    .entries(entries)
                    .build();

        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse OFAC XML file: " + file, e);
        }
    }

    private Map<String, Object> parseSdnEntry(XMLStreamReader reader) throws Exception {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("listType", "OFAC");

        String uid = null;
        String firstName = null;
        String lastName = null;
        String entityType = null;
        String remarks = null;

        List<Map<String, Object>> aliases = new ArrayList<>();
        List<Map<String, Object>> addresses = new ArrayList<>();
        List<Map<String, Object>> nationalities = new ArrayList<>();
        List<Map<String, Object>> datesOfBirth = new ArrayList<>();
        List<Map<String, Object>> placeOfBirths = new ArrayList<>();
        List<Map<String, Object>> documents = new ArrayList<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();

                switch (name) {
                    case "uid" -> uid = readSimpleText(reader);
                    case "firstName" -> {
                        if (firstName == null) {
                            firstName = readSimpleText(reader);
                        }
                    }
                    case "lastName" -> {
                        if (lastName == null) {
                            lastName = readSimpleText(reader);
                        }
                    }
                    case "sdnType" -> entityType = readSimpleText(reader);
                    case "remarks" -> remarks = readSimpleText(reader);
                    case "aka" -> aliases.add(parseAka(reader));
                    case "address" -> addresses.add(parseAddress(reader));
                    case "nationality" -> nationalities.add(parseNationality(reader));
                    case "dateOfBirthItem" -> datesOfBirth.add(parseDateOfBirth(reader));
                    case "placeOfBirthItem" -> placeOfBirths.add(parsePlaceOfBirth(reader));
                    case "id" -> documents.add(parseId(reader));
                    default -> {
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "sdnEntry".equals(reader.getLocalName())) {
                break;
            }
        }

        String primaryName = joinName(firstName, lastName);

        attributes.put("sourceRef", safe(uid));
        attributes.put("entityType", safe(entityType));
        attributes.put("primaryName", safe(primaryName));
        attributes.put("remarks", safe(remarks));
        attributes.put("aliases", aliases);
        attributes.put("addresses", addresses);
        attributes.put("nationalities", nationalities);
        attributes.put("datesOfBirth", datesOfBirth);
        attributes.put("placeOfBirths", placeOfBirths);
        attributes.put("documents", documents);

        return attributes;
    }

    private Map<String, Object> parseAka(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "firstName" -> map.put("firstName", readSimpleText(reader));
                    case "lastName" -> map.put("lastName", readSimpleText(reader));
                    case "category" -> map.put("category", readSimpleText(reader));
                    case "type" -> map.put("type", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "aka".equals(reader.getLocalName())) {
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
                    case "address1" -> map.put("address1", readSimpleText(reader));
                    case "address2" -> map.put("address2", readSimpleText(reader));
                    case "city" -> map.put("city", readSimpleText(reader));
                    case "stateOrProvince" -> map.put("stateOrProvince", readSimpleText(reader));
                    case "postalCode" -> map.put("postalCode", readSimpleText(reader));
                    case "country" -> map.put("country", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "address".equals(reader.getLocalName())) {
                break;
            }
        }
        return map;
    }

    private Map<String, Object> parseNationality(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "country" -> map.put("country", readSimpleText(reader));
                    case "mainEntry" -> map.put("mainEntry", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "nationality".equals(reader.getLocalName())) {
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
                    case "dateOfBirth" -> map.put("dateOfBirth", readSimpleText(reader));
                    case "mainEntry" -> map.put("mainEntry", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "dateOfBirthItem".equals(reader.getLocalName())) {
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
                    case "placeOfBirth" -> map.put("placeOfBirth", readSimpleText(reader));
                    case "mainEntry" -> map.put("mainEntry", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "placeOfBirthItem".equals(reader.getLocalName())) {
                break;
            }
        }
        return map;
    }

    private Map<String, Object> parseId(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "idType" -> map.put("idType", readSimpleText(reader));
                    case "idNumber" -> map.put("idNumber", readSimpleText(reader));
                    case "idCountry" -> map.put("idCountry", readSimpleText(reader));
                    case "expirationDate" -> map.put("expirationDate", readSimpleText(reader));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "id".equals(reader.getLocalName())) {
                break;
            }
        }
        return map;
    }

    private String readSimpleText(XMLStreamReader reader) throws Exception {
        return safe(reader.getElementText());
    }

    private String joinName(String firstName, String lastName) {
        String joined = String.join(" ",
                firstName == null ? "" : firstName.trim(),
                lastName == null ? "" : lastName.trim()).trim();
        return joined.isEmpty() ? null : joined.replaceAll("\\s+", " ");
    }

    private String value(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }
}