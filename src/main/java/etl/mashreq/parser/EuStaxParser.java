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
public class EuStaxParser extends AbstractStaxParser {

    private final CanonicalizationService canonicalizationService;

    @Override
    public boolean supports(String sourceId) {
        return "eu".equalsIgnoreCase(sourceId);
    }

    @Override
    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

        try (InputStream in = Files.newInputStream(file)) {
            XMLStreamReader reader = newFactory().createXMLStreamReader(in);

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT
                        && "sanctionEntity".equals(reader.getLocalName())) {

                    Map<String, Object> attributes = parseSanctionEntity(reader);

                    String businessKey = value(attributes.get("sourceRef"));
                    if (businessKey == null || businessKey.isBlank()) {
                        log.warn("Skipping EU entry with missing euReferenceNumber in file={}", file.getFileName());
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

            log.info("Parsed {} EU entries from {}", entries.size(), file.getFileName());

            return NormalizedSanctionsFile.builder()
                    .sourceId(source.getId())
                    .fileName(file.getFileName().toString())
                    .loadedAt(Instant.now())
                    .entries(entries)
                    .build();

        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse EU XML file: " + file, e);
        }
    }

    private Map<String, Object> parseSanctionEntity(XMLStreamReader reader) throws Exception {
        Map<String, Object> attributes = new LinkedHashMap<>();

        String sourceRef = safe(reader.getAttributeValue(null, "euReferenceNumber"));
        String logicalId = safe(reader.getAttributeValue(null, "logicalId"));
        String unitedNationId = safe(reader.getAttributeValue(null, "unitedNationId"));
        String designationDetails = safe(reader.getAttributeValue(null, "designationDetails"));

        String entityType = null;
        String primaryName = null;
        String remark = null;

        List<Map<String, Object>> aliases = new ArrayList<>();
        List<Map<String, Object>> citizenships = new ArrayList<>();
        List<Map<String, Object>> birthdates = new ArrayList<>();
        List<Map<String, Object>> regulations = new ArrayList<>();
        List<Map<String, Object>> identifications = new ArrayList<>();
        List<Map<String, Object>> addresses = new ArrayList<>();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();

                switch (name) {
                    case "remark" -> remark = readSimpleText(reader);

                    case "subjectType" -> entityType = safe(reader.getAttributeValue(null, "code"));

                    case "nameAlias" -> {
                        Map<String, Object> alias = parseNameAlias(reader);
                        aliases.add(alias);

                        if (primaryName == null) {
                            String candidate = value(alias.get("wholeName"));
                            if (candidate != null && !candidate.isBlank()) {
                                primaryName = candidate;
                            }
                        }
                    }

                    case "citizenship" -> citizenships.add(parseCitizenship(reader));
                    case "birthdate" -> birthdates.add(parseBirthdate(reader));
                    case "regulation" -> regulations.add(parseRegulation(reader));
                    case "identification" -> identifications.add(parseIdentification(reader));
                    case "address" -> addresses.add(parseAddress(reader));

                    default -> {
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "sanctionEntity".equals(reader.getLocalName())) {
                break;
            }
        }

        attributes.put("sourceRef", sourceRef);
        attributes.put("listType", "EU");
        attributes.put("entityType", entityType);
        attributes.put("primaryName", primaryName);
        attributes.put("logicalId", logicalId);
        attributes.put("unitedNationId", unitedNationId);
        attributes.put("designationDetails", designationDetails);
        attributes.put("remark", remark);
        attributes.put("aliases", aliases);
        attributes.put("citizenships", citizenships);
        attributes.put("birthdates", birthdates);
        attributes.put("regulations", regulations);
        attributes.put("identifications", identifications);
        attributes.put("addresses", addresses);

        return attributes;
    }

    private Map<String, Object> parseNameAlias(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("wholeName", safe(reader.getAttributeValue(null, "wholeName")));
        map.put("firstName", safe(reader.getAttributeValue(null, "firstName")));
        map.put("middleName", safe(reader.getAttributeValue(null, "middleName")));
        map.put("lastName", safe(reader.getAttributeValue(null, "lastName")));
        map.put("function", safe(reader.getAttributeValue(null, "function")));
        map.put("gender", safe(reader.getAttributeValue(null, "gender")));
        map.put("title", safe(reader.getAttributeValue(null, "title")));
        map.put("nameLanguage", safe(reader.getAttributeValue(null, "nameLanguage")));
        map.put("strong", safe(reader.getAttributeValue(null, "strong")));
        map.put("logicalId", safe(reader.getAttributeValue(null, "logicalId")));

        consumeToEnd(reader, "nameAlias");
        return map;
    }

    private Map<String, Object> parseCitizenship(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("region", safe(reader.getAttributeValue(null, "region")));
        map.put("countryIso2Code", safe(reader.getAttributeValue(null, "countryIso2Code")));
        map.put("countryDescription", safe(reader.getAttributeValue(null, "countryDescription")));
        map.put("logicalId", safe(reader.getAttributeValue(null, "logicalId")));

        consumeToEnd(reader, "citizenship");
        return map;
    }

    private Map<String, Object> parseBirthdate(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("birthdate", safe(reader.getAttributeValue(null, "birthdate")));
        map.put("year", safe(reader.getAttributeValue(null, "year")));
        map.put("monthOfYear", safe(reader.getAttributeValue(null, "monthOfYear")));
        map.put("dayOfMonth", safe(reader.getAttributeValue(null, "dayOfMonth")));
        map.put("circa", safe(reader.getAttributeValue(null, "circa")));
        map.put("calendarType", safe(reader.getAttributeValue(null, "calendarType")));
        map.put("city", safe(reader.getAttributeValue(null, "city")));
        map.put("zipCode", safe(reader.getAttributeValue(null, "zipCode")));
        map.put("region", safe(reader.getAttributeValue(null, "region")));
        map.put("place", safe(reader.getAttributeValue(null, "place")));
        map.put("countryIso2Code", safe(reader.getAttributeValue(null, "countryIso2Code")));
        map.put("countryDescription", safe(reader.getAttributeValue(null, "countryDescription")));
        map.put("logicalId", safe(reader.getAttributeValue(null, "logicalId")));

        consumeToEnd(reader, "birthdate");
        return map;
    }

    private Map<String, Object> parseRegulation(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("regulationType", safe(reader.getAttributeValue(null, "regulationType")));
        map.put("organisationType", safe(reader.getAttributeValue(null, "organisationType")));
        map.put("publicationDate", safe(reader.getAttributeValue(null, "publicationDate")));
        map.put("entryIntoForceDate", safe(reader.getAttributeValue(null, "entryIntoForceDate")));
        map.put("numberTitle", safe(reader.getAttributeValue(null, "numberTitle")));
        map.put("programme", safe(reader.getAttributeValue(null, "programme")));
        map.put("logicalId", safe(reader.getAttributeValue(null, "logicalId")));

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT
                    && "publicationUrl".equals(reader.getLocalName())) {
                map.put("publicationUrl", readSimpleText(reader));
            } else if (event == XMLStreamConstants.END_ELEMENT
                    && "regulation".equals(reader.getLocalName())) {
                break;
            }
        }

        return map;
    }

    private Map<String, Object> parseIdentification(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("identificationTypeCode", safe(reader.getAttributeValue(null, "identificationTypeCode")));
        map.put("number", safe(reader.getAttributeValue(null, "number")));
        map.put("countryIso2Code", safe(reader.getAttributeValue(null, "countryIso2Code")));
        map.put("countryDescription", safe(reader.getAttributeValue(null, "countryDescription")));
        map.put("logicalId", safe(reader.getAttributeValue(null, "logicalId")));

        consumeToEnd(reader, "identification");
        return map;
    }

    private Map<String, Object> parseAddress(XMLStreamReader reader) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("street", safe(reader.getAttributeValue(null, "street")));
        map.put("place", safe(reader.getAttributeValue(null, "place")));
        map.put("city", safe(reader.getAttributeValue(null, "city")));
        map.put("region", safe(reader.getAttributeValue(null, "region")));
        map.put("zipCode", safe(reader.getAttributeValue(null, "zipCode")));
        map.put("poBox", safe(reader.getAttributeValue(null, "poBox")));
        map.put("countryIso2Code", safe(reader.getAttributeValue(null, "countryIso2Code")));
        map.put("countryDescription", safe(reader.getAttributeValue(null, "countryDescription")));
        map.put("logicalId", safe(reader.getAttributeValue(null, "logicalId")));

        consumeToEnd(reader, "address");
        return map;
    }

    private void consumeToEnd(XMLStreamReader reader, String closingTag) throws Exception {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.END_ELEMENT
                    && closingTag.equals(reader.getLocalName())) {
                break;
            }
        }
    }

    private String readSimpleText(XMLStreamReader reader) throws Exception {
        return safe(reader.getElementText());
    }

    private String value(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }
}