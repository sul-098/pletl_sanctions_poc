package etl.mashreq.parser.uae;

import etl.mashreq.util.HashUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared normalization logic for all UAE format parsers.
 *
 * Every format parser (XML, Excel, PDF) produces raw records as Map<String,Object>
 * with loosely-named keys. This helper converts those raw records into the single
 * canonical model that the diff engine and DB persistence layer expect.
 *
 * Canonical output keys (must match all other source parsers):
 *   sourceRef, listType, entityType, primaryName, nationality, dateOfBirth,
 *   remarks, aliases, addresses, documents, nationalities, datesOfBirth
 */
public final class UaeNormalizationHelper {

    private UaeNormalizationHelper() {}

    public static Map<String, Object> normalize(Map<String, Object> raw) {
        Map<String, Object> out = new LinkedHashMap<>();

        String sheetName = value(raw.get("sheetName"));

        String referenceNumber = firstNonBlank(raw,
                "reference_number", "reference", "ref_no", "listing_id", "id", "serial_no", "uid");

        String name = firstNonBlank(raw,
                "name", "full_name", "english_name", "listed_name",
                "individual_name", "entity_name");

        String entityType = firstNonBlank(raw, "entity_type", "type", "category");
        if (entityType == null) {
            entityType = inferEntityTypeFromSheet(sheetName);
        }

        String nationality = firstNonBlank(raw, "nationality", "citizenship", "country");
        String dob         = firstNonBlank(raw, "date_of_birth", "dob", "birth_date");
        String remarks     = firstNonBlank(raw, "remarks", "other_info", "comments");

        out.put("sourceRef",   referenceNumber);
        out.put("listType",    "UAE");
        out.put("entityType",  entityType);
        out.put("primaryName", name);
        out.put("remarks",     remarks);

        // Scalar convenience fields used by DB persistence
        out.put("nationality", nationality);
        out.put("dateOfBirth", dob);

        // Canonical collection fields — always present (empty list if no data)
        out.put("aliases",   new ArrayList<>());
        out.put("addresses", new ArrayList<>());
        out.put("documents", new ArrayList<>());
        out.put("nationalities", nationality != null
                ? List.of(Map.of("country", nationality))
                : new ArrayList<>());
        out.put("datesOfBirth", dob != null
                ? List.of(Map.of("date", dob))
                : new ArrayList<>());

        return out;
    }

    /**
     * Builds the business key for a normalized UAE record.
     * Prefers the stable sourceRef. Falls back to a composite key for
     * PDF/Excel rows that have no stable reference number.
     */
    public static String buildBusinessKey(Map<String, Object> normalized) {
        String sourceRef = value(normalized.get("sourceRef"));
        if (sourceRef != null) {
            return sourceRef;
        }

        String name        = value(normalized.get("primaryName"));
        String type        = value(normalized.get("entityType"));
        String dob         = value(normalized.get("dateOfBirth"));
        String nationality = value(normalized.get("nationality"));

        String composite = String.join("|",
                safeKey(type), safeKey(name), safeKey(dob), safeKey(nationality));

        if (composite.isBlank()) return null;
        composite = composite.toLowerCase();
        // Hash long composites to stay within business_key column limit (190 chars safe margin)
        if (composite.length() > 190) {
            composite = safeKey(type).toLowerCase() + "|" + HashUtils.sha256(composite);
        }
        return composite;
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    static String firstNonBlank(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object v = map.get(key);
            if (v != null && !v.toString().trim().isBlank()) return v.toString().trim();
        }
        return null;
    }

    static String value(Object v) {
        if (v == null) return null;
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private static String safeKey(String v) {
        return v == null ? "" : v.trim().replaceAll("\\s+", " ");
    }

    private static String inferEntityTypeFromSheet(String sheetName) {
        if (sheetName == null) return "UNKNOWN";
        String s = sheetName.toLowerCase();
        if (s.contains("individual") || s.contains("person"))           return "INDIVIDUAL";
        if (s.contains("entity") || s.contains("organization")
                || s.contains("organisation"))                           return "ENTITY";
        return sheetName;
    }
}
