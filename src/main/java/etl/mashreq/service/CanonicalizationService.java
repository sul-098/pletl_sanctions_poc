package etl.mashreq.service;

import  etl.mashreq.util.HashUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CanonicalizationService {

    private final ObjectMapper objectMapper;

    public Map<String, Object> canonicalizeMap(Map<String, Object> input) {
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            result.put(entry.getKey(), canonicalizeValue(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Object canonicalizeValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String str) {
            String normalized = StringUtils.normalizeSpace(str);
            return normalized.isBlank() ? null : normalized;
        }

        if (value instanceof Map<?, ?> map) {
            Map<String, Object> normalizedMap = new TreeMap<>();
            map.forEach((k, v) -> normalizedMap.put(String.valueOf(k), canonicalizeValue(v)));
            return normalizedMap.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (a, b) -> a,
                            TreeMap::new
                    ));
        }

        if (value instanceof List<?> list) {
            List<Object> normalized = list.stream()
                    .map(this::canonicalizeValue)
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparing(this::stableString))
                    .collect(Collectors.toList());
            return normalized;
        }

        return value;
    }

    public String computeHash(Map<String, Object> canonicalMap) {
        try {
            String json = objectMapper.writeValueAsString(canonicalMap);
            return HashUtils.sha256(json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize canonical map", e);
        }
    }

    private String stableString(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            return String.valueOf(o);
        }
    }
}
