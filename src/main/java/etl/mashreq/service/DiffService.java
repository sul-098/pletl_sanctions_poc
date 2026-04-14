package etl.mashreq.service;

import  etl.mashreq.domain.*;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

@Service
public class DiffService {

    public DeltaFile compare(NormalizedSanctionsFile previous, NormalizedSanctionsFile current) {
        Map<String, NormalizedEntry> prevEntries = previous.getEntries();
        Map<String, NormalizedEntry> currEntries = current.getEntries();

        Set<String> allKeys = new TreeSet<>();
        allKeys.addAll(prevEntries.keySet());
        allKeys.addAll(currEntries.keySet());

        List<DeltaEntry> changes = new ArrayList<>();

        int added = 0;
        int removed = 0;
        int updated = 0;
        int unchanged = 0;

        for (String key : allKeys) {
            NormalizedEntry prev = prevEntries.get(key);
            NormalizedEntry curr = currEntries.get(key);

            if (prev == null) {
                added++;
                changes.add(DeltaEntry.builder()
                        .changeType(ChangeType.ADDED)
                        .businessKey(key)
                        .current(curr.getAttributes())
                        .build());
                continue;
            }

            if (curr == null) {
                removed++;
                changes.add(DeltaEntry.builder()
                        .changeType(ChangeType.REMOVED)
                        .businessKey(key)
                        .previous(prev.getAttributes())
                        .build());
                continue;
            }

            if (Objects.equals(prev.getCanonicalHash(), curr.getCanonicalHash())) {
                unchanged++;
                continue;
            }

            updated++;
            List<FieldChange> fieldChanges = new ArrayList<>();
            compareObjects("", prev.getAttributes(), curr.getAttributes(), fieldChanges);

            changes.add(DeltaEntry.builder()
                    .changeType(ChangeType.UPDATED)
                    .businessKey(key)
                    .previous(prev.getAttributes())
                    .current(curr.getAttributes())
                    .fieldChanges(fieldChanges)
                    .build());
        }

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalCurrentEntries", currEntries.size());
        summary.put("totalPreviousEntries", prevEntries.size());
        summary.put("added", added);
        summary.put("removed", removed);
        summary.put("updated", updated);
        summary.put("unchanged", unchanged);

        return DeltaFile.builder()
                .sourceId(current.getSourceId())
                .sourceFile(current.getFileName())
                .previousFile(previous.getFileName())
                .generatedAt(Instant.now())
                .comparisonMode("DELTA")
                .summary(summary)
                .changes(changes)
                .build();
    }

    @SuppressWarnings("unchecked")
    private void compareObjects(String path, Object oldObj, Object newObj, List<FieldChange> changes) {
        if (Objects.equals(oldObj, newObj)) {
            return;
        }

        if (oldObj instanceof Map<?, ?> oldMap && newObj instanceof Map<?, ?> newMap) {
            Set<String> keys = new TreeSet<>();
            oldMap.keySet().forEach(k -> keys.add(String.valueOf(k)));
            newMap.keySet().forEach(k -> keys.add(String.valueOf(k)));

            for (String key : keys) {
                String nextPath = path.isBlank() ? key : path + "." + key;
                compareObjects(nextPath, ((Map<String, Object>) oldMap).get(key),
                        ((Map<String, Object>) newMap).get(key), changes);
            }
            return;
        }

        changes.add(FieldChange.builder()
                .fieldPath(path)
                .oldValue(oldObj)
                .newValue(newObj)
                .build());
    }

    public DeltaFile initialLoad(NormalizedSanctionsFile current) {
        List<DeltaEntry> changes = current.getEntries().values().stream()
                .map(entry -> DeltaEntry.builder()
                        .changeType(ChangeType.ADDED)
                        .businessKey(entry.getBusinessKey())
                        .current(entry.getAttributes())
                        .build())
                .toList();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalCurrentEntries", current.getEntries().size());
        summary.put("totalPreviousEntries", 0);
        summary.put("added", current.getEntries().size());
        summary.put("removed", 0);
        summary.put("updated", 0);
        summary.put("unchanged", 0);

        return DeltaFile.builder()
                .sourceId(current.getSourceId())
                .sourceFile(current.getFileName())
                .previousFile(null)
                .generatedAt(Instant.now())
                .comparisonMode("INITIAL_LOAD")
                .summary(summary)
                .changes(changes)
                .build();
    }
}
