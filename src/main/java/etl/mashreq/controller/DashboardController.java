package etl.mashreq.controller;

import etl.mashreq.domain.ChangeType;
import etl.mashreq.domain.DeltaEntry;
import etl.mashreq.domain.DeltaFile;
import etl.mashreq.service.DeltaDashboardService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Collections;
import java.util.List;

@Controller
@RequiredArgsConstructor
public class DashboardController {

    private final DeltaDashboardService dashboardService;

    @GetMapping("/dashboard")
    public String dashboard(Model model) {
        model.addAttribute("sources", List.of("ofac", "hmt", "un", "eu"));
        model.addAttribute("selectedSource", null);
        model.addAttribute("delta", null);
        model.addAttribute("addedChanges", Collections.emptyList());
        model.addAttribute("removedChanges", Collections.emptyList());
        model.addAttribute("updatedChanges", Collections.emptyList());
        return "dashboard";
    }

    @GetMapping("/dashboard/{source}")
    public String dashboardBySource(@PathVariable String source, Model model) {
        DeltaFile delta = dashboardService.loadLatestDelta(source).orElse(null);

        List<DeltaEntry> allChanges =
                (delta != null && delta.getChanges() != null) ? delta.getChanges() : Collections.emptyList();

        List<DeltaEntry> addedChanges = allChanges.stream()
                .filter(c -> c != null && c.getChangeType() == ChangeType.ADDED)
                .toList();

        List<DeltaEntry> removedChanges = allChanges.stream()
                .filter(c -> c != null && c.getChangeType() == ChangeType.REMOVED)
                .toList();

        List<DeltaEntry> updatedChanges = allChanges.stream()
                .filter(c -> c != null && c.getChangeType() == ChangeType.UPDATED)
                .toList();

        model.addAttribute("sources", List.of("ofac", "hmt", "un", "eu"));
        model.addAttribute("selectedSource", source);
        model.addAttribute("delta", delta);
        model.addAttribute("addedChanges", addedChanges);
        model.addAttribute("removedChanges", removedChanges);
        model.addAttribute("updatedChanges", updatedChanges);

        return "dashboard";
    }
}