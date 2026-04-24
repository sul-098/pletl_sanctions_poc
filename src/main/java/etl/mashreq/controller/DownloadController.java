package etl.mashreq.controller;

import etl.mashreq.service.DownloadOrchestrator;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class DownloadController {

    private final DownloadOrchestrator downloadOrchestrator;

    @GetMapping("/download/all")
    public String downloadAll() {
        downloadOrchestrator.downloadAll();
        return "Download triggered for all sources";
    }
}