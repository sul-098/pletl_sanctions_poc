package etl.mashreq;

import etl.mashreq.service.ProcessingOrchestrator;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class HealthController {

    private final ProcessingOrchestrator processingOrchestrator;

    @GetMapping("/health")
    public String health() {
        return "Sanctions POC is running";
    }

    @GetMapping("/process")
    public String process() {
        processingOrchestrator.processAll();
        return "Processing triggered successfully";
    }
}
