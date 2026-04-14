package etl.mashreq.service;

import  etl.mashreq.config.SanctionsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SchedulerService {

    private final SanctionsProperties properties;
    private final ProcessingOrchestrator processingOrchestrator;

    @Scheduled(cron = "${sanctions.poll-cron}")
    public void poll() {
        if (!properties.isSchedulerEnabled()) {
            return;
        }

        log.info("Starting sanctions folder poll");
        processingOrchestrator.processAll();
        log.info("Completed sanctions folder poll");
    }
}
