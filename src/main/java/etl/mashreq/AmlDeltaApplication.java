package etl.mashreq;

import etl.mashreq.service.ProcessingOrchestrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import etl.mashreq.config.SanctionsProperties;

@SpringBootApplication
@EnableConfigurationProperties(SanctionsProperties.class)
public class AmlDeltaApplication implements CommandLineRunner {

    @Autowired
    private ProcessingOrchestrator processingOrchestrator;

    public static void main(String[] args) {
        SpringApplication.run(AmlDeltaApplication.class, args);
    }

    @Override
    public void run(String... args) {
        processingOrchestrator.processAll();
    }
}