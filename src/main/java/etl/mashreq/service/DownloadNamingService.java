package etl.mashreq.service;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class DownloadNamingService {

    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public String generateFileName(String prefix) {
        return prefix + "-" + LocalDateTime.now().format(FORMAT) + ".xml";
    }
}