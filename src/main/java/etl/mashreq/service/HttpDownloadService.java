package etl.mashreq.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Service
@Slf4j
public class HttpDownloadService {

    private final HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .connectTimeout(Duration.ofSeconds(30))
            .build();

    public InputStream download(String url) {
        try {
            log.info("Downloading from URL={}", url);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(2))
                    .header("User-Agent", "Mozilla/5.0 AML-Sanctions-POC/1.0")
                    .header("Accept", "application/xml, text/xml, application/octet-stream, */*")
                    .GET()
                    .build();

            HttpResponse<InputStream> response =
                    client.send(request, HttpResponse.BodyHandlers.ofInputStream());

            int status = response.statusCode();
            log.info("Download response status={} finalUri={}", status, response.uri());

            if (status < 200 || status >= 300) {
                throw new RuntimeException("Download failed with status=" + status + " for finalUri=" + response.uri());
            }

            return response.body();

        } catch (Exception e) {
            throw new RuntimeException("Download failed for URL=" + url, e);
        }
    }
}