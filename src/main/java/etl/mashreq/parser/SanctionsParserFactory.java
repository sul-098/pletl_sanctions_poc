package etl.mashreq.parser;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class SanctionsParserFactory {

    private final List<SanctionsSourceParser> parsers;

    public SanctionsSourceParser getParser(String sourceId) {
        return parsers.stream()
                .filter(p -> p.supports(sourceId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No parser found for source: " + sourceId));
    }
}