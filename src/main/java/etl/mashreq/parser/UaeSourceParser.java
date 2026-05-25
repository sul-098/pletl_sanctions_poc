package etl.mashreq.parser;

import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.parser.uae.UaeExcelParser;
import etl.mashreq.parser.uae.UaeFormat;
import etl.mashreq.parser.uae.UaeFormatDetector;
import etl.mashreq.parser.uae.UaePdfParser;
import etl.mashreq.parser.uae.UaeXmlParser;
import etl.mashreq.service.CanonicalizationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

/**
 * Top-level UAE parser registered with SanctionsParserFactory.
 *
 * Detects the file format and delegates to the appropriate strategy:
 *   XML   → UaeXmlParser
 *   XLSX/XLS → UaeExcelParser
 *   PDF   → UaePdfParser
 *
 * Adding a new UAE format = add a new strategy class + one case in the switch.
 * Existing parsers are never touched.
 *
 * All three strategies produce identical canonical output regardless of source
 * format, so the diff engine and DB persistence layer are format-agnostic.
 */
@Component
@Slf4j
public class UaeSourceParser implements SanctionsSourceParser {

    private final UaeXmlParser   xmlParser;
    private final UaeExcelParser excelParser;
    private final UaePdfParser   pdfParser;

    public UaeSourceParser(CanonicalizationService canonicalizationService) {
        this.xmlParser   = new UaeXmlParser(canonicalizationService);
        this.excelParser = new UaeExcelParser(canonicalizationService);
        this.pdfParser   = new UaePdfParser(canonicalizationService);
    }

    @Override
    public boolean supports(String sourceId) {
        return "uae".equalsIgnoreCase(sourceId);
    }

    @Override
    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        UaeFormat format = UaeFormatDetector.detect(file);

        log.info("UAE parser dispatching: file={} format={}", file.getFileName(), format);

        return switch (format) {
            case XML   -> xmlParser.parse(file, source);
            case EXCEL -> excelParser.parse(file, source);
            case PDF   -> pdfParser.parse(file, source);
        };
    }
}
