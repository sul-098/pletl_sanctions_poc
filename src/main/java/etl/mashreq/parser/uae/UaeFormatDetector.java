package etl.mashreq.parser.uae;

import java.nio.file.Path;

/**
 * Detects the UAE file format from the file extension.
 * Magic-byte detection can be added here in the future if extension-based
 * detection proves insufficient (e.g. files with wrong extensions).
 */
public final class UaeFormatDetector {

    private UaeFormatDetector() {}

    public static UaeFormat detect(Path file) {
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".xlsx") || name.endsWith(".xls")) return UaeFormat.EXCEL;
        if (name.endsWith(".pdf"))                           return UaeFormat.PDF;
        if (name.endsWith(".xml"))                           return UaeFormat.XML;
        throw new IllegalArgumentException(
                "Unsupported UAE file format — cannot determine parser for: " + file.getFileName());
    }
}
