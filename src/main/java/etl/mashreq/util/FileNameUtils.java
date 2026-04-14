package etl.mashreq.util;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FileNameUtils {

    private FileNameUtils() {}

    public static Comparator<Path> comparatorByEmbeddedTimestamp(String regex) {
        Pattern pattern = Pattern.compile(regex);

        return Comparator.comparing(path -> {
            Matcher matcher = pattern.matcher(path.getFileName().toString());
            if (matcher.matches() && matcher.groupCount() >= 1) {
                return matcher.group(1);
            }
            return "";
        });
    }
}
