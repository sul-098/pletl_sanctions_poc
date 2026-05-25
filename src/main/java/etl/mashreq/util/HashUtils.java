package etl.mashreq.util;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public final class HashUtils {

    private HashUtils() {}

    public static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return hexEncode(hash);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to hash string content", ex);
        }
    }

    /** Streams the file through SHA-256 without loading it fully into memory. */
    public static String sha256File(Path file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
                byte[] buf = new byte[8192];
                while (in.read(buf) != -1) { /* digest updated by DigestInputStream */ }
            }
            return hexEncode(digest.digest());
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to hash file: " + file, ex);
        }
    }

    private static String hexEncode(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
