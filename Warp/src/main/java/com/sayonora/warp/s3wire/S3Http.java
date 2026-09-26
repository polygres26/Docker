package com.sayonora.warp.s3wire;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/** Small pure HTTP/S3 semantics used by {@link PostgresObjectStore}: ranges, conditionals, ETags, upload ids. */
final class S3Http {

    private S3Http() {
    }

    // ---- Range -----------------------------------------------------------------------------------

    /** Inclusive byte range within an object. */
    record ByteRange(long start, long end) {
        long length() {
            return end - start + 1;
        }
    }

    /**
     * Parses a single-range {@code Range: bytes=...} header against an object of {@code size} bytes.
     *
     * @return the satisfiable range, or {@code null} when the header must be ignored (absent, malformed,
     *         or multiple ranges -- S3 serves the whole object then)
     * @throws S3WireException 416 InvalidRange when the range starts beyond the end
     */
    static ByteRange parseRange(String header, long size) {
        if (header == null) {
            return null;
        }
        String h = header.trim();
        if (!h.regionMatches(true, 0, "bytes=", 0, 6)) {
            return null;
        }
        String spec = h.substring(6).trim();
        if (spec.contains(",")) {
            return null;
        }
        int dash = spec.indexOf('-');
        if (dash < 0) {
            return null;
        }
        String a = spec.substring(0, dash).trim();
        String b = spec.substring(dash + 1).trim();
        try {
            if (a.isEmpty()) {
                if (b.isEmpty()) {
                    return null;
                }
                long n = Long.parseLong(b);
                if (n <= 0 || size == 0) {
                    throw invalidRange();
                }
                return new ByteRange(Math.max(0, size - n), size - 1);
            }
            long start = Long.parseLong(a);
            long end = b.isEmpty() ? size - 1 : Long.parseLong(b);
            if (start < 0 || !b.isEmpty() && end < start) {
                return null;
            }
            if (start >= size) {
                throw invalidRange();
            }
            return new ByteRange(start, Math.min(end, size - 1));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static S3WireException invalidRange() {
        return new S3WireException(416, "InvalidRange", "The requested range is not satisfiable");
    }

    // ---- conditional requests --------------------------------------------------------------------

    /** Outcome of evaluating conditional headers: 200 = proceed, 304 = not modified, 412 = precondition failed. */
    static int evaluate(ObjectStore.Conditions c, String eTag, Instant lastModified) {
        Instant lm = lastModified.truncatedTo(ChronoUnit.SECONDS);
        boolean ifMatchPresent = c.ifMatch() != null;
        if (ifMatchPresent && !etagMatches(c.ifMatch(), eTag)) {
            return 412;
        }
        if (!ifMatchPresent && c.ifUnmodifiedSince() != null) {
            Instant t = parseHttpDate(c.ifUnmodifiedSince());
            if (t != null && lm.isAfter(t)) {
                return 412;
            }
        }
        // S3 (and MinIO) evaluate both: a matching If-None-Match OR an unmodified-since If-Modified-Since answers 304
        // (RFC 7232 would ignore If-Modified-Since when If-None-Match is present)
        if (c.ifNoneMatch() != null && etagMatches(c.ifNoneMatch(), eTag)) {
            return 304;
        }
        if (c.ifModifiedSince() != null) {
            Instant t = parseHttpDate(c.ifModifiedSince());
            if (t != null && !lm.isAfter(t)) {
                return 304;
            }
        }
        return 200;
    }

    static boolean etagMatches(String header, String eTag) {
        String bare = strip(eTag);
        for (String part : header.split(",")) {
            String p = part.trim();
            if (p.equals("*") || strip(p).equals(bare)) {
                return true;
            }
        }
        return false;
    }

    private static String strip(String etag) {
        String e = etag.trim();
        if (e.startsWith("W/")) {
            e = e.substring(2);
        }
        if (e.length() >= 2 && e.startsWith("\"") && e.endsWith("\"")) {
            e = e.substring(1, e.length() - 1);
        }
        return e;
    }

    static Instant parseHttpDate(String s) {
        try {
            return ZonedDateTime.parse(s.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        } catch (RuntimeException e) {
            return null;
        }
    }

    // ---- ETags -----------------------------------------------------------------------------------

    static String quote(byte[] md5) {
        return "\"" + S3SigV4Verifier.hex(md5) + "\"";
    }

    /** S3's multipart ETag: md5 over the concatenated binary part md5s, then {@code -<parts>}. */
    static String multipartEtag(List<byte[]> partMd5s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (byte[] p : partMd5s) {
                md.update(p);
            }
            return "\"" + S3SigV4Verifier.hex(md.digest()) + "-" + partMd5s.size() + "\"";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] unhex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    // ---- upload ids ------------------------------------------------------------------------------

    /** {@code base64url(hostName).<uuid hex>}: the shard that holds the upload is readable from the id alone. */
    static String encodeUploadId(String host, UUID id) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(host.getBytes(StandardCharsets.UTF_8))
                + "." + id.toString().replace("-", "");
    }

    record UploadRef(String host, UUID id) {
    }

    /** @return null when {@code uploadId} is not one of ours */
    static UploadRef decodeUploadId(String uploadId) {
        if (uploadId == null) {
            return null;
        }
        int dot = uploadId.lastIndexOf('.');
        if (dot <= 0 || uploadId.length() - dot - 1 != 32) {
            return null;
        }
        try {
            String host = new String(Base64.getUrlDecoder().decode(uploadId.substring(0, dot)), StandardCharsets.UTF_8);
            String h = uploadId.substring(dot + 1);
            UUID id = UUID.fromString(h.substring(0, 8) + "-" + h.substring(8, 12) + "-" + h.substring(12, 16) + "-"
                    + h.substring(16, 20) + "-" + h.substring(20));
            return new UploadRef(host, id);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
