package com.sayonora.wire.gcswire;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32C;

/** Hash encodings of GCS: {@code crc32c} = base64 of the big-endian 4 bytes, {@code md5Hash} = base64 of the digest. */
final class GcsHash {

    private GcsHash() {
    }

    static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static String crc32cB64(long crc) {
        byte[] b = {(byte) (crc >>> 24), (byte) (crc >>> 16), (byte) (crc >>> 8), (byte) crc};
        return Base64.getEncoder().encodeToString(b);
    }

    static String crc32c(byte[] data) {
        CRC32C c = new CRC32C();
        c.update(data, 0, data.length);
        return crc32cB64(c.getValue());
    }

    static String md5B64(byte[] data) {
        return Base64.getEncoder().encodeToString(md5().digest(data));
    }

    static String md5Hex(byte[] digest) {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /** Parses {@code X-Goog-Hash: crc32c=..,md5=..} (several header lines joined by comma) into algo -> base64 value. */
    static Map<String, String> parseGoogHash(String header) {
        Map<String, String> out = new LinkedHashMap<>();
        if (header == null) {
            return out;
        }
        for (String part : header.split(",")) {
            String p = part.trim();
            int eq = p.indexOf('=');
            if (eq > 0) {
                out.put(p.substring(0, eq).trim().toLowerCase(), p.substring(eq + 1).trim());
            }
        }
        return out;
    }

    static String googHash(String crc32c, String md5) {
        StringBuilder sb = new StringBuilder();
        if (crc32c != null) {
            sb.append("crc32c=").append(crc32c);
        }
        if (md5 != null) {
            sb.append(sb.length() > 0 ? "," : "").append("md5=").append(md5);
        }
        return sb.toString();
    }

    /** The opaque ETag GCS reports: a base64 token that changes with generation and metageneration. */
    static String etag(long generation, long metageneration) {
        return Base64.getEncoder().withoutPadding().encodeToString(
                (generation + "/" + metageneration).getBytes(StandardCharsets.UTF_8)) + "=";
    }

    static byte[] b64(String s) {
        return Base64.getDecoder().decode(s);
    }
}
