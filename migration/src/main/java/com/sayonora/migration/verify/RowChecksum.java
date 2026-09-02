package com.sayonora.migration.verify;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

/**
 * A per-row checksum (MD5 of {@code id||content}, folded to a {@code long}) combined across all
 * rows via XOR -- deliberately commutative and associative, so the combined checksum is the SAME
 * regardless of the order rows are visited in. That matters here specifically because source and
 * target are read through completely different paths (a Mongo cursor vs. a JDBC ResultSet) with no
 * guaranteed shared ordering, and a real migration may have several partitions read in parallel in
 * any order -- a checksum that cared about order would flag false mismatches on perfectly correct
 * data.
 *
 * <p>MD5 here is used purely as a fast, well-distributed hash for drift DETECTION, not for any
 * security property -- collisions matter only in the "did we get unlucky enough to miss a real
 * difference" sense, not an adversarial one, so MD5's cryptographic weaknesses are irrelevant to
 * this use.
 */
public final class RowChecksum {

    private RowChecksum() {
    }

    /** Combines this row's checksum into {@code runningChecksum} (start at {@code 0L} for the
     * first row) via XOR, hashing the two texts AS GIVEN -- correct only when both sides already
     * produce byte-identical text for equal content (true for {@code id}, since it's stored as
     * plain {@code text}, not reformatted). For a JSON/{@code jsonb} column, use {@link
     * #combineJson} instead. */
    public static long combine(long runningChecksum, String idText, String contentText) {
        return runningChecksum ^ hash(idText, contentText);
    }

    /** Same as {@link #combine}, but canonicalizes {@code jsonContent} first (recursively sorted
     * object keys, no incidental whitespace) before hashing -- REQUIRED whenever one side of the
     * comparison is Postgres {@code jsonb}: Postgres reorders object keys and reformats whitespace
     * on storage (confirmed live: comparing raw jsonb text output against the source's own JSON
     * text failed for every row, even ones that had replicated correctly, until this was added),
     * so raw-text comparison would flag every single row as a false mismatch, not just real drift.
     * Two JSON texts that are semantically equal but written differently (key order, spacing)
     * canonicalize to the same result and therefore hash identically. */
    public static long combineJson(long runningChecksum, String idText, String jsonContent) {
        return runningChecksum ^ hash(idText, canonicalizeJson(jsonContent));
    }

    private static String canonicalizeJson(String json) {
        return canonicalize(JsonParser.parseString(json)).toString();
    }

    private static JsonElement canonicalize(JsonElement element) {
        if (element.isJsonObject()) {
            JsonObject obj = element.getAsJsonObject();
            TreeMap<String, JsonElement> sorted = new TreeMap<>();
            for (var entry : obj.entrySet()) {
                sorted.put(entry.getKey(), canonicalize(entry.getValue()));
            }
            JsonObject canonical = new JsonObject();
            sorted.forEach(canonical::add);
            return canonical;
        }
        if (element.isJsonArray()) {
            JsonArray array = element.getAsJsonArray();
            JsonArray canonical = new JsonArray();
            array.forEach(e -> canonical.add(canonicalize(e)));
            return canonical;
        }
        return element;
    }

    private static long hash(String idText, String contentText) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(idText.getBytes(StandardCharsets.UTF_8));
            md5.update((byte) 0); // separator -- avoids "ab"+"c" colliding with "a"+"bc"
            md5.update(contentText.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md5.digest();
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (digest[i] & 0xFF);
            }
            return value;
        } catch (NoSuchAlgorithmException e) {
            // MD5 is a JDK-guaranteed MessageDigest algorithm (java.security.MessageDigest's own
            // spec) -- unreachable on any real JVM, not a real failure mode to design around.
            throw new IllegalStateException("MD5 not available", e);
        }
    }
}
