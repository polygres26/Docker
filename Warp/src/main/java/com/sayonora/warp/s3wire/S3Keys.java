package com.sayonora.warp.s3wire;

import java.nio.charset.StandardCharsets;

/**
 * Key helpers for the Postgres object store. S3 lists keys in UTF-8 byte order, which equals Unicode
 * code point order; Postgres {@code COLLATE "C"} on a UTF-8 database compares the same way, and
 * Java's {@link String#compareTo} (UTF-16 code units) does NOT for supplementary characters, so every
 * comparison Warp does itself (the cross-shard merge) goes through {@link #compare}.
 */
final class S3Keys {

    static final int MAX_KEY_BYTES = 1024;

    private S3Keys() {
    }

    /** Code point (== UTF-8 byte) order. */
    static int compare(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    /**
     * Smallest string greater than every string that starts with {@code prefix} (in code point order),
     * or {@code null} when there is none (empty prefix, or only U+10FFFF characters). Used as the
     * exclusive upper bound of a btree range scan for a prefix and to skip a whole common prefix.
     */
    static String prefixUpperBound(String prefix) {
        int[] cps = prefix.codePoints().toArray();
        int n = cps.length;
        while (n > 0 && cps[n - 1] == Character.MAX_CODE_POINT) {
            n--;
        }
        if (n == 0) {
            return null;
        }
        int last = cps[n - 1] + 1;
        if (last >= 0xD800 && last <= 0xDFFF) {
            last = 0xE000; // surrogates are not valid text in Postgres
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n - 1; i++) {
            sb.appendCodePoint(cps[i]);
        }
        return sb.appendCodePoint(last).toString();
    }

    /** @throws S3WireException 400 for keys Postgres text cannot hold or that exceed 1024 UTF-8 bytes */
    static void validate(String key) {
        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new S3WireException(400, "KeyTooLongError", "Your key is too long");
        }
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c == 0) {
                throw new S3WireException(400, "InvalidArgument", "Object keys may not contain the NUL character");
            }
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= key.length() || !Character.isLowSurrogate(key.charAt(i + 1))) {
                    throw new S3WireException(400, "InvalidArgument", "Object key is not valid UTF-8");
                }
                i++;
            } else if (Character.isLowSurrogate(c)) {
                throw new S3WireException(400, "InvalidArgument", "Object key is not valid UTF-8");
            }
        }
    }

    /** The string hashed to pick the owning shard. Buckets cannot contain '/', so this is unambiguous. */
    static String shardKey(String bucket, String key) {
        return bucket + "/" + key;
    }
}
