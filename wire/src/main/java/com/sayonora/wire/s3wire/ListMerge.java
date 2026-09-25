package com.sayonora.wire.s3wire;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Cross-shard listing: each shard returns its own sorted page of entries (objects and rolled-up common
 * prefixes); this class k-way merges them in key order, drops duplicate common prefixes (one prefix can
 * exist on several shards) and truncates to max-keys. The continuation token is just the last returned
 * entry (its kind and string), so a token stays valid whatever the shards hold in between.
 */
final class ListMerge {

    private ListMerge() {
    }

    /** One listing entry: an object (obj != null) or a common prefix. */
    record Entry(String sortKey, S3Xml.ObjectEntry obj) {
        boolean isPrefix() {
            return obj == null;
        }
    }

    record Merged(List<Entry> entries, boolean truncated) {
    }

    /**
     * @param perShard each list sorted by {@link S3Keys#compare}; a shard that has more entries than it
     *                 returned must have returned at least {@code max + 1}
     */
    static Merged merge(List<List<Entry>> perShard, int max) {
        int k = perShard.size();
        int[] idx = new int[k];
        List<Entry> out = new ArrayList<>();
        boolean truncated = false;
        Entry last = null;
        while (true) {
            int best = -1;
            for (int i = 0; i < k; i++) {
                if (idx[i] < perShard.get(i).size()) {
                    if (best < 0 || S3Keys.compare(perShard.get(i).get(idx[i]).sortKey(),
                            perShard.get(best).get(idx[best]).sortKey()) < 0) {
                        best = i;
                    }
                }
            }
            if (best < 0) {
                break;
            }
            Entry e = perShard.get(best).get(idx[best]++);
            if (last != null && e.isPrefix() && last.isPrefix() && last.sortKey().equals(e.sortKey())) {
                continue; // same common prefix reported by another shard
            }
            if (out.size() == max) {
                truncated = true;
                break;
            }
            out.add(e);
            last = e;
        }
        return new Merged(out, truncated);
    }

    // ---- continuation token ----------------------------------------------------------------------

    /** Where to resume: {@code after} is exclusive for a key, or the first string past a whole common prefix. */
    record Marker(String value, boolean inclusive) {
    }

    static String encodeToken(Entry last) {
        String raw = (last.isPrefix() ? "P" : "K") + last.sortKey();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /** @return null when the token is exhausted (a common prefix ending the key space); throws 400 when malformed */
    static Marker decodeToken(String token) {
        String raw;
        try {
            raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new S3WireException(400, "InvalidArgument", "The continuation token provided is incorrect");
        }
        if (raw.isEmpty() || raw.charAt(0) != 'K' && raw.charAt(0) != 'P') {
            throw new S3WireException(400, "InvalidArgument", "The continuation token provided is incorrect");
        }
        String v = raw.substring(1);
        if (raw.charAt(0) == 'K') {
            return new Marker(v, false);
        }
        String ub = S3Keys.prefixUpperBound(v);
        return ub == null ? null : new Marker(ub, true);
    }

    /** True when {@code marker} is a common prefix already returned (v1 marker = a previous NextMarker). */
    static boolean isRolledUpPrefix(String marker, String prefix, String delimiter) {
        return delimiter != null && !delimiter.isEmpty() && marker.startsWith(prefix) && marker.endsWith(delimiter)
                && marker.length() > prefix.length()
                && marker.indexOf(delimiter, prefix.length()) == marker.length() - delimiter.length();
    }
}
