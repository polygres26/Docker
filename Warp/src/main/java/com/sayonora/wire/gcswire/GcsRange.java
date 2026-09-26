package com.sayonora.wire.gcswire;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Range arithmetic of the resumable upload protocol and of media downloads.
 *
 * <p>Upload {@code Content-Range}: {@code bytes first-last/total}, {@code bytes first-last/*}, {@code bytes *}{@code /total}
 * (final empty request or "how far are you"), {@code bytes *}{@code /*} (status query). Every chunk except the last must be a
 * multiple of 256 KiB; the server persists only the aligned prefix of a non-final chunk and reports it in {@code Range}.
 */
final class GcsRange {

    static final int ALIGN = 256 * 1024;

    private GcsRange() {
    }

    /** {@code first}/{@code last} = -1 for a query ({@code bytes *}{@code /...}); {@code total} = -1 when unknown ({@code *}). */
    record ContentRange(long first, long last, long total) {
        boolean query() {
            return first < 0;
        }

        long length() {
            return query() ? 0 : last - first + 1;
        }
    }

    private static final Pattern CR = Pattern.compile("bytes\\s+(?:(\\d+)-(\\d+)|\\*)/(\\d+|\\*)");

    /** @return null when the header is malformed */
    static ContentRange parseContentRange(String h) {
        if (h == null) {
            return null;
        }
        Matcher m = CR.matcher(h.trim());
        if (!m.matches()) {
            return null;
        }
        long total = "*".equals(m.group(3)) ? -1 : Long.parseLong(m.group(3));
        if (m.group(1) == null) {
            return new ContentRange(-1, -1, total);
        }
        long a = Long.parseLong(m.group(1));
        long b = Long.parseLong(m.group(2));
        if (b < a || total >= 0 && b >= total) {
            return null;
        }
        return new ContentRange(a, b, total);
    }

    /** Bytes of a chunk of {@code length} that the server persists: everything when final, else the 256 KiB-aligned prefix. */
    static long persistable(long length, boolean last) {
        return last ? length : length / ALIGN * ALIGN;
    }

    /** {@code Range} header of a 308 answer: {@code bytes=0-(persisted-1)}, or null when nothing was persisted. */
    static String rangeHeader(long persisted) {
        return persisted <= 0 ? null : "bytes=0-" + (persisted - 1);
    }

    /** A download range resolved against a size: {@code start..end} inclusive. */
    record Span(long start, long end) {
        long length() {
            return end - start + 1;
        }
    }

    /**
     * Resolves a single {@code Range: bytes=...} header. Returns null for "serve everything" (absent, malformed or multi-range,
     * which GCS answers with the whole object), {@code Span(-1,-1)} when unsatisfiable.
     */
    static Span parseRange(String h, long size) {
        if (h == null || !h.trim().toLowerCase().startsWith("bytes=") || h.indexOf(',') >= 0) {
            return null;
        }
        String spec = h.trim().substring(6).trim();
        int dash = spec.indexOf('-');
        if (dash < 0) {
            return null;
        }
        String a = spec.substring(0, dash).trim();
        String b = spec.substring(dash + 1).trim();
        try {
            if (a.isEmpty()) {
                long n = Long.parseLong(b);
                if (n <= 0) {
                    return new Span(-1, -1);
                }
                if (size == 0) {
                    return new Span(-1, -1);
                }
                return new Span(Math.max(0, size - n), size - 1);
            }
            long start = Long.parseLong(a);
            long end = b.isEmpty() ? size - 1 : Math.min(Long.parseLong(b), size - 1);
            if (start >= size || end < start && !b.isEmpty() && Long.parseLong(b) < start) {
                return new Span(-1, -1);
            }
            return new Span(start, end);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
