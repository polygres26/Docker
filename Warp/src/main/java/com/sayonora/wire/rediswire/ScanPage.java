package com.sayonora.wire.rediswire;

/** Cursor arithmetic for SSCAN/HSCAN/ZSCAN: small collections come back whole (like Redis listpacks), larger ones by offset. */
final class ScanPage {

    static final long WHOLE_LIMIT = 128;

    private ScanPage() {
    }

    /** {offset, limit, nextCursor}. */
    static long[] plan(long cursor, long count, long total) {
        if (total <= WHOLE_LIMIT && cursor == 0) {
            return new long[] {0, Math.max(total, 1), 0};
        }
        long off = cursor;
        long next = off + count >= total ? 0 : off + count;
        return new long[] {off, count, next};
    }
}
