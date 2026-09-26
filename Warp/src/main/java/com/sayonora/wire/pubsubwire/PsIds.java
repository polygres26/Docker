package com.sayonora.wire.pubsubwire;

import java.util.concurrent.atomic.AtomicLong;

/** Message ids: increasing decimal numbers like Pub/Sub's (time in ms, a random node part, a counter). */
final class PsIds {

    private static final long EPOCH = 1_577_836_800_000L; // 2020-01-01
    private static final long NODE = (long) (Math.random() * 255) << 12;
    private static final AtomicLong LAST = new AtomicLong();

    private PsIds() {
    }

    static String next() {
        long cand = ((System.currentTimeMillis() - EPOCH) << 20) | NODE;
        return Long.toString(LAST.updateAndGet(l -> Math.max(cand, l + 1)));
    }
}
