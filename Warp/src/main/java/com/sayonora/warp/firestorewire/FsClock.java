package com.sayonora.warp.firestorewire;

import com.google.protobuf.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

/** Strictly increasing microsecond commit clock (per process). */
final class FsClock {

    private static final AtomicLong LAST = new AtomicLong();

    private FsClock() {
    }

    static long nowMicros() {
        java.time.Instant i = java.time.Instant.now();
        long now = i.getEpochSecond() * 1_000_000L + i.getNano() / 1000;
        return LAST.updateAndGet(prev -> Math.max(prev + 1, now));
    }

    /** Non-advancing wall clock (read times). */
    static long wallMicros() {
        java.time.Instant i = java.time.Instant.now();
        return i.getEpochSecond() * 1_000_000L + i.getNano() / 1000;
    }

    static Timestamp ts(long micros) {
        return Timestamp.newBuilder().setSeconds(Math.floorDiv(micros, 1_000_000L)).setNanos((int) Math.floorMod(micros, 1_000_000L) * 1000).build();
    }

    static long micros(Timestamp t) {
        return t.getSeconds() * 1_000_000L + t.getNanos() / 1000;
    }
}
