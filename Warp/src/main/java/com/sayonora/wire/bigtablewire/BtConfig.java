package com.sayonora.wire.bigtablewire;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Environment configuration of bigtablewire. */
final class BtConfig {

    /** Bearer tokens accepted (WARP_BIGTABLEWIRE_TOKENS, comma separated); empty = no authentication, like the emulator. */
    final Set<String> tokens;
    /** Period of the garbage-collection sweep of column family GC rules; 0 disables it (WARP_BIGTABLEWIRE_GC_INTERVAL_SECONDS). */
    final long gcIntervalSeconds;
    /** Cells fetched from Postgres per round trip of a scan (WARP_BIGTABLEWIRE_SCAN_PAGE_CELLS). */
    final int scanPageCells;
    /** Approximate stored bytes between two SampleRowKeys samples (WARP_BIGTABLEWIRE_SAMPLE_BYTES). */
    final long sampleBytes;
    /** Bytes of chunks after which ReadRows sends a response message. */
    final int responseBytes;

    BtConfig(Set<String> tokens, long gcIntervalSeconds, int scanPageCells, long sampleBytes, int responseBytes) {
        this.tokens = tokens;
        this.gcIntervalSeconds = gcIntervalSeconds;
        this.scanPageCells = scanPageCells;
        this.sampleBytes = sampleBytes;
        this.responseBytes = responseBytes;
    }

    static BtConfig fromEnv() {
        String t = System.getenv("WARP_BIGTABLEWIRE_TOKENS");
        Set<String> tokens = new HashSet<>();
        if (t != null) {
            Arrays.stream(t.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(tokens::add);
        }
        return new BtConfig(tokens, longEnv("WARP_BIGTABLEWIRE_GC_INTERVAL_SECONDS", 60),
                (int) Math.max(10, longEnv("WARP_BIGTABLEWIRE_SCAN_PAGE_CELLS", 2000)),
                Math.max(1, longEnv("WARP_BIGTABLEWIRE_SAMPLE_BYTES", 1L << 20)), 1 << 20);
    }

    static long longEnv(String name, long def) {
        String v = System.getenv(name);
        try {
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
