package com.sayonora.warp.core;

/** Counts the transactions in a MySQL GTID set string ({@code uuid:1-5:7,uuid2:1-3}). */
final class GtidSets {

    private GtidSets() {
    }

    /** Number of transactions in {@code set}; 0 for null/blank. Tolerates newlines and spaces that
     * {@code SHOW REPLICA STATUS} inserts between uuids. */
    static long count(String set) {
        if (set == null || set.isBlank()) {
            return 0;
        }
        long total = 0;
        for (String source : set.replaceAll("\\s+", "").split(",")) {
            if (source.isEmpty()) {
                continue;
            }
            String[] parts = source.split(":");
            for (int i = 1; i < parts.length; i++) {
                String iv = parts[i];
                int dash = iv.indexOf('-');
                try {
                    if (dash < 0) {
                        Long.parseLong(iv);
                        total += 1;
                    } else {
                        long lo = Long.parseLong(iv.substring(0, dash));
                        long hi = Long.parseLong(iv.substring(dash + 1));
                        total += Math.max(0, hi - lo + 1);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("not a GTID set: " + set);
                }
            }
        }
        return total;
    }
}
