package com.sayonora.wire.dynamowire;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background expiry for tables with TimeToLive enabled: periodically deletes items whose TTL
 * attribute (a Number holding epoch seconds) is in the past. Like DynamoDB, expiry is not
 * instantaneous -- an expired item can still be read until the next sweep. Every Warp node runs a
 * sweeper (deletes are idempotent); on a sharded store each sweep visits every shard.
 */
final class TtlSweeper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TtlSweeper.class);
    private static final int BATCH = 1000;

    private final PgItemStore store;
    private final com.sayonora.wire.cluster.RowCache cache;
    private final long intervalMs;
    private ScheduledExecutorService executor;

    TtlSweeper(PgItemStore store, com.sayonora.wire.cluster.RowCache cache, long intervalMs) {
        this.store = store;
        this.cache = cache;
        this.intervalMs = intervalMs;
    }

    static long configuredIntervalMs() {
        String v = System.getProperty("warp.dynamowire.ttl.sweep.ms", System.getenv("WARP_DYNAMOWIRE_TTL_SWEEP_MS"));
        if (v == null || v.isBlank()) return 30_000L;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return 30_000L;
        }
    }

    void start() {
        if (intervalMs <= 0) {
            log.info("dynamowire: TTL sweeper disabled");
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dynamowire-ttl-sweeper");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(() -> {
            try {
                sweepOnce();
            } catch (RuntimeException e) {
                log.warn("dynamowire: TTL sweep failed: {}", e.toString());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("dynamowire: TTL sweeper running every {} ms", intervalMs);
    }

    /** One pass over every TTL-enabled table; returns the number of items deleted. */
    int sweepOnce() {
        int total = 0;
        long now = System.currentTimeMillis() / 1000;
        List<TableSchema> tables = store.tablesWithTtl();
        for (TableSchema t : tables) {
            while (true) {
                List<String[]> deleted = store.sweepExpired(t, now, BATCH);
                total += deleted.size();
                if (cache != null) {
                    String pg = store.tableToPgName(t.tableName());
                    for (String[] d : deleted) {
                        cache.invalidate(com.sayonora.wire.cluster.RowCache.key(pg, d[0], t.hasSortKey() ? d[1] : null));
                    }
                }
                if (deleted.size() < BATCH) break;
            }
        }
        return total;
    }

    @Override
    public void close() {
        if (executor != null) executor.shutdownNow();
    }
}
