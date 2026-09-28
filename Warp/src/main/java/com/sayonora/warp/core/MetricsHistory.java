package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Fixed-size in-memory ring of periodic samples of the cumulative counters Warp already keeps, so the
 * admin UI can draw real trends. Each sample holds cumulative values (statements, errors, reads,
 * writes, per-protocol counts and round-trip totals, QoS admissions/rejections, cache hits/misses);
 * rates are the difference between two samples, computed by the reader. Node-local, and empty after a restart.
 */
public final class MetricsHistory {

    public record Sample(long timestampMillis, Map<String, Long> counters) {
    }

    private final int capacity;
    private final long intervalMillis;
    private final Supplier<Map<String, Long>> source;
    private final Sample[] ring;
    private int next;
    private int size;
    private ScheduledExecutorService scheduler;

    public MetricsHistory(int capacity, long intervalMillis, Supplier<Map<String, Long>> source) {
        this.capacity = Math.max(2, capacity);
        this.intervalMillis = Math.max(1, intervalMillis);
        this.source = source;
        this.ring = new Sample[this.capacity];
    }

    public int capacity() {
        return capacity;
    }

    public long intervalMillis() {
        return intervalMillis;
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-metrics-history");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::sampleQuietly, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    private void sampleQuietly() {
        try {
            sampleNow(System.currentTimeMillis());
        } catch (RuntimeException e) {
            // a failed sample just leaves a gap; never stop the sampler
        }
    }

    /** Takes one sample immediately (the scheduler calls this; tests call it directly). */
    public void sampleNow(long nowMillis) {
        Map<String, Long> counters = new LinkedHashMap<>(source.get());
        synchronized (this) {
            ring[next] = new Sample(nowMillis, counters);
            next = (next + 1) % capacity;
            size = Math.min(size + 1, capacity);
        }
    }

    /** Oldest first. */
    public synchronized List<Sample> samples() {
        List<Sample> out = new ArrayList<>(size);
        int start = size < capacity ? 0 : next;
        for (int i = 0; i < size; i++) {
            out.add(ring[(start + i) % capacity]);
        }
        return out;
    }

    /**
     * The standard source: cumulative counters from the stats stage, QoS stage and (optionally) cache.
     * {@code extra} may add further keys (e.g. cache counters) and may be null.
     */
    public static Supplier<Map<String, Long>> standardSource(StatsCollectorStage stats, QosControlStage qos,
            Supplier<Map<String, Long>> extra) {
        return () -> {
            Map<String, Long> m = new LinkedHashMap<>();
            long statements = 0;
            long errors = 0;
            for (StatsCollectorStage.Counters c : stats.snapshot().values()) {
                statements += c.statementCount().sum();
                errors += c.errorCount().sum();
            }
            m.put("statements", statements);
            m.put("errors", errors);
            SqlMetricsCollector sql = stats.sqlMetrics();
            m.put("reads", sql.totalReadCount());
            m.put("writes", sql.totalWriteCount());
            m.put("other", sql.totalOtherCount());
            sql.protocolCountsSnapshot().forEach((p, n) -> m.put("proto." + p, n));
            Map<String, long[]> rtt = new LinkedHashMap<>();
            for (SqlMetricsCollector.RttOutcomeStat r : sql.rttOutcomeSnapshot()) {
                long[] acc = rtt.computeIfAbsent(r.protocol(), k -> new long[2]);
                acc[0] += r.calls();
                acc[1] += r.totalMillis();
            }
            rtt.forEach((p, acc) -> {
                m.put("rttCalls." + p, acc[0]);
                m.put("rttMs." + p, acc[1]);
            });
            if (qos != null) {
                long admitted = 0;
                long rejected = 0;
                for (QosControlStage.Counters c : qos.snapshot().values()) {
                    admitted += c.admitted().sum();
                    rejected += c.rejected().sum();
                }
                m.put("qosAdmitted", admitted);
                m.put("qosRejected", rejected);
            }
            if (extra != null) {
                m.putAll(extra.get());
            }
            return m;
        };
    }
}
