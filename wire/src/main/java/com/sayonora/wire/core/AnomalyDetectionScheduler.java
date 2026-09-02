package com.sayonora.wire.core;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically compares each protocol's traffic RATE against its own recent baseline and, on a
 * real deterministic threshold breach, asks the LLM to phrase ONE short plain-English note about
 * it -- same "runs on a fixed interval, unset means the feature doesn't exist" shape as {@link
 * StatisticsScheduler}. Deliberately NOT "ask the LLM whether this is anomalous": that decision
 * is cheap, deterministic, and needs to be true every time or it isn't worth surfacing at all --
 * the LLM's only job here is turning numbers a human would otherwise have to interpret into one
 * readable sentence, and it's entirely optional (no LLM configured just means the raw numbers are
 * recorded with {@code narrative == null}, still useful on their own).
 *
 * <p>Reads {@link StatsCollectorStage#sqlMetricsSnapshot()}'s CUMULATIVE fields only
 * ({@code protocolCounts}, never {@code readsPerSec}/{@code writesPerSec}) and computes its own
 * rate from its own previous poll -- {@code SqlMetricsCollector.snapshot()}'s rate fields share
 * one mutable "since the last ANY caller's poll" window with every other consumer (the admin
 * {@code /api/metrics/summary} endpoint polls the same collector on every request), so relying on
 * them here would silently corrupt with whatever cadence an admin dashboard happens to be
 * refreshed at. Cumulative counters have no such coupling: a delta over this scheduler's own fixed
 * interval is exactly this scheduler's own rate, regardless of who else polls in between.
 *
 * <p>Baseline is a simple per-protocol exponential moving average, updated every cycle regardless
 * of whether that cycle was itself flagged -- a real, sustained traffic shift is expected to
 * become the new "normal" after enough cycles rather than alerting forever; this is the standard
 * EMA-baseline tradeoff, not a bug. In-memory only, like {@link StatsCollectorStage}'s own
 * counters: a restart losing the baseline (a few cycles of reduced sensitivity while it re-learns)
 * is an acceptable cost for not needing a persistence layer for what is fundamentally an advisory
 * signal, not a source of truth anything else depends on.
 */
public final class AnomalyDetectionScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionScheduler.class);
    private static final double EMA_ALPHA = 0.3;
    private static final int NOTES_RING_SIZE = 100;

    public record AnomalyNote(Instant timestamp, String protocol, double baselinePerSec, double currentPerSec,
            double ratio, String narrative) {
    }

    private final StatsCollectorStage statsStage;
    private final Supplier<TranslationLlmClient> llmClientSupplier;
    private final double ratioThreshold;
    private final double minRatePerSec;
    private final ScheduledExecutorService executor;

    private final Map<String, Long> lastCounts = new LinkedHashMap<>();
    private final Map<String, Double> baselinePerSec = new LinkedHashMap<>();
    private long lastPollNanos = -1;

    // Bounded, most-recent-first -- same "in-memory ring, a restart losing it is fine" tradeoff
    // AuditLog's own ring buffer already makes for a similar advisory/observability signal.
    private final Deque<AnomalyNote> notes = new ArrayDeque<>(NOTES_RING_SIZE);

    private AnomalyDetectionScheduler(StatsCollectorStage statsStage, Supplier<TranslationLlmClient> llmClientSupplier,
            double ratioThreshold, double minRatePerSec) {
        this.statsStage = statsStage;
        this.llmClientSupplier = llmClientSupplier;
        this.ratioThreshold = ratioThreshold;
        this.minRatePerSec = minRatePerSec;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-anomaly-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /** {@code null} (no scheduler constructed, no background thread) unless {@code
     * WARP_ANOMALY_SCAN_INTERVAL_MINUTES} is set to a positive integer -- same default-off
     * convention {@link StatisticsScheduler#startIfConfigured} uses. The LLM narration step is
     * independently optional: this still runs (and still records purely numeric anomalies) with
     * no LLM configured at all, since {@code llmClientSupplier} is consulted fresh every cycle,
     * the same "read the current hot-reloadable client at call time" pattern {@link
     * QueryRepairStage} uses. */
    public static AnomalyDetectionScheduler startIfConfigured(StatsCollectorStage statsStage,
            Supplier<TranslationLlmClient> llmClientSupplier) {
        int intervalMinutes = intEnv("WARP_ANOMALY_SCAN_INTERVAL_MINUTES", 0);
        if (intervalMinutes <= 0) {
            return null;
        }
        double ratioThreshold = doubleEnv("WARP_ANOMALY_RATIO_THRESHOLD", 3.0);
        double minRatePerSec = doubleEnv("WARP_ANOMALY_MIN_RATE_PER_SEC", 0.5);
        AnomalyDetectionScheduler scheduler =
                new AnomalyDetectionScheduler(statsStage, llmClientSupplier, ratioThreshold, minRatePerSec);
        scheduler.executor.scheduleWithFixedDelay(scheduler::runCycleSafely, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
        log.info("anomaly detection: scanning traffic every {} minute(s) (ratio threshold={}x, min rate={}/s)",
                intervalMinutes, ratioThreshold, minRatePerSec);
        return scheduler;
    }

    /** Test-only entry point: builds a scheduler without registering it on a fixed-delay
     * executor, so {@link #runCycle()} can be invoked directly and deterministically instead of
     * waiting on real minute-granularity scheduling -- same reasoning {@code StatisticsScheduler}
     * gives {@code runCycle()}'s own package-private visibility. */
    static AnomalyDetectionScheduler forTesting(StatsCollectorStage statsStage,
            Supplier<TranslationLlmClient> llmClientSupplier, double ratioThreshold, double minRatePerSec) {
        return new AnomalyDetectionScheduler(statsStage, llmClientSupplier, ratioThreshold, minRatePerSec);
    }

    private void runCycleSafely() {
        try {
            runCycle();
        } catch (RuntimeException e) {
            log.warn("anomaly detection: scan cycle failed, will retry next interval ({})", e.toString());
        }
    }

    void runCycle() {
        SqlMetricsCollector.Snapshot snapshot = statsStage.sqlMetricsSnapshot();
        long now = System.nanoTime();
        if (lastPollNanos < 0) {
            // First cycle: nothing to compare against yet -- just seed lastCounts/lastPollNanos.
            lastCounts.putAll(snapshot.protocolCounts());
            lastPollNanos = now;
            return;
        }
        double elapsedSec = Math.max((now - lastPollNanos) / 1_000_000_000.0, 1.0);
        lastPollNanos = now;

        for (var entry : snapshot.protocolCounts().entrySet()) {
            String protocol = entry.getKey();
            long current = entry.getValue();
            long previous = lastCounts.getOrDefault(protocol, current);
            lastCounts.put(protocol, current);
            double ratePerSec = Math.max((current - previous) / elapsedSec, 0);

            Double baseline = baselinePerSec.get(protocol);
            if (baseline == null) {
                baselinePerSec.put(protocol, ratePerSec);
                continue;
            }
            if (ratePerSec >= minRatePerSec && baseline > 0 && ratePerSec / baseline >= ratioThreshold) {
                recordAnomaly(protocol, baseline, ratePerSec, snapshot);
            }
            baselinePerSec.put(protocol, baseline + EMA_ALPHA * (ratePerSec - baseline));
        }
    }

    private void recordAnomaly(String protocol, double baseline, double current, SqlMetricsCollector.Snapshot snapshot) {
        double ratio = current / baseline;
        String narrative = null;
        TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
        if (llmClient != null) {
            try {
                narrative = llmClient.summarizeAnomaly(protocol, baseline, current, ratio, snapshot.topSql());
            } catch (Exception e) {
                log.warn("anomaly detection: LLM narration failed, recording the raw numbers only: {}", e.getMessage());
            }
        }
        AnomalyNote note = new AnomalyNote(Instant.now(), protocol, baseline, current, ratio, narrative);
        log.info("anomaly detection: {} traffic at {}/s, {}x its recent baseline of {}/s{}",
                protocol, String.format(java.util.Locale.ROOT, "%.2f", current), String.format(java.util.Locale.ROOT, "%.1f", ratio),
                String.format(java.util.Locale.ROOT, "%.2f", baseline), narrative == null ? "" : " -- " + narrative);
        synchronized (notes) {
            if (notes.size() >= NOTES_RING_SIZE) {
                notes.removeLast();
            }
            notes.addFirst(note);
        }
    }

    /** Most-recent-first, for the admin API. */
    public List<AnomalyNote> recentNotes(int limit) {
        synchronized (notes) {
            return notes.stream().limit(limit).toList();
        }
    }

    private static int intEnv(String name, int defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static double doubleEnv(String name, double defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
