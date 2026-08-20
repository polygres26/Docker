package com.polygres.wire.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

/**
 * What customers actually want to see on a metrics dashboard: which wire protocol is carrying
 * traffic, how many reads/writes per second, and which SQL is costing the most. Fed by
 * {@link StatsCollectorStage} on every statement that passes through the shared pipeline (pgwire,
 * mywire, mssqlwire, orawire -- see the {@code pipelineStages} wiring in {@code Main}; mongowire
 * and dynamowire have their own request paths and aren't reflected here). Deliberately not a
 * Prometheus counter: this holds enough shape (normalized SQL text, per-protocol breakdown) that
 * a plain counter can't capture, and is read back as one JSON snapshot by the admin API.
 */
public final class SqlMetricsCollector {

    private static final int TOP_SQL_CAP = 500;
    private static final Pattern STRING_LITERAL = Pattern.compile("'(?:[^']|'')*'");
    private static final Pattern NUMBER_LITERAL = Pattern.compile("\\b\\d+(?:\\.\\d+)?\\b");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    public enum StatementKind { READ, WRITE, OTHER }

    public record SqlStat(String normalizedSql, long calls, long totalMillis, long avgMillis) {
    }

    public record BackendStat(String backend, long calls, long reads, long writes, long totalMillis, long avgMillis) {
    }

    public record Snapshot(
            Map<String, Long> protocolCounts,
            long totalReads,
            long totalWrites,
            long totalOther,
            double readsPerSec,
            double writesPerSec,
            List<SqlStat> topSql,
            List<BackendStat> byBackend) {
    }

    private static final class SqlEntry {
        final String normalizedSql;
        final LongAdder calls = new LongAdder();
        final LongAdder totalNanos = new LongAdder();

        SqlEntry(String normalizedSql) {
            this.normalizedSql = normalizedSql;
        }
    }

    private static final class BackendEntry {
        final LongAdder calls = new LongAdder();
        final LongAdder reads = new LongAdder();
        final LongAdder writes = new LongAdder();
        final LongAdder totalNanos = new LongAdder();
    }

    private final ConcurrentHashMap<String, LongAdder> byProtocol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SqlEntry> sqlStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BackendEntry> byBackend = new ConcurrentHashMap<>();
    private final LongAdder totalReads = new LongAdder();
    private final LongAdder totalWrites = new LongAdder();
    private final LongAdder totalOther = new LongAdder();

    private final AtomicLong lastPollReads = new AtomicLong(0);
    private final AtomicLong lastPollWrites = new AtomicLong(0);
    private final AtomicLong lastPollNanos = new AtomicLong(System.nanoTime());

    public void record(SourceDialect dialect, String sqlText, long elapsedNanos) {
        record(dialect, null, sqlText, elapsedNanos);
    }

    /**
     * @param backendName the resolved routing target (see {@code Statement#targetBackend()},
     *      set by {@code RouterStage} earlier in the pipeline) -- {@code null} or blank folds
     *      into {@code "default"} so single-backend deployments (no {@code POLYWIRE_BACKENDS}
     *      configured) still get a per-backend row instead of disappearing from the breakdown.
     */
    public void record(SourceDialect dialect, String backendName, String sqlText, long elapsedNanos) {
        byProtocol.computeIfAbsent(protocolName(dialect), k -> new LongAdder()).increment();

        StatementKind kind = classify(sqlText);
        switch (kind) {
            case READ -> totalReads.increment();
            case WRITE -> totalWrites.increment();
            case OTHER -> totalOther.increment();
        }

        String backend = (backendName == null || backendName.isBlank()) ? "default" : backendName;
        BackendEntry backendEntry = byBackend.computeIfAbsent(backend, k -> new BackendEntry());
        backendEntry.calls.increment();
        backendEntry.totalNanos.add(elapsedNanos);
        if (kind == StatementKind.READ) {
            backendEntry.reads.increment();
        } else if (kind == StatementKind.WRITE) {
            backendEntry.writes.increment();
        }

        String normalized = normalize(sqlText);
        if (normalized != null) {
            SqlEntry entry = sqlStats.get(normalized);
            if (entry == null) {
                if (sqlStats.size() >= TOP_SQL_CAP) {
                    return;
                }
                entry = sqlStats.computeIfAbsent(normalized, SqlEntry::new);
            }
            entry.calls.increment();
            entry.totalNanos.add(elapsedNanos);
        }
    }

    public static String protocolName(SourceDialect dialect) {
        if (dialect == null) {
            return "unknown";
        }
        return switch (dialect) {
            case POSTGRES -> "pgwire";
            case MYSQL -> "mywire";
            case SQL_SERVER -> "mssqlwire";
            case ORACLE -> "orawire";
            default -> dialect.name().toLowerCase(Locale.ROOT);
        };
    }

    static StatementKind classify(String sqlText) {
        if (sqlText == null || sqlText.isBlank()) {
            return StatementKind.OTHER;
        }
        String trimmed = sqlText.stripLeading();
        int end = 0;
        while (end < trimmed.length() && Character.isLetter(trimmed.charAt(end))) {
            end++;
        }
        String keyword = trimmed.substring(0, end).toUpperCase(Locale.ROOT);
        return switch (keyword) {
            case "SELECT", "SHOW", "EXPLAIN", "WITH" -> StatementKind.READ;
            case "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE" -> StatementKind.WRITE;
            default -> StatementKind.OTHER;
        };
    }

    static String normalize(String sqlText) {
        if (sqlText == null || sqlText.isBlank()) {
            return null;
        }
        String normalized = STRING_LITERAL.matcher(sqlText).replaceAll("?");
        normalized = NUMBER_LITERAL.matcher(normalized).replaceAll("?");
        normalized = WHITESPACE.matcher(normalized).replaceAll(" ").trim();
        if (normalized.length() > 300) {
            normalized = normalized.substring(0, 300) + "…";
        }
        return normalized;
    }

    public Snapshot snapshot() {
        long now = System.nanoTime();
        long reads = totalReads.sum();
        long writes = totalWrites.sum();

        long prevReads = lastPollReads.getAndSet(reads);
        long prevWrites = lastPollWrites.getAndSet(writes);
        long prevNanos = lastPollNanos.getAndSet(now);
        double elapsedSec = Math.max((now - prevNanos) / 1_000_000_000.0, 0.001);

        double readsPerSec = Math.max((reads - prevReads) / elapsedSec, 0);
        double writesPerSec = Math.max((writes - prevWrites) / elapsedSec, 0);

        Map<String, Long> protocolCounts = new java.util.LinkedHashMap<>();
        byProtocol.forEach((k, v) -> protocolCounts.put(k, v.sum()));

        List<SqlStat> top = new ArrayList<>();
        for (SqlEntry entry : sqlStats.values()) {
            long calls = entry.calls.sum();
            long totalMs = entry.totalNanos.sum() / 1_000_000;
            long avgMs = calls == 0 ? 0 : totalMs / calls;
            top.add(new SqlStat(entry.normalizedSql, calls, totalMs, avgMs));
        }
        top.sort(Comparator.comparingLong(SqlStat::totalMillis).reversed());
        List<SqlStat> top10 = top.size() > 10 ? top.subList(0, 10) : top;

        List<BackendStat> backendStats = new ArrayList<>();
        for (var e : byBackend.entrySet()) {
            BackendEntry be = e.getValue();
            long calls = be.calls.sum();
            long totalMs = be.totalNanos.sum() / 1_000_000;
            long avgMs = calls == 0 ? 0 : totalMs / calls;
            backendStats.add(new BackendStat(e.getKey(), calls, be.reads.sum(), be.writes.sum(), totalMs, avgMs));
        }
        backendStats.sort(Comparator.comparingLong(BackendStat::calls).reversed());

        return new Snapshot(protocolCounts, reads, writes, totalOther.sum(), readsPerSec, writesPerSec,
                List.copyOf(top10), List.copyOf(backendStats));
    }
}
