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
 * traffic, how many reads/writes per second, and which SQL (or, for mongowire/dynamowire, which
 * operation) is costing the most. One shared instance across every protocol PolyWire speaks --
 * {@link StatsCollectorStage} feeds it via {@link #record(SourceDialect, String, String, long)}
 * for the SQL wire protocols that pass through the shared pipeline (pgwire, mywire, mssqlwire,
 * orawire), and mongowire/dynamowire feed it directly via {@link #recordOperation} at their own
 * single dispatch choke point (they never build a {@code Statement} or go through {@code
 * PipelineStage}s -- see {@code MongoCommandDispatcher#dispatch} / {@code
 * OperationHandlers#dispatch}), since those protocols don't have literal SQL text to classify or
 * normalize but have an equally natural single label (the command/operation name). Both paths
 * update the same protocol/backend/read-write/top-N-cost breakdown; the caller can't tell from
 * the snapshot which path a given data point came from, which is the point -- one pipeline, one
 * dashboard, regardless of wire protocol. Deliberately not a Prometheus counter: this holds
 * enough shape (per-item labels, per-protocol breakdown) that a plain counter can't capture, and
 * is read back as one JSON snapshot by the admin API.
 *
 * <p><b>RTT vs. execution time:</b> {@link #record}/{@link #recordOperation} measure only
 * pipeline/backend execution time (firewall + router + QoS + translation + the actual backend
 * round trip) -- for the SQL protocols that's everything {@code StatementPipeline.execute}
 * spans, which stops before the session handler serializes and writes the response back to the
 * client socket. {@link #recordRtt}/{@link #recordOperationRtt} are a second, optional signal
 * fed separately by each session handler wrapping its own full request-read-to-response-written
 * span -- true "server-side round trip" in the sense a reverse proxy's `$request_time` is (total
 * time PolyWire itself took to service the request), not network RTT to the client, which no
 * server-side vantage point can measure. Not every call site reports it: pgwire's extended query
 * protocol splits Bind (which executes the query -- no data sent yet) from Execute (a separate,
 * client-paced message that streams the already-computed result), so a span joining the two would
 * include client think-time, not just PolyWire's own service time -- Bind never gets an RTT
 * sample. Execute's own span is honest on its own, though (no backend re-execution, no client
 * gap inside it), so it does get one -- same reasoning as orawire's Fetch below. Every other call
 * site (pgwire simple query, pgwire Execute, mywire, mssqlwire, orawire, gRPC, mongowire,
 * dynamowire, sqswire) reports both exec time and RTT.
 */
public final class SqlMetricsCollector {

    private static final int TOP_SQL_CAP = 500;
    private static final Pattern STRING_LITERAL = Pattern.compile("'(?:[^']|'')*'");
    private static final Pattern NUMBER_LITERAL = Pattern.compile("\\b\\d+(?:\\.\\d+)?\\b");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    public enum StatementKind { READ, WRITE, OTHER }

    public record SqlStat(String normalizedSql, long calls, long totalMillis, long avgMillis,
            Long avgRttMillis) {
    }

    public record BackendStat(String backend, long calls, long reads, long writes, long totalMillis, long avgMillis) {
    }

    /**
     * One row of the "how long did each kind of outcome actually take" breakdown -- {@code
     * outcome} is one of {@link #OUTCOME_CACHE_HIT}, {@link #OUTCOME_PG_READ}, or {@link
     * #OUTCOME_PG_WRITE}. Separate from {@link SqlStat}/{@link BackendStat}: those answer "which
     * statement/backend is expensive", this answers "is the cache actually saving us anything,
     * per protocol" -- the comparison only means something once cache hits and real Postgres
     * reads are timed the same way, side by side.
     */
    public record RttOutcomeStat(String protocol, String outcome, long calls, long totalMillis, long avgMillis) {
    }

    public record Snapshot(
            Map<String, Long> protocolCounts,
            long totalReads,
            long totalWrites,
            long totalOther,
            double readsPerSec,
            double writesPerSec,
            List<SqlStat> topSql,
            List<BackendStat> byBackend,
            Long avgRttMs,
            long rttSamples) {
    }

    private static final class SqlEntry {
        final String normalizedSql;
        final LongAdder calls = new LongAdder();
        final LongAdder totalNanos = new LongAdder();
        final LongAdder rttCalls = new LongAdder();
        final LongAdder rttTotalNanos = new LongAdder();

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

    private static final class RttOutcomeEntry {
        final LongAdder calls = new LongAdder();
        final LongAdder totalNanos = new LongAdder();
    }

    public static final String OUTCOME_CACHE_HIT = "cache_hit";
    public static final String OUTCOME_PG_READ = "pg_read";
    public static final String OUTCOME_PG_WRITE = "pg_write";
    /** sqswire has no cache layer at all (see {@code PgQueueStore}) -- a queue's whole point is
     * that every enqueue/dequeue is a real state change, nothing is safely repeatable from a
     * cache. So instead of cache_hit/pg_read/pg_write, sqswire reports these two outcomes. */
    public static final String OUTCOME_ENQUEUE = "enqueue";
    public static final String OUTCOME_DEQUEUE = "dequeue";

    private final ConcurrentHashMap<String, LongAdder> byProtocol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SqlEntry> sqlStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BackendEntry> byBackend = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RttOutcomeEntry> byRttOutcome = new ConcurrentHashMap<>();
    private final LongAdder totalReads = new LongAdder();
    private final LongAdder totalWrites = new LongAdder();
    private final LongAdder totalOther = new LongAdder();
    private final LongAdder rttSamples = new LongAdder();
    private final LongAdder totalRttNanos = new LongAdder();

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
        recordOperation(protocolName(dialect), backendName, classify(sqlText), normalize(sqlText), elapsedNanos);
    }

    /**
     * The mongowire/dynamowire entry point -- {@code label} is whatever that protocol's own
     * dispatch already has on hand as a natural, already-bounded-cardinality description (a
     * DynamoDB operation name like {@code "PutItem"}, a Mongo command name like {@code "find"} --
     * see the call sites in {@code DynamoWireServer}/{@code MongoWireSessionHandler}), not
     * something this class derives by parsing SQL text the way {@link #record} does. {@code kind}
     * is supplied by the caller rather than inferred, since there's no SQL keyword to classify.
     */
    public void recordOperation(String protocolName, String backendName, StatementKind kind, String label, long elapsedNanos) {
        byProtocol.computeIfAbsent(protocolName == null ? "unknown" : protocolName, k -> new LongAdder()).increment();

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

        if (label != null && !label.isBlank()) {
            SqlEntry entry = sqlStats.get(label);
            if (entry == null) {
                if (sqlStats.size() >= TOP_SQL_CAP) {
                    return;
                }
                entry = sqlStats.computeIfAbsent(label, SqlEntry::new);
            }
            entry.calls.increment();
            entry.totalNanos.add(elapsedNanos);
        }
    }

    /**
     * Convenience for call sites (sqswire, dynamowire) whose single existing measurement already
     * spans the full request-read-to-response-written window -- their {@code finally} block wraps
     * the response write, not just the business logic -- so the same {@code elapsedNanos} is
     * simultaneously valid as both exec time and RTT. Equivalent to calling {@link #recordOperation
     * (String, String, StatementKind, String, long)} followed by {@link #recordOperationRtt}.
     */
    public void recordOperation(String protocolName, String backendName, StatementKind kind, String label,
            long elapsedNanos, long rttNanos) {
        recordOperation(protocolName, backendName, kind, label, elapsedNanos);
        recordOperationRtt(label, rttNanos);
    }

    /**
     * Fed separately from {@link #record}, by a session handler that measured its own
     * read-request-to-write-response span -- see the class javadoc's RTT section. {@code
     * sqlText} is normalized the same way {@link #record} does so it lands in the same
     * per-fingerprint bucket that call already created.
     */
    public void recordRtt(SourceDialect dialect, String sqlText, long rttNanos) {
        recordOperationRtt(normalize(sqlText), rttNanos);
    }

    /** The mongowire/dynamowire/sqswire/gRPC entry point -- {@code label} matches what {@link #recordOperation} used. */
    public void recordOperationRtt(String label, long rttNanos) {
        rttSamples.increment();
        totalRttNanos.add(rttNanos);
        if (label == null || label.isBlank()) {
            return;
        }
        // Only updates an entry that record()/recordOperation() already created for this same
        // call -- never creates one on its own, so it can't bypass TOP_SQL_CAP or add an entry
        // with exec stats missing.
        SqlEntry entry = sqlStats.get(label);
        if (entry != null) {
            entry.rttCalls.increment();
            entry.rttTotalNanos.add(rttNanos);
        }
    }

    /**
     * Records how long one cache-hit, real-Postgres-read, or real-Postgres-write took, for one
     * protocol -- see {@link #OUTCOME_CACHE_HIT}/{@link #OUTCOME_PG_READ}/{@link
     * #OUTCOME_PG_WRITE}. Cardinality is bounded (a handful of protocols x 3 outcomes), so unlike
     * {@link #sqlStats} this is safe to also expose as a Prometheus series. Fed from two places:
     * {@code CacheStage.handleCacheableSelect}'s hit branch (the only place that today returns
     * *before* {@link StatsCollectorStage#handle} ever runs, so without this call a cache hit
     * left no timing trace anywhere), and {@link StatsCollectorStage#handle} itself for the
     * pg_read/pg_write case -- both a cache miss and a naturally non-cacheable statement land
     * there, and this class already has to classify the SQL text for {@link #totalReads}/{@link
     * #totalWrites}, so reusing that classification costs nothing extra.
     */
    public void recordRttOutcome(String protocolName, String outcome, long elapsedNanos) {
        if (outcome == null) {
            return;
        }
        String key = (protocolName == null ? "unknown" : protocolName) + "|" + outcome;
        RttOutcomeEntry entry = byRttOutcome.computeIfAbsent(key, k -> new RttOutcomeEntry());
        entry.calls.increment();
        entry.totalNanos.add(elapsedNanos);
    }

    public List<RttOutcomeStat> rttOutcomeSnapshot() {
        List<RttOutcomeStat> stats = new ArrayList<>();
        byRttOutcome.forEach((key, entry) -> {
            int split = key.indexOf('|');
            String protocol = key.substring(0, split);
            String outcome = key.substring(split + 1);
            long calls = entry.calls.sum();
            long totalMs = entry.totalNanos.sum() / 1_000_000;
            long avgMs = calls == 0 ? 0 : totalMs / calls;
            stats.add(new RttOutcomeStat(protocol, outcome, calls, totalMs, avgMs));
        });
        stats.sort(Comparator.comparing(RttOutcomeStat::protocol).thenComparing(RttOutcomeStat::outcome));
        return stats;
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
            // gRPC's own native driver protocol -- was indistinguishable from MCP traffic in
            // every metrics view until SourceDialect.MCP was split out as its own value (see its
            // javadoc); this case is what actually renames the dashboard label from the old
            // "polywire_native" to something a reader recognizes.
            case POLYWIRE_NATIVE -> "grpc";
            case MCP -> "mcp";
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
            long rttCalls = entry.rttCalls.sum();
            Long avgRttMs = rttCalls == 0 ? null : entry.rttTotalNanos.sum() / 1_000_000 / rttCalls;
            top.add(new SqlStat(entry.normalizedSql, calls, totalMs, avgMs, avgRttMs));
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

        long rttSampleCount = rttSamples.sum();
        Long avgRttMs = rttSampleCount == 0 ? null : totalRttNanos.sum() / 1_000_000 / rttSampleCount;

        return new Snapshot(protocolCounts, reads, writes, totalOther.sum(), readsPerSec, writesPerSec,
                List.copyOf(top10), List.copyOf(backendStats), avgRttMs, rttSampleCount);
    }
}
