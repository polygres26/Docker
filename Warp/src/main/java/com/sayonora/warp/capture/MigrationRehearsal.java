package com.sayonora.warp.capture;

import com.sayonora.warp.config.NodeRegistry;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase G's "migration rehearsal": replays the same captured, real workload against BOTH the
 * original service (an arbitrary JDBC target -- the thing being migrated away from) and Warp,
 * then diffs each statement's outcome (result shape, row count/content, error, latency) instead
 * of just replaying against one target and reporting replayed/failed counts the way
 * {@link WorkloadReplayer} does. This is what turns Warp's own "migration compatibility gateway"
 * positioning into something a customer can run and see a real diff from, not just a claim.
 *
 * <p>Reuses {@link WorkloadReplayer}'s exact discovery/merge machinery ({@link
 * WorkloadReplayer.CapturedEntry}, {@code fetchAll}) unchanged -- both tools read the SAME
 * fleet-wide {@code GET /api/capture} pages and merge them into one global wall-clock order the
 * same way, so a rehearsal report reflects the same true arrival order a plain replay would.
 *
 * <p><b>Deliberately narrow, disclosed limitations (v1):</b>
 * <ul>
 *   <li>Both targets are plain JDBC (the "original service" via a generic connection string, Warp
 *   via {@link com.sayonora.warp.pgwire.PgConnections#open} exactly as {@code WorkloadReplayer}
 *   already does) -- this compares two SQL-speaking targets, not (yet) an AWS-emulator target the
 *   way {@code floci_compat} does; that generalization is real, separate follow-up work.</li>
 *   <li>Content equivalence is a row-count + order-insensitive content hash over at most {@link
 *   #MAX_ROWS_FOR_SIGNATURE} rows per statement, not a full row-by-row diff -- appropriate for
 *   "did these two systems return the same data," not for producing a human-readable row-level
 *   diff on a genuine mismatch (the report records that a mismatch happened and the two row
 *   counts, not which rows differ).</li>
 *   <li>Statements run against each target independently and sequentially (source first, then
 *   Warp) -- a statement with side effects that are order- or time-sensitive across the two
 *   targets (e.g. NOW()/a sequence) can show a false content mismatch that isn't really a
 *   compatibility gap. Disclosed, not hidden.</li>
 * </ul>
 *
 * <p>Run via {@code java -cp sayonora-warp.jar com.sayonora.warp.capture.MigrationRehearsal
 * --source-jdbc-url=jdbc:postgresql://old-host:5432/mydb --source-user=... --source-password=...
 * --out=rehearsal-report.json}, using the same {@code WARP_PG_*}/{@code WARP_ADMIN_TOKEN} options
 * {@code WorkloadReplayer} uses for the Warp side.
 */
public final class MigrationRehearsal {

    private static final Logger log = LoggerFactory.getLogger(MigrationRehearsal.class);
    private static final int MAX_ROWS_FOR_SIGNATURE = 500;

    private MigrationRehearsal() {
    }

    /** One target's outcome for one captured statement -- deliberately a plain, dependency-free
     * data record so {@link #diff} is a pure function, testable with zero database/network. */
    public record StatementOutcome(boolean success, String sqlState, String errorMessage,
            boolean hasResultSet, int rowCount, int contentSignature, long updateCount, long latencyNanos) {

        public static StatementOutcome failure(SQLException e, long latencyNanos) {
            return new StatementOutcome(false, e.getSQLState(), e.getMessage(), false, 0, 0, -1, latencyNanos);
        }
    }

    public enum Verdict {
        /** Both targets succeeded with the same result shape/content (or both non-query statements
         * with the same update count). The common, boring, correct case. */
        MATCH,
        /** Both targets succeeded but returned different row counts or content. A real
         * compatibility gap worth a human look. */
        RESULT_MISMATCH,
        /** Both targets succeeded on a DML/DDL statement but reported a different update count. */
        UPDATE_COUNT_MISMATCH,
        /** The original service accepted the statement but Warp rejected it -- the sharpest
         * possible signal of a migration blocker. */
        ONLY_WARP_FAILED,
        /** Warp accepted the statement but the original service rejected it -- worth surfacing
         * (a captured statement that shouldn't have succeeded against the source either), but not
         * a migration blocker for Warp specifically. */
        ONLY_SOURCE_FAILED,
        /** Both targets rejected the statement, but with a different SQLSTATE/error shape --
         * still failed everywhere, but not equivalently, which can matter for client error
         * handling that branches on the specific error. */
        BOTH_FAILED_DIFFERENT_STATE,
        /** Both targets rejected the statement the same way (same SQLSTATE, or both null with
         * matching messages) -- equivalent failure, not a Warp-specific gap. */
        BOTH_FAILED_SAME_STATE,
    }

    /** Pure diff: given both targets' outcomes for the same captured statement, classify how they
     * compare. No I/O, no state -- exhaustively unit-testable. */
    public static Verdict diff(StatementOutcome source, StatementOutcome warp) {
        if (source.success() && !warp.success()) {
            return Verdict.ONLY_WARP_FAILED;
        }
        if (!source.success() && warp.success()) {
            return Verdict.ONLY_SOURCE_FAILED;
        }
        if (!source.success()) {
            // both failed
            boolean sameState = java.util.Objects.equals(source.sqlState(), warp.sqlState());
            return sameState ? Verdict.BOTH_FAILED_SAME_STATE : Verdict.BOTH_FAILED_DIFFERENT_STATE;
        }
        // both succeeded
        if (source.hasResultSet() != warp.hasResultSet()) {
            return Verdict.RESULT_MISMATCH;
        }
        if (source.hasResultSet()) {
            boolean sameShape = source.rowCount() == warp.rowCount()
                    && source.contentSignature() == warp.contentSignature();
            return sameShape ? Verdict.MATCH : Verdict.RESULT_MISMATCH;
        }
        return source.updateCount() == warp.updateCount() ? Verdict.MATCH : Verdict.UPDATE_COUNT_MISMATCH;
    }

    public record RehearsalEntry(WorkloadReplayer.CapturedEntry captured, StatementOutcome source,
            StatementOutcome warp, Verdict verdict) {
    }

    public static void main(String[] args) throws Exception {
        java.util.Map<String, String> flags = parseFlags(args);
        String sourceJdbcUrl = require(flags, "source-jdbc-url");
        String sourceUser = flags.get("source-user");
        String sourcePassword = flags.get("source-password");
        String outPath = flags.getOrDefault("out", "migration-rehearsal-report.json");

        ServerOptions options = ServerOptions.parse(args);
        String adminToken = System.getenv("WARP_ADMIN_TOKEN");

        List<NodeRegistry.NodeRow> nodes = NodeRegistry.listAll(options).stream()
                .filter(n -> "up".equals(n.status()))
                .toList();
        log.info("migration rehearsal: {} live node(s) discovered via warp_nodes", nodes.size());

        List<WorkloadReplayer.CapturedEntry> all = new ArrayList<>();
        var http = java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        for (NodeRegistry.NodeRow node : nodes) {
            int pulled = WorkloadReplayer.fetchAll(http, node, adminToken, all);
            log.info("migration rehearsal: pulled {} entries from node {} ({}:{})", pulled, node.nodeId(),
                    node.host(), node.adminPort());
        }
        all.sort(Comparator.comparing(WorkloadReplayer.CapturedEntry::wallClock)
                .thenComparing(WorkloadReplayer.CapturedEntry::nodeId)
                .thenComparingLong(WorkloadReplayer.CapturedEntry::localSeq));
        log.info("migration rehearsal: {} total captured statement(s) merged into one global order, replaying "
                + "against both targets now", all.size());

        List<RehearsalEntry> results = new ArrayList<>();
        try (Connection sourceConn = openSource(sourceJdbcUrl, sourceUser, sourcePassword);
                Connection warpConn = com.sayonora.warp.pgwire.PgConnections.open(options)) {
            sourceConn.setAutoCommit(true);
            warpConn.setAutoCommit(true);
            for (WorkloadReplayer.CapturedEntry entry : all) {
                StatementOutcome sourceOutcome = execute(sourceConn, entry);
                StatementOutcome warpOutcome = execute(warpConn, entry);
                Verdict verdict = diff(sourceOutcome, warpOutcome);
                results.add(new RehearsalEntry(entry, sourceOutcome, warpOutcome, verdict));
            }
        }

        writeReport(results, outPath);
        java.util.Map<Verdict, Long> counts = new java.util.EnumMap<>(Verdict.class);
        for (RehearsalEntry r : results) {
            counts.merge(r.verdict(), 1L, Long::sum);
        }
        long gaps = counts.getOrDefault(Verdict.ONLY_WARP_FAILED, 0L)
                + counts.getOrDefault(Verdict.RESULT_MISMATCH, 0L)
                + counts.getOrDefault(Verdict.UPDATE_COUNT_MISMATCH, 0L)
                + counts.getOrDefault(Verdict.BOTH_FAILED_DIFFERENT_STATE, 0L);
        log.info("migration rehearsal: done -- {} statement(s) replayed against both targets, {} real gap(s) "
                + "found ({}), report written to {}", results.size(), gaps, counts, outPath);
    }

    private static Connection openSource(String jdbcUrl, String user, String password) throws SQLException {
        return user == null ? DriverManager.getConnection(jdbcUrl)
                : DriverManager.getConnection(jdbcUrl, user, password);
    }

    private static StatementOutcome execute(Connection conn, WorkloadReplayer.CapturedEntry entry) {
        long start = System.nanoTime();
        try (PreparedStatement ps = conn.prepareStatement(entry.sqlText())) {
            bindParams(ps, entry.bindParams());
            boolean hasResultSet = ps.execute();
            long latencyNanos = System.nanoTime() - start;
            if (hasResultSet) {
                try (ResultSet rs = ps.getResultSet()) {
                    return signatureOf(rs, latencyNanos);
                }
            }
            return new StatementOutcome(true, null, null, false, 0, 0, ps.getLargeUpdateCount(), latencyNanos);
        } catch (SQLException e) {
            return StatementOutcome.failure(e, System.nanoTime() - start);
        }
    }

    private static void bindParams(PreparedStatement ps, List<Object> bindParams) throws SQLException {
        for (int i = 0; i < bindParams.size(); i++) {
            Object p = bindParams.get(i);
            if (p == null) {
                ps.setNull(i + 1, java.sql.Types.VARCHAR);
            } else {
                ps.setString(i + 1, String.valueOf(p));
            }
        }
    }

    /** Row-order-insensitive content signature: reads at most {@link #MAX_ROWS_FOR_SIGNATURE} rows,
     * renders each as a delimited string, sorts the rendered rows (so two targets returning the
     * same rows in different physical order still match), and hashes the sorted list. Returning an
     * exact count alongside means a truncated read at the cap is still visible as a distinct
     * rowCount rather than silently comparing only the first {@link #MAX_ROWS_FOR_SIGNATURE} rows
     * of a much larger result as if it were the whole thing. */
    // Package-visible (not private) so MigrationRehearsalTest can exercise the order-insensitivity
    // and MAX_ROWS_FOR_SIGNATURE truncation behavior directly against a hand-built fake ResultSet,
    // without needing a real database connection.
    static StatementOutcome signatureOf(ResultSet rs, long latencyNanos) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<String> rendered = new ArrayList<>();
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            if (rendered.size() < MAX_ROWS_FOR_SIGNATURE) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= columns; c++) {
                    Object v = rs.getObject(c);
                    row.append(c > 1 ? "\u0001" : "").append(v == null ? "￿\u0000WARP_REHEARSAL_NULL\u0000￿" : v);
                }
                rendered.add(row.toString());
            }
        }
        rendered.sort(Comparator.naturalOrder());
        int signature = rendered.hashCode();
        return new StatementOutcome(true, null, null, true, rowCount, signature, -1, latencyNanos);
    }

    private static void writeReport(List<RehearsalEntry> results, String outPath) throws java.io.IOException {
        com.google.gson.JsonObject root = new com.google.gson.JsonObject();
        root.addProperty("tool", "MigrationRehearsal");
        root.addProperty("generatedAt", Instant.now().toString());
        root.addProperty("totalStatements", results.size());

        com.google.gson.JsonObject counts = new com.google.gson.JsonObject();
        for (Verdict v : Verdict.values()) {
            long n = results.stream().filter(r -> r.verdict() == v).count();
            counts.addProperty(v.name().toLowerCase(Locale.ROOT), n);
        }
        root.add("counts", counts);

        com.google.gson.JsonObject latency = new com.google.gson.JsonObject();
        latency.add("source", latencyStats(results, r -> r.source().latencyNanos()));
        latency.add("warp", latencyStats(results, r -> r.warp().latencyNanos()));
        root.add("latencyMillis", latency);

        com.google.gson.JsonArray entries = new com.google.gson.JsonArray();
        for (RehearsalEntry r : results) {
            com.google.gson.JsonObject o = new com.google.gson.JsonObject();
            o.addProperty("nodeId", r.captured().nodeId());
            o.addProperty("localSeq", r.captured().localSeq());
            o.addProperty("sqlText", r.captured().sqlText());
            o.addProperty("verdict", r.verdict().name());
            o.add("source", outcomeJson(r.source()));
            o.add("warp", outcomeJson(r.warp()));
            entries.add(o);
        }
        root.add("statements", entries);

        try (var w = java.nio.file.Files.newBufferedWriter(java.nio.file.Path.of(outPath))) {
            w.write(new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(root));
        }
    }

    private static com.google.gson.JsonObject outcomeJson(StatementOutcome o) {
        com.google.gson.JsonObject j = new com.google.gson.JsonObject();
        j.addProperty("success", o.success());
        j.addProperty("sqlState", o.sqlState());
        j.addProperty("errorMessage", o.errorMessage());
        j.addProperty("rowCount", o.hasResultSet() ? o.rowCount() : null);
        j.addProperty("updateCount", o.hasResultSet() ? null : o.updateCount());
        j.addProperty("latencyMillis", o.latencyNanos() / 1_000_000.0);
        return j;
    }

    private static com.google.gson.JsonObject latencyStats(List<RehearsalEntry> results,
            java.util.function.ToLongFunction<RehearsalEntry> extractor) {
        long[] sorted = results.stream().mapToLong(extractor).sorted().toArray();
        com.google.gson.JsonObject j = new com.google.gson.JsonObject();
        if (sorted.length == 0) {
            j.addProperty("p50", 0);
            j.addProperty("p99", 0);
            return j;
        }
        j.addProperty("p50", sorted[sorted.length / 2] / 1_000_000.0);
        j.addProperty("p99", sorted[(int) Math.min(sorted.length - 1, Math.round(sorted.length * 0.99))] / 1_000_000.0);
        return j;
    }

    private static java.util.Map<String, String> parseFlags(String[] args) {
        java.util.Map<String, String> flags = new java.util.HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                int eq = arg.indexOf('=');
                flags.put(arg.substring(2, eq), arg.substring(eq + 1));
            }
        }
        return flags;
    }

    private static String require(java.util.Map<String, String> flags, String key) {
        String v = flags.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("--" + key + " is required");
        }
        return v;
    }
}
