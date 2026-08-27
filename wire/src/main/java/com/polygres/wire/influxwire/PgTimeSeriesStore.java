package com.polygres.wire.influxwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Point storage for influxwire, backed by plain Postgres -- same "one physical table per
 * collection, real SQL underneath, no external service required" shape as
 * {@code oswire.PostgresSearchStore}/dynamowire's {@code PgItemStore}.
 *
 * <p>One physical table per measurement, {@code polywire_influx_<measurement>}:
 * <pre>
 *   time    TIMESTAMPTZ NOT NULL,  -- indexed; the hypertable partitioning column when TimescaleDB is present
 *   tags    JSONB NOT NULL DEFAULT '{}',  -- GIN-indexed
 *   fields  JSONB NOT NULL DEFAULT '{}'
 * </pre>
 * Tags/fields are stored as {@code jsonb} rather than one real column per tag/field key
 * deliberately, for V1: a real InfluxDB measurement's tag/field set isn't declared up front (any
 * point can introduce a new one), and a wide-column design would need a live {@code ALTER TABLE
 * ADD COLUMN} race on every previously-unseen key under concurrent writes. {@code jsonb} sidesteps
 * that entirely at the cost of losing per-column type constraints and needing a GIN index instead
 * of plain btree per tag -- a real, honest V1 trade-off (matching this codebase's other stores'
 * "table per collection, jsonb body" convention -- see {@code PostgresSearchStore}'s {@code source}
 * column), not a temporary shortcut, though a wide-column V2 remains a real option if per-tag
 * query performance at high cardinality turns out to need it.
 *
 * <p><b>TimescaleDB, detected not assumed:</b> every other store in this codebase deliberately
 * avoids requiring a Postgres extension (see {@code PostgresSearchStore}'s javadoc on why k-NN is
 * a linear scan rather than requiring {@code vector}) -- but a real time-series write/retention
 * workload is exactly what TimescaleDB hypertables exist for, and plain Postgres genuinely won't
 * hold up at real InfluxDB-class write volume (unbounded table growth, no chunk exclusion, slow
 * row-by-row retention deletes). Rather than pick one of "always require the extension" (breaks
 * every other protocol's zero-extension promise) or "never use it" (dishonest about real
 * performance at scale) for everyone, {@link #timescaleAvailable} checks
 * {@code pg_extension} once per backend and this store takes a genuinely different code path per
 * result: {@link #ensureMeasurement} calls {@code create_hypertable(...)} only when the extension
 * is actually installed on that specific backend, and falls back to a plain indexed table
 * otherwise -- both paths use the identical schema and SQL for every other operation, so nothing
 * downstream of {@link #ensureMeasurement} needs to know or care which one is live.
 */
public final class PgTimeSeriesStore {

    private static final Logger log = LoggerFactory.getLogger(PgTimeSeriesStore.class);
    private static final String TABLE_PREFIX = "polywire_influx_";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> ensuredMeasurements = new ConcurrentHashMap<>();
    /** Cached per physical backend (jdbcUrl), not per call -- {@code pg_extension} doesn't change
     * mid-process, and checking it on every write would cost a real round trip each time. */
    private final ConcurrentHashMap<String, Boolean> timescaleAvailableCache = new ConcurrentHashMap<>();

    public PgTimeSeriesStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    private BackendTarget defaultTarget() {
        BackendTarget target = backendRegistry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
        if (target == null) {
            throw new IllegalStateException("influxwire: no default backend configured");
        }
        return target;
    }

    static String pgTableName(String measurement) {
        if (!IDENTIFIER.matcher(measurement).matches()) {
            throw new InfluxException(
                    "influxwire: measurement name must match [A-Za-z_][A-Za-z0-9_]* -- got \"" + measurement + "\"");
        }
        return TABLE_PREFIX + measurement.toLowerCase(Locale.ROOT);
    }

    private boolean timescaleAvailable(BackendTarget target) throws SQLException {
        Boolean cached = timescaleAvailableCache.get(target.jdbcUrl());
        if (cached != null) {
            return cached;
        }
        boolean available;
        try (Connection c = target.open();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")) {
            available = rs.next();
        }
        timescaleAvailableCache.put(target.jdbcUrl(), available);
        log.info("influxwire: TimescaleDB {} on backend {} -- {}", available ? "detected" : "not detected",
                target.jdbcUrl(), available ? "new measurements become hypertables" : "new measurements are plain indexed tables");
        return available;
    }

    /** Idempotent; called on every write so a measurement never has to be pre-declared, matching
     * {@code PostgresSearchStore#ensureCollection}'s convention. Cached per (measurement, physical
     * backend) pair for the same reason that cache is: a backend that only starts receiving this
     * measurement's writes later (failover, a shard added afterward) still needs the table created
     * on it. */
    public void ensureMeasurement(BackendTarget target, String measurement) throws SQLException {
        String table = pgTableName(measurement);
        String key = table + "@" + target.jdbcUrl();
        if (ensuredMeasurements.putIfAbsent(key, Boolean.TRUE) != null) {
            return;
        }
        try (Connection c = target.open(); var st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + "time TIMESTAMPTZ NOT NULL, "
                    + "tags JSONB NOT NULL DEFAULT '{}', "
                    + "fields JSONB NOT NULL DEFAULT '{}')");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS " + table + "_time_idx ON " + table + " (time DESC)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS " + table + "_tags_idx ON " + table + " USING GIN (tags)");
            if (timescaleAvailable(target)) {
                // migrate_data => true: harmless/no-op on a table CREATE TABLE just made empty,
                // but means this call is also safe (not a failed precondition) if a future version
                // of this class ever calls ensureMeasurement against a table that picked up rows
                // before TimescaleDB was detected available.
                // executeQuery, not executeUpdate -- create_hypertable() is a SELECT-returning
                // function call (it returns the new/existing hypertable's id and schema/table
                // name), not DDL; pgjdbc's executeUpdate throws "A result was returned when none
                // was expected" against it (found live, on the very first real TimescaleDB
                // backend this was tested against).
                st.executeQuery("SELECT create_hypertable('" + table + "', 'time', "
                        + "if_not_exists => true, migrate_data => true)").close();
                log.info("influxwire: {} created as a TimescaleDB hypertable", table);
            } else {
                log.info("influxwire: {} created as a plain Postgres table (no TimescaleDB on this backend)", table);
            }
        }
    }

    /** @return true, once detected -- exposed for the admin/diagnostic surface and tests; not
     * re-checked per call once cached (see {@link #timescaleAvailableCache}'s own javadoc). */
    public boolean usesTimescale(String measurement) throws SQLException {
        BackendTarget target = defaultTarget();
        ensureMeasurement(target, measurement);
        return timescaleAvailable(target);
    }

    public void write(List<InfluxPoint> points) throws SQLException {
        if (points.isEmpty()) {
            return;
        }
        BackendTarget target = defaultTarget();
        // Group by measurement first -- each measurement is its own table/PreparedStatement/batch,
        // but a single /write body routinely carries points for more than one measurement.
        Map<String, List<InfluxPoint>> byMeasurement = new LinkedHashMap<>();
        for (InfluxPoint p : points) {
            byMeasurement.computeIfAbsent(p.measurement(), m -> new ArrayList<>()).add(p);
        }
        try (Connection c = target.open()) {
            for (Map.Entry<String, List<InfluxPoint>> entry : byMeasurement.entrySet()) {
                ensureMeasurement(target, entry.getKey());
                String table = pgTableName(entry.getKey());
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO " + table + " (time, tags, fields) VALUES (?, ?::jsonb, ?::jsonb)")) {
                    for (InfluxPoint p : entry.getValue()) {
                        ps.setTimestamp(1, Timestamp.from(Instant.ofEpochSecond(0, p.timestampNanos())));
                        ps.setString(2, toJson(p.tags()));
                        ps.setString(3, toJson(p.fields()));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
        }
    }

    /**
     * V1 read path: every point for a measurement, most recent first, optionally bounded by
     * {@code limit}. This is deliberately NOT InfluxQL -- {@code InfluxWireServer} only calls this
     * for the narrow {@code SELECT * FROM <measurement>} shape it recognizes without a full parser;
     * see that class's javadoc for the honest "everything else returns a clear 400, not a wrong
     * answer" scope line. A real InfluxQL translator (WHERE/GROUP BY time()/aggregate functions) is
     * out of this V1's scope.
     */
    public JsonArray selectAll(String measurement, int limit) throws SQLException {
        BackendTarget target = defaultTarget();
        ensureMeasurement(target, measurement);
        String table = pgTableName(measurement);
        JsonArray rows = new JsonArray();
        try (Connection c = target.open();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT time, tags, fields FROM " + table + " ORDER BY time DESC LIMIT ?")) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JsonObject row = new JsonObject();
                    row.addProperty("time", rs.getTimestamp(1).toInstant().toString());
                    row.add("tags", JsonParser.parseString(rs.getString(2)).getAsJsonObject());
                    row.add("fields", JsonParser.parseString(rs.getString(3)).getAsJsonObject());
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    /** {@code SHOW MEASUREMENTS} -- every table this store owns on the default backend. */
    public List<String> listMeasurements() throws SQLException {
        List<String> names = new ArrayList<>();
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                                + "AND table_name LIKE ?")) {
            ps.setString(1, TABLE_PREFIX + "%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1).substring(TABLE_PREFIX.length()));
                }
            }
        }
        return names;
    }

    private static String toJson(Map<String, ?> map) {
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, ?> e : map.entrySet()) {
            Object v = e.getValue();
            if (v instanceof String s) {
                obj.addProperty(e.getKey(), s);
            } else if (v instanceof Boolean b) {
                obj.addProperty(e.getKey(), b);
            } else if (v instanceof Number n) {
                obj.addProperty(e.getKey(), n);
            } else {
                obj.addProperty(e.getKey(), String.valueOf(v));
            }
        }
        return obj.toString();
    }
}
