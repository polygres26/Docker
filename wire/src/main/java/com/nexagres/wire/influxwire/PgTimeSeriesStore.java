package com.nexagres.wire.influxwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.BackendTarget;
import com.nexagres.wire.core.DdlTemplates;
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
            String engine = DdlTemplates.engineDirFor(target.jdbcUrl());
            // Real DDL, loaded from ddl/<engine>/influxwire_measurement_table.sql -- see
            // DdlTemplates' own javadoc for the full reasoning and the real per-engine differences
            // (Postgres JSONB+GIN -> Oracle CLOB+IS JSON / SQL Server NVARCHAR(MAX)+ISJSON() /
            // MySQL JSON, each missing a real GIN-equivalent generic tag index -- see each file's
            // own comment).
            List<String> tableDdl = engine == null ? null
                    : DdlTemplates.loadStatements(engine, "influxwire_measurement_table", Map.of("table", table));
            if (tableDdl == null) {
                throw new SQLException("influxwire: no real DDL template for this backend's own engine "
                        + "(jdbcUrl=" + target.jdbcUrl() + ") -- see BackendDriverRegistry for the "
                        + "currently-supported engine list");
            }
            for (String statement : tableDdl) {
                st.executeUpdate(statement);
            }
            if ("postgres".equals(engine) && timescaleAvailable(target)) {
                // migrate_data => true: harmless/no-op on a table CREATE TABLE just made empty,
                // but means this call is also safe (not a failed precondition) if a future version
                // of this class ever calls ensureMeasurement against a table that picked up rows
                // before TimescaleDB was detected available.
                // executeQuery, not executeUpdate -- create_hypertable() is a SELECT-returning
                // function call (it returns the new/existing hypertable's id and schema/table
                // name), not DDL; pgjdbc's executeUpdate throws "A result was returned when none
                // was expected" against it (found live, on the very first real TimescaleDB
                // backend this was tested against). Real, TimescaleDB-only optimization -- no
                // equivalent DDL exists for the other 3 engines, see ddl/postgres/
                // influxwire_hypertable.sql's own comment.
                String hypertableSql = DdlTemplates.loadStatements("postgres", "influxwire_hypertable",
                        Map.of("table", table)).get(0);
                st.executeQuery(hypertableSql).close();
                log.info("influxwire: {} created as a TimescaleDB hypertable", table);
            } else if ("postgres".equals(engine)) {
                log.info("influxwire: {} created as a plain Postgres table (no TimescaleDB on this backend)", table);
            } else {
                log.info("influxwire: {} created as a plain {} table (TimescaleDB-style hypertable "
                        + "partitioning has no equivalent on this engine)", table, engine);
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

    /** Real InfluxDB's own {@code /query} series shape is column-name-list + row-of-values, not a
     * fixed (time, tags, fields) triple -- the actual column set varies with the SELECT list and
     * any GROUP BY. {@link InfluxWireServer} renders this directly into that shape. */
    public record QueryResult(List<String> columns, List<List<Object>> rows) {
    }

    /**
     * Executes a parsed {@link InfluxQlParser.SelectStatement} as real, parameterized SQL against
     * this measurement's table. Two real, different SQL shapes depending on the statement:
     *
     * <p><b>No aggregation, no GROUP BY</b> (a bare {@code SELECT field1,field2 FROM m WHERE ...}):
     * one row per matching point, columns = {@code time} + the requested field names, each pulled
     * out of the {@code fields} jsonb column with {@code ->}.
     *
     * <p><b>Aggregated</b> (any {@code mean/sum/count/min/max(...)} in the select list, or a
     * {@code GROUP BY}): a real {@code GROUP BY} query. Time-bucketing uses Postgres's own native
     * {@code date_bin()} (built into stock Postgres since v14 -- deliberately NOT TimescaleDB's
     * {@code time_bucket()}, so aggregation works identically on both {@link #ensureMeasurement}
     * code paths, not just the hypertable one) anchored to the Unix epoch, matching real InfluxDB's
     * own UTC-aligned bucket boundaries. Numeric aggregates cast the target field to
     * {@code double precision} via {@code (fields->>'field')::double precision} -- a field that
     * was written as a string or bool for some points and a number for others will throw a real
     * Postgres cast error there, which is an honest reflection of asking a numeric aggregate to
     * work over genuinely mixed-type data, not a bug to hide.
     */
    public QueryResult select(InfluxQlParser.SelectStatement stmt) throws SQLException {
        BackendTarget target = defaultTarget();
        ensureMeasurement(target, stmt.measurement());
        String table = pgTableName(stmt.measurement());

        boolean aggregated = stmt.groupBy() != null
                || stmt.selectList().stream().anyMatch(i -> i.func() != null);

        StringBuilder sql = new StringBuilder("SELECT ");
        List<String> columns = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        if (aggregated) {
            List<String> selectExprs = new ArrayList<>();
            String bucketExpr = null;
            if (stmt.groupBy() != null) {
                long bucketMillis = InfluxQlParser.parseDurationMillis(stmt.groupBy().durationLiteral());
                bucketExpr = "date_bin(('" + bucketMillis + " milliseconds')::interval, time, "
                        + "'epoch'::timestamptz)";
                selectExprs.add(bucketExpr + " AS time");
                columns.add("time");
            }
            for (String tagCol : stmt.groupBy() == null ? List.<String>of() : stmt.groupBy().tagColumns()) {
                validateIdentifier(tagCol);
                selectExprs.add("tags->>'" + tagCol + "' AS " + tagCol);
                columns.add(tagCol);
            }
            for (InfluxQlParser.SelectItem item : stmt.selectList()) {
                if (item.isWildcard()) {
                    throw new InfluxException("influxwire: SELECT * can't be combined with aggregation/GROUP BY");
                }
                validateIdentifier(item.field());
                String colAlias = (item.func() == null ? item.field() : item.func().name().toLowerCase(Locale.ROOT))
                        + "_" + item.field();
                String fieldExpr = "(fields->>'" + item.field() + "')::double precision";
                String aggExpr = switch (item.func() == null ? InfluxQlParser.AggFunc.MEAN : item.func()) {
                    case MEAN -> "avg(" + fieldExpr + ")";
                    case SUM -> "sum(" + fieldExpr + ")";
                    case COUNT -> "count(fields->'" + item.field() + "')";
                    case MIN -> "min(" + fieldExpr + ")";
                    case MAX -> "max(" + fieldExpr + ")";
                };
                selectExprs.add(aggExpr + " AS " + colAlias);
                columns.add(colAlias);
            }
            sql.append(String.join(", ", selectExprs)).append(" FROM ").append(table);
            appendWhere(sql, params, stmt.where());
            List<String> groupCols = new ArrayList<>();
            if (bucketExpr != null) {
                groupCols.add(bucketExpr);
            }
            for (String tagCol : stmt.groupBy() == null ? List.<String>of() : stmt.groupBy().tagColumns()) {
                groupCols.add("tags->>'" + tagCol + "'");
            }
            if (!groupCols.isEmpty()) {
                sql.append(" GROUP BY ").append(String.join(", ", groupCols));
                sql.append(" ORDER BY ").append(groupCols.get(0));
            }
        } else {
            List<String> selectExprs = new ArrayList<>();
            selectExprs.add("time");
            columns.add("time");
            if (stmt.selectList().size() == 1 && stmt.selectList().get(0).isWildcard()) {
                selectExprs.add("tags");
                selectExprs.add("fields");
                columns.add("tags");
                columns.add("fields");
            } else {
                for (InfluxQlParser.SelectItem item : stmt.selectList()) {
                    validateIdentifier(item.field());
                    selectExprs.add("fields->'" + item.field() + "' AS " + item.field());
                    columns.add(item.field());
                }
            }
            sql.append(String.join(", ", selectExprs)).append(" FROM ").append(table);
            appendWhere(sql, params, stmt.where());
            sql.append(" ORDER BY time DESC");
        }
        if (stmt.limit() != null) {
            sql.append(" LIMIT ").append(stmt.limit().intValue());
        }

        List<List<Object>> rows = new ArrayList<>();
        try (Connection c = target.open(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof java.time.Instant instant) {
                    ps.setTimestamp(i + 1, Timestamp.from(instant));
                } else {
                    ps.setString(i + 1, String.valueOf(p));
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                java.sql.ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        row.add(resultValue(rs, i, md.getColumnTypeName(i)));
                    }
                    rows.add(row);
                }
            }
        }
        return new QueryResult(columns, rows);
    }

    private static Object resultValue(ResultSet rs, int col, String pgType) throws SQLException {
        Object v = rs.getObject(col);
        if (v == null) {
            return null;
        }
        if (v instanceof Timestamp ts) {
            return ts.toInstant().toString();
        }
        if (pgType.equals("jsonb") || pgType.equals("json")) {
            return JsonParser.parseString(v.toString());
        }
        return v;
    }

    private static void appendWhere(StringBuilder sql, List<Object> params, List<InfluxQlParser.Condition> where) {
        if (where.isEmpty()) {
            return;
        }
        sql.append(" WHERE ");
        List<String> clauses = new ArrayList<>();
        for (InfluxQlParser.Condition cond : where) {
            String op = switch (cond.op()) {
                case EQ -> "=";
                case NEQ -> "!=";
                case GT -> ">";
                case LT -> "<";
                case GTE -> ">=";
                case LTE -> "<=";
            };
            if (cond.isTime()) {
                clauses.add("time " + op + " ?");
                params.add(java.time.Instant.parse(cond.value()));
            } else {
                validateIdentifier(cond.column());
                // A WHERE column could be a tag or a field -- V1 doesn't track per-measurement
                // schema metadata to disambiguate, so it checks tags first (the overwhelmingly
                // common real InfluxQL WHERE-clause case) and falls back to the field's own text
                // representation otherwise, via COALESCE.
                clauses.add("COALESCE(tags->>'" + cond.column() + "', fields->>'" + cond.column() + "') "
                        + op + " ?");
                params.add(cond.value());
            }
        }
        sql.append(String.join(" AND ", clauses));
    }

    private static void validateIdentifier(String s) {
        if (!IDENTIFIER.matcher(s).matches()) {
            throw new InfluxException("influxwire: identifier must match [A-Za-z_][A-Za-z0-9_]* -- got \"" + s + "\"");
        }
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
