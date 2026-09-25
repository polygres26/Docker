package com.sayonora.wire.influxwire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.DdlTemplates;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Point storage for influxwire on plain Postgres: one physical table per measurement,
 * {@code warp_influx_<measurement>}:
 * <pre>
 *   time    TIMESTAMPTZ NOT NULL,          -- microsecond part of the point time (hypertable column with TimescaleDB)
 *   tags    JSONB NOT NULL DEFAULT '{}',   -- GIN-indexed
 *   fields  JSONB NOT NULL DEFAULT '{}',
 *   db, rp  TEXT                           -- InfluxDB database / retention policy the point belongs to
 *   ns      SMALLINT                       -- remaining 0..999 nanoseconds, so timestamps are exact to the ns
 * </pre>
 * A unique index on (db, rp, time, ns, md5(tags)) gives InfluxDB's "same series + same timestamp = merge the
 * fields" write semantics via {@code ON CONFLICT DO UPDATE SET fields = old || new}.
 *
 * <p>A small catalog lives on the store's first host ({@code _warp_influx_dbs/_rps/_meas/_fields/_tagkeys}):
 * databases, retention policies, which measurements exist, and each field's type (float/integer/string/boolean,
 * which JSON numbers cannot carry) and each measurement's tag keys. Measurement names are encoded into table names
 * so any name (case, unicode, punctuation) is a distinct measurement; plain lower-case identifiers keep the
 * historical {@code warp_influx_<name>} table.
 *
 * <p>Sharding: several hosts hash a series (measurement + sorted tag set) to exactly one host; reads fetch the
 * needed raw points from every host and {@link InfluxEngine} evaluates InfluxQL over the merged points, so every
 * function (median, percentile, derivative, ...) is exact across shards. TimescaleDB is detected per backend and
 * used for the hypertable when present. Postgres-only, like the rest of the query path.
 */
public final class PgTimeSeriesStore implements InfluxBackend {

    private static final Logger log = LoggerFactory.getLogger(PgTimeSeriesStore.class);
    private static final String TABLE_PREFIX = "warp_influx_";
    private static final Pattern PLAIN = Pattern.compile("[a-z_][a-z0-9_]{0,38}");
    private static final long MICRO = 1_000L;
    private static final RetentionPolicy AUTOGEN = new RetentionPolicy("autogen", 0, 168L * 3600_000_000_000L, 1, true);

    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> ensuredMeasurements = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> timescaleAvailableCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final Set<String> knownDbs = ConcurrentHashMap.newKeySet();
    private final Set<String> knownRps = ConcurrentHashMap.newKeySet();
    private final Set<String> knownMeas = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, String> defaultRpCache = new ConcurrentHashMap<>();
    private volatile boolean catalogReady;

    public PgTimeSeriesStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    // ------------------------------------------------------------------ hosts

    private List<String> hostNames() {
        List<String> hosts = backendRegistry.storeHosts(com.sayonora.wire.core.StoreType.INFLUXDB);
        return hosts.isEmpty() ? List.of(BackendRegistry.DEFAULT_BACKEND_NAME) : hosts;
    }

    private BackendTarget targetNamed(String name) {
        BackendTarget target = backendRegistry.resolveForRouting(name);
        if (target == null) {
            throw new IllegalStateException("influxwire: backend \"" + name + "\" is not configured");
        }
        return target;
    }

    private BackendTarget defaultTarget() {
        return targetNamed(hostNames().get(0));
    }

    private List<BackendTarget> hostTargets() {
        List<BackendTarget> out = new ArrayList<>();
        for (String n : hostNames()) {
            out.add(targetNamed(n));
        }
        return out;
    }

    /** Shard key of a point: measurement + its full (sorted) tag set = one time series lives on one host. */
    static String seriesKey(InfluxPoint p) {
        StringBuilder sb = new StringBuilder(p.measurement());
        new TreeMap<>(p.tags()).forEach((k, v) -> sb.append(',').append(k).append('=').append(v));
        return sb.toString();
    }

    /** Physical table of a measurement. Plain lower-case identifiers keep {@code warp_influx_<name>}. */
    static String pgTableName(String measurement) {
        if (measurement == null || measurement.isEmpty()) {
            throw new InfluxException("invalid measurement name");
        }
        if (PLAIN.matcher(measurement).matches() && !measurement.startsWith("x0")) {
            return TABLE_PREFIX + measurement;
        }
        byte[] utf8 = measurement.getBytes(StandardCharsets.UTF_8);
        if (utf8.length <= 24) {
            return TABLE_PREFIX + "x0x" + HexFormat.of().formatHex(utf8);
        }
        try {
            byte[] d = java.security.MessageDigest.getInstance("SHA-256").digest(utf8);
            return TABLE_PREFIX + "x0h" + HexFormat.of().formatHex(d, 0, 20);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ------------------------------------------------------------------ DDL

    private boolean timescaleAvailable(BackendTarget target) throws SQLException {
        Boolean cached = timescaleAvailableCache.get(target.jdbcUrl());
        if (cached != null) {
            return cached;
        }
        boolean available;
        try (Connection c = target.open();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")) {
            available = rs.next();
        }
        timescaleAvailableCache.put(target.jdbcUrl(), available);
        log.info("influxwire: TimescaleDB {} on backend {}", available ? "detected" : "not detected", target.jdbcUrl());
        return available;
    }

    private static void requirePostgres(BackendTarget target) {
        if (!"postgres".equals(DdlTemplates.engineDirFor(target.jdbcUrl()))) {
            throw new InfluxException(500, "influxwire's InfluxQL engine requires a Postgres backend (this backend is "
                    + DdlTemplates.engineDirFor(target.jdbcUrl()) + ")");
        }
    }

    /** Idempotent; cached per (measurement, physical backend). */
    public void ensureMeasurement(BackendTarget target, String measurement) throws SQLException {
        String table = pgTableName(measurement);
        String key = table + "@" + target.jdbcUrl();
        if (ensuredMeasurements.containsKey(key)) {
            return;
        }
        requirePostgres(target);
        try (Connection c = target.open(); var st = c.createStatement()) {
            Map<String, String> vars = Map.of("table", table);
            for (String stmt : DdlTemplates.loadStatements("postgres", "influxwire_measurement_table", vars)) {
                st.executeUpdate(stmt);
            }
            // legacy tables (created before db/rp/ns existed) may hold duplicate (series, time) rows: keep the newest
            boolean hasUnique;
            try (ResultSet rs = st.executeQuery("SELECT 1 FROM pg_indexes WHERE indexname = '" + table + "_uq'")) {
                hasUnique = rs.next();
            }
            if (!hasUnique) {
                st.executeUpdate("DELETE FROM " + table + " a USING " + table + " b WHERE a.ctid < b.ctid AND a.db = b.db "
                        + "AND a.rp = b.rp AND a.time = b.time AND a.ns = b.ns AND a.tags = b.tags");
                for (String stmt : DdlTemplates.loadStatements("postgres", "influxwire_unique", vars)) {
                    st.executeUpdate(stmt);
                }
            }
            if (timescaleAvailable(target)) {
                String hypertableSql = DdlTemplates.loadStatements("postgres", "influxwire_hypertable", vars).get(0);
                st.executeQuery(hypertableSql).close();
            }
        }
        ensuredMeasurements.put(key, Boolean.TRUE);
    }

    public boolean usesTimescale(String measurement) throws SQLException {
        BackendTarget target = defaultTarget();
        ensureMeasurement(target, measurement);
        return timescaleAvailable(target);
    }

    // ------------------------------------------------------------------ catalog

    private void ensureCatalog() throws SQLException {
        if (catalogReady) {
            return;
        }
        synchronized (this) {
            if (catalogReady) {
                return;
            }
            BackendTarget t = defaultTarget();
            requirePostgres(t);
            String[] ddl = {
                "CREATE TABLE IF NOT EXISTS _warp_influx_dbs (name TEXT PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS _warp_influx_rps (db TEXT NOT NULL, name TEXT NOT NULL, duration BIGINT NOT NULL, "
                        + "shard_duration BIGINT NOT NULL, replication INT NOT NULL, is_default BOOLEAN NOT NULL, PRIMARY KEY (db, name))",
                "CREATE TABLE IF NOT EXISTS _warp_influx_meas (db TEXT NOT NULL, rp TEXT NOT NULL, meas TEXT NOT NULL, PRIMARY KEY (db, rp, meas))",
                "CREATE TABLE IF NOT EXISTS _warp_influx_fields (db TEXT NOT NULL, rp TEXT NOT NULL, meas TEXT NOT NULL, key TEXT NOT NULL, "
                        + "type TEXT NOT NULL, PRIMARY KEY (db, rp, meas, key))",
                "CREATE TABLE IF NOT EXISTS _warp_influx_tagkeys (db TEXT NOT NULL, rp TEXT NOT NULL, meas TEXT NOT NULL, key TEXT NOT NULL, "
                        + "PRIMARY KEY (db, rp, meas, key))"};
            try (Connection c = t.open(); Statement st = c.createStatement()) {
                for (String d : ddl) {
                    try {
                        st.executeUpdate(d);
                    } catch (SQLException e) {
                        if (!"23505".equals(e.getSQLState()) && !"42P07".equals(e.getSQLState())) {
                            throw e; // concurrent CREATE TABLE IF NOT EXISTS race
                        }
                    }
                }
            }
            adoptLegacy();
            catalogReady = true;
        }
    }

    /** Tables written by earlier versions (no db column) become visible to every database as db ''. */
    private void adoptLegacy() throws SQLException {
        for (BackendTarget t : hostTargets()) {
            List<String> tables = new ArrayList<>();
            try (Connection c = t.open();
                    PreparedStatement ps = c.prepareStatement(
                            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE ? "
                                    + "AND table_name NOT IN (SELECT table_name FROM information_schema.columns "
                                    + "WHERE table_schema = current_schema() AND column_name = 'ns')")) {
                ps.setString(1, TABLE_PREFIX.replace("_", "\\_") + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tables.add(rs.getString(1));
                    }
                }
            }
            for (String table : tables) {
                String meas = table.substring(TABLE_PREFIX.length());
                if (!PLAIN.matcher(meas).matches()) {
                    continue;
                }
                ensureMeasurement(t, meas);
                Map<String, String> ftypes = new LinkedHashMap<>();
                Set<String> tkeys = new java.util.TreeSet<>();
                try (Connection c = t.open(); Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery("SELECT tags::text, fields::text FROM " + table + " LIMIT 1000")) {
                    while (rs.next()) {
                        tkeys.addAll(JsonParser.parseString(rs.getString(1)).getAsJsonObject().keySet());
                        for (var e : JsonParser.parseString(rs.getString(2)).getAsJsonObject().entrySet()) {
                            ftypes.putIfAbsent(e.getKey(), inferType(e.getValue().getAsJsonPrimitive()));
                        }
                    }
                }
                try (Connection c = defaultTarget().open()) {
                    catalogAddMeasurement(c, "", "autogen", meas);
                    for (var e : ftypes.entrySet()) {
                        catalogAddField(c, "", "autogen", meas, e.getKey(), e.getValue());
                    }
                    for (String k : tkeys) {
                        catalogAddTagKey(c, "", "autogen", meas, k);
                    }
                }
                log.info("influxwire: adopted legacy measurement table {}", table);
            }
        }
    }

    private static String inferType(JsonPrimitive p) {
        if (p.isBoolean()) {
            return "boolean";
        }
        if (p.isString()) {
            return "string";
        }
        String s = p.getAsString();
        return s.indexOf('.') >= 0 || s.indexOf('e') >= 0 || s.indexOf('E') >= 0 ? "float" : "integer";
    }

    private void catalogAddMeasurement(Connection c, String db, String rp, String meas) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO _warp_influx_meas (db, rp, meas) VALUES (?,?,?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, db);
            ps.setString(2, rp);
            ps.setString(3, meas);
            ps.executeUpdate();
        }
    }

    private void catalogAddField(Connection c, String db, String rp, String meas, String key, String type) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO _warp_influx_fields (db, rp, meas, key, type) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, db);
            ps.setString(2, rp);
            ps.setString(3, meas);
            ps.setString(4, key);
            ps.setString(5, type);
            ps.executeUpdate();
        }
    }

    private void catalogAddTagKey(Connection c, String db, String rp, String meas, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO _warp_influx_tagkeys (db, rp, meas, key) VALUES (?,?,?,?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, db);
            ps.setString(2, rp);
            ps.setString(3, meas);
            ps.setString(4, key);
            ps.executeUpdate();
        }
    }

    // ------------------------------------------------------------------ databases / retention policies

    @Override
    public List<String> databases() throws SQLException {
        ensureCatalog();
        List<String> out = new ArrayList<>();
        try (Connection c = defaultTarget().open(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT name FROM _warp_influx_dbs")) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        java.util.Collections.sort(out);
        return out;
    }

    @Override
    public boolean databaseExists(String db) throws SQLException {
        ensureCatalog();
        if (knownDbs.contains(db)) {
            return true;
        }
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement("SELECT 1 FROM _warp_influx_dbs WHERE name = ?")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    knownDbs.add(db);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void createDatabase(String db, RetentionPolicy rp) throws SQLException {
        ensureCatalog();
        RetentionPolicy def = rp == null ? AUTOGEN : rp;
        try (Connection c = defaultTarget().open()) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO _warp_influx_dbs (name) VALUES (?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, db);
                if (ps.executeUpdate() == 0) {
                    knownDbs.add(db);
                    return;
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO _warp_influx_rps (db, name, duration, shard_duration, replication, is_default) VALUES (?,?,?,?,?,true) "
                            + "ON CONFLICT DO NOTHING")) {
                ps.setString(1, db);
                ps.setString(2, def.name());
                ps.setLong(3, def.durationNanos());
                ps.setLong(4, def.shardDurationNanos());
                ps.setInt(5, def.replication());
                ps.executeUpdate();
            }
        }
        knownDbs.add(db);
    }

    @Override
    public void dropDatabase(String db) throws SQLException {
        ensureCatalog();
        List<String> meas = new ArrayList<>();
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement("SELECT DISTINCT meas FROM _warp_influx_meas WHERE db = ?")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    meas.add(rs.getString(1));
                }
            }
        }
        for (String m : meas) {
            for (BackendTarget t : hostTargets()) {
                try (Connection c = t.open(); PreparedStatement ps = c.prepareStatement("DELETE FROM " + pgTableName(m) + " WHERE db = ?")) {
                    ps.setString(1, db);
                    ps.executeUpdate();
                } catch (SQLException e) {
                    if (!"42P01".equals(e.getSQLState())) {
                        throw e;
                    }
                }
            }
        }
        try (Connection c = defaultTarget().open()) {
            for (String t : new String[] {"_warp_influx_tagkeys", "_warp_influx_fields", "_warp_influx_meas", "_warp_influx_rps"}) {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + t + " WHERE db = ?")) {
                    ps.setString(1, db);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM _warp_influx_dbs WHERE name = ?")) {
                ps.setString(1, db);
                ps.executeUpdate();
            }
        }
        invalidateDb(db);
    }

    private void invalidateDb(String db) {
        knownDbs.remove(db);
        String prefix = db + "\u0000";
        knownRps.removeIf(k -> k.startsWith(prefix));
        knownMeas.removeIf(k -> k.startsWith(prefix));
        schemaCache.keySet().removeIf(k -> k.startsWith(prefix));
        schemaLoadedAt.keySet().removeIf(k -> k.startsWith(prefix));
        defaultRpCache.remove(db);
    }

    @Override
    public List<RetentionPolicy> retentionPolicies(String db) throws SQLException {
        ensureCatalog();
        List<RetentionPolicy> out = new ArrayList<>();
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT name, duration, shard_duration, replication, is_default FROM _warp_influx_rps WHERE db = ? ORDER BY name")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new RetentionPolicy(rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getInt(4), rs.getBoolean(5)));
                }
            }
        }
        return out;
    }

    @Override
    public void createRetentionPolicy(String db, RetentionPolicy rp) throws SQLException {
        ensureCatalog();
        try (Connection c = defaultTarget().open()) {
            if (rp.isDefault()) {
                try (PreparedStatement ps = c.prepareStatement("UPDATE _warp_influx_rps SET is_default = false WHERE db = ?")) {
                    ps.setString(1, db);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO _warp_influx_rps (db, name, duration, shard_duration, replication, is_default) VALUES (?,?,?,?,?,?)")) {
                ps.setString(1, db);
                ps.setString(2, rp.name());
                ps.setLong(3, rp.durationNanos());
                ps.setLong(4, rp.shardDurationNanos());
                ps.setInt(5, rp.replication());
                ps.setBoolean(6, rp.isDefault());
                ps.executeUpdate();
            }
        }
        defaultRpCache.remove(db);
    }

    @Override
    public void alterRetentionPolicy(String db, RetentionPolicy rp) throws SQLException {
        ensureCatalog();
        try (Connection c = defaultTarget().open()) {
            if (rp.isDefault()) {
                try (PreparedStatement ps = c.prepareStatement("UPDATE _warp_influx_rps SET is_default = false WHERE db = ?")) {
                    ps.setString(1, db);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE _warp_influx_rps SET duration = ?, shard_duration = ?, replication = ?, is_default = (is_default OR ?) "
                            + "WHERE db = ? AND name = ?")) {
                ps.setLong(1, rp.durationNanos());
                ps.setLong(2, rp.shardDurationNanos());
                ps.setInt(3, rp.replication());
                ps.setBoolean(4, rp.isDefault());
                ps.setString(5, db);
                ps.setString(6, rp.name());
                ps.executeUpdate();
            }
        }
        defaultRpCache.remove(db);
    }

    @Override
    public void dropRetentionPolicy(String db, String rpName) throws SQLException {
        ensureCatalog();
        List<String> meas = new ArrayList<>();
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement("SELECT meas FROM _warp_influx_meas WHERE db = ? AND rp = ?")) {
            ps.setString(1, db);
            ps.setString(2, rpName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    meas.add(rs.getString(1));
                }
            }
        }
        for (String m : meas) {
            dropMeasurement(db, rpName, m);
        }
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement("DELETE FROM _warp_influx_rps WHERE db = ? AND name = ?")) {
            ps.setString(1, db);
            ps.setString(2, rpName);
            ps.executeUpdate();
        }
        knownRps.remove(db + "\u0000" + rpName);
        defaultRpCache.remove(db);
    }

    @Override
    public String defaultRetentionPolicy(String db) throws SQLException {
        return defaultRp(db);
    }

    String defaultRp(String db) throws SQLException {
        String cached = defaultRpCache.get(db);
        if (cached != null) {
            return cached;
        }
        for (RetentionPolicy r : retentionPolicies(db)) {
            if (r.isDefault()) {
                defaultRpCache.put(db, r.name());
                return r.name();
            }
        }
        return "autogen";
    }

    // ------------------------------------------------------------------ schema

    private static String schemaKey(String db, String rp, String meas) {
        return db + "\u0000" + rp + "\u0000" + meas;
    }

    @Override
    public List<String> measurements(String db, String rp) throws SQLException {
        ensureCatalog();
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement("SELECT DISTINCT meas FROM _warp_influx_meas WHERE (db = ? OR db = '') AND rp = ?")) {
            ps.setString(1, db);
            ps.setString(2, rp);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        }
        return new ArrayList<>(names);
    }

    /** All measurement names on any database (MCP / diagnostics). */
    public List<String> listMeasurements() throws SQLException {
        ensureCatalog();
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        try (Connection c = defaultTarget().open(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT DISTINCT meas FROM _warp_influx_meas")) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        }
        return new ArrayList<>(names);
    }

    @Override
    public Schema schema(String db, String rp, String meas) throws SQLException {
        Schema src = cachedSchema(db, rp, meas);
        Schema copy = new Schema();
        synchronized (src) {
            copy.fields.putAll(src.fields);
            copy.tagKeys.addAll(src.tagKeys);
        }
        return copy;
    }

    private Schema loadSchema(String db, String rp, String meas) throws SQLException {
        ensureCatalog();
        Schema s = new Schema();
        try (Connection c = defaultTarget().open();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT 'f', key, type FROM _warp_influx_fields WHERE (db = ? OR db = '') AND rp = ? AND meas = ? "
                                + "UNION ALL SELECT 't', key, '' FROM _warp_influx_tagkeys WHERE (db = ? OR db = '') AND rp = ? AND meas = ? "
                                + "ORDER BY 1, 2")) {
            for (int k = 0; k < 2; k++) {
                ps.setString(1 + 3 * k, db);
                ps.setString(2 + 3 * k, rp);
                ps.setString(3 + 3 * k, meas);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (rs.getString(1).equals("f")) {
                        s.fields.putIfAbsent(rs.getString(2), rs.getString(3));
                    } else {
                        s.tagKeys.add(rs.getString(2));
                    }
                }
            }
        }
        return s;
    }

    /** Schemas are cached for a couple of seconds (writes through this node update the entry in place; drops evict it). */
    private static final long SCHEMA_TTL_MS = 2000;
    private final ConcurrentHashMap<String, Long> schemaLoadedAt = new ConcurrentHashMap<>();

    private Schema cachedSchema(String db, String rp, String meas) throws SQLException {
        String key = schemaKey(db, rp, meas);
        Schema s = schemaCache.get(key);
        Long at = schemaLoadedAt.get(key);
        if (s == null || at == null || System.currentTimeMillis() - at > SCHEMA_TTL_MS) {
            s = loadSchema(db, rp, meas);
            schemaCache.put(key, s);
            schemaLoadedAt.put(key, System.currentTimeMillis());
        }
        return s;
    }

    // ------------------------------------------------------------------ writes

    private static long floorMod(long a, long b) {
        return Math.floorMod(a, b);
    }

    @Override
    public WriteOutcome write(String db, String rp, List<InfluxPoint> points, boolean autoCreateDb) throws SQLException {
        if (points.isEmpty()) {
            return new WriteOutcome(0, null);
        }
        ensureCatalog();
        if (!knownDbs.contains(db) && !databaseExists(db)) {
            if (!autoCreateDb) {
                throw new InfluxException(404, "database not found: \"" + db + "\"");
            }
            createDatabase(db, null);
        }
        String rpName = rp == null || rp.isEmpty() ? defaultRp(db) : rp;
        if (!knownRps.contains(db + "\u0000" + rpName)) {
            boolean found = false;
            for (RetentionPolicy r : retentionPolicies(db)) {
                found |= r.name().equals(rpName);
            }
            if (!found) {
                throw new InfluxException(500, "retention policy not found: " + rpName);
            }
            knownRps.add(db + "\u0000" + rpName);
        }
        // (1) type analysis: inside the batch first (whole write is rejected), then against the stored schema (points dropped)
        Map<String, Map<String, String>> batchTypes = new LinkedHashMap<>();
        for (InfluxPoint p : points) {
            Map<String, String> m = batchTypes.computeIfAbsent(p.measurement(), k -> new LinkedHashMap<>());
            for (var e : p.fields().entrySet()) {
                String t = typeOf(e.getValue());
                String prev = m.putIfAbsent(e.getKey(), t);
                if (prev != null && !prev.equals(t)) {
                    throw new InfluxException("field type conflict");
                }
            }
        }
        List<InfluxPoint> survivors = new ArrayList<>(points.size());
        int dropped = 0;
        String conflict = null;
        Map<String, Schema> schemas = new LinkedHashMap<>();
        for (String meas : batchTypes.keySet()) {
            schemas.put(meas, cachedSchema(db, rpName, meas));
        }
        for (InfluxPoint p : points) {
            Schema sc = schemas.get(p.measurement());
            String bad = null;
            for (var e : p.fields().entrySet()) {
                String have = sc.fields.get(e.getKey());
                String want = typeOf(e.getValue());
                if (have != null && !have.equals(want)) {
                    bad = "field type conflict: input field \"" + e.getKey() + "\" on measurement \"" + p.measurement()
                            + "\" is type " + want + ", already exists as type " + have;
                    break;
                }
            }
            if (bad != null) {
                dropped++;
                if (conflict == null) {
                    conflict = bad;
                }
            } else {
                survivors.add(p);
            }
        }
        // (2) register new measurements / fields / tag keys in the catalog (authoritative for concurrent writers)
        for (String meas : batchTypes.keySet()) {
            Schema sc = schemas.get(meas);
            Map<String, String> newFields = new LinkedHashMap<>();
            Set<String> newTags = new java.util.TreeSet<>();
            boolean anySurvivor = false;
            for (InfluxPoint p : survivors) {
                if (!p.measurement().equals(meas)) {
                    continue;
                }
                anySurvivor = true;
                for (var e : p.fields().entrySet()) {
                    if (!sc.fields.containsKey(e.getKey())) {
                        newFields.putIfAbsent(e.getKey(), typeOf(e.getValue()));
                    }
                }
                for (String k : p.tags().keySet()) {
                    if (!sc.tagKeys.contains(k)) {
                        newTags.add(k);
                    }
                }
            }
            boolean newMeas = anySurvivor && !knownMeas.contains(db + "\u0000" + rpName + "\u0000" + meas);
            if (newFields.isEmpty() && newTags.isEmpty() && !newMeas) {
                continue;
            }
            try (Connection c = defaultTarget().open()) {
                catalogAddMeasurement(c, db, rpName, meas);
                for (var e : newFields.entrySet()) {
                    catalogAddField(c, db, rpName, meas, e.getKey(), e.getValue());
                }
                for (String k : newTags) {
                    catalogAddTagKey(c, db, rpName, meas, k);
                }
            }
            if (!newFields.isEmpty()) {
                // a concurrent writer may have registered a different type first: re-read and drop conflicting points
                Schema fresh = loadSchema(db, rpName, meas);
                for (var e : newFields.entrySet()) {
                    String have = fresh.fields.get(e.getKey());
                    if (have != null && !have.equals(e.getValue())) {
                        final String key = e.getKey();
                        int before = survivors.size();
                        survivors.removeIf(p -> p.measurement().equals(meas) && p.fields().containsKey(key));
                        dropped += before - survivors.size();
                        if (conflict == null) {
                            conflict = "field type conflict: input field \"" + key + "\" on measurement \"" + meas + "\" is type "
                                    + e.getValue() + ", already exists as type " + have;
                        }
                    }
                }
                schemaCache.put(schemaKey(db, rpName, meas), fresh);
                schemaLoadedAt.put(schemaKey(db, rpName, meas), System.currentTimeMillis());
            } else {
                sc.tagKeys.addAll(newTags);
            }
            knownMeas.add(db + "\u0000" + rpName + "\u0000" + meas);
            for (var e : newFields.entrySet()) {
                sc.fields.putIfAbsent(e.getKey(), e.getValue());
            }
            synchronized (sc) {
                sc.tagKeys.addAll(newTags);
            }
        }
        // (3) merge duplicates inside the batch (same measurement + tags + time: later fields override), then shard + upsert
        Map<String, InfluxPoint> merged = new LinkedHashMap<>();
        for (InfluxPoint p : survivors) {
            String k = seriesKey(p) + "\u0000" + p.timestampNanos();
            InfluxPoint prev = merged.get(k);
            if (prev == null) {
                merged.put(k, p);
            } else {
                Map<String, Object> f = new LinkedHashMap<>(prev.fields());
                f.putAll(p.fields());
                merged.put(k, new InfluxPoint(p.measurement(), p.tags(), f, p.timestampNanos()));
            }
        }
        List<InfluxPoint> toWrite = new ArrayList<>(merged.values());
        if (!toWrite.isEmpty()) {
            List<String> hosts = hostNames();
            if (hosts.size() == 1) {
                writeTo(targetNamed(hosts.get(0)), db, rpName, toWrite);
            } else {
                com.sayonora.wire.core.ShardingStrategy strategy = com.sayonora.wire.core.ShardingStrategy.hash(hosts);
                Map<String, List<InfluxPoint>> perHost = new LinkedHashMap<>();
                for (InfluxPoint p : toWrite) {
                    perHost.computeIfAbsent(strategy.resolve(seriesKey(p)), h -> new ArrayList<>()).add(p);
                }
                List<String> failures = new ArrayList<>();
                for (Map.Entry<String, List<InfluxPoint>> e : perHost.entrySet()) {
                    try {
                        writeTo(targetNamed(e.getKey()), db, rpName, e.getValue());
                    } catch (SQLException | RuntimeException ex) {
                        failures.add(e.getKey() + " (" + e.getValue().size() + " point(s): " + ex.getMessage() + ")");
                    }
                }
                if (!failures.isEmpty()) {
                    throw new InfluxException(500, "partial write: points destined for " + String.join("; ", failures)
                            + " were NOT written; the remaining hosts' points were. Retry the request (writes are idempotent per series+timestamp).");
                }
            }
        }
        return new WriteOutcome(dropped, conflict);
    }

    private static String typeOf(Object v) {
        if (v instanceof Long) {
            return "integer";
        }
        if (v instanceof Double) {
            return "float";
        }
        if (v instanceof Boolean) {
            return "boolean";
        }
        return "string";
    }

    private void writeTo(BackendTarget target, String db, String rp, List<InfluxPoint> points) throws SQLException {
        Map<String, List<InfluxPoint>> byMeasurement = new LinkedHashMap<>();
        for (InfluxPoint p : points) {
            byMeasurement.computeIfAbsent(p.measurement(), m -> new ArrayList<>()).add(p);
        }
        try (Connection c = target.open()) {
            for (Map.Entry<String, List<InfluxPoint>> entry : byMeasurement.entrySet()) {
                ensureMeasurement(target, entry.getKey());
                String table = pgTableName(entry.getKey());
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + table + " AS t (db, rp, time, ns, tags, fields) "
                        + "VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb) ON CONFLICT (db, rp, time, ns, (md5(tags::text))) "
                        + "DO UPDATE SET fields = t.fields || EXCLUDED.fields")) {
                    for (InfluxPoint p : entry.getValue()) {
                        long micros = Math.floorDiv(p.timestampNanos(), MICRO);
                        ps.setString(1, db);
                        ps.setString(2, rp);
                        ps.setTimestamp(3, Timestamp.from(Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                                floorMod(micros, 1_000_000L) * 1000L)));
                        ps.setShort(4, (short) floorMod(p.timestampNanos(), MICRO));
                        ps.setString(5, toJson(p.tags()));
                        ps.setString(6, toJson(p.fields()));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
        }
    }

    // ------------------------------------------------------------------ reads

    private static void appendFilter(StringBuilder sql, List<Object> params, Filter f) {
        if (f == null) {
            return;
        }
        if (f.lo != Long.MIN_VALUE) {
            sql.append(" AND time >= ?");
            params.add(micros(Math.floorDiv(f.lo, MICRO)));
        }
        if (f.hi != Long.MAX_VALUE) {
            sql.append(" AND time <= ?");
            params.add(micros(Math.floorDiv(f.hi, MICRO)));
        }
        if (f.tree != null) {
            sql.append(" AND (");
            appendNode(sql, params, f.tree);
            sql.append(")");
        }
        if (f.exact != null) {
            if (f.exact.isEmpty()) {
                sql.append(" AND FALSE");
            } else {
                sql.append(" AND (");
                for (int i = 0; i < f.exact.size(); i++) {
                    sql.append(i > 0 ? " OR " : "").append("tags = ?::jsonb");
                    params.add(toJson(f.exact.get(i)));
                }
                sql.append(")");
            }
        }
    }

    private static Timestamp micros(long micros) {
        return Timestamp.from(Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), floorMod(micros, 1_000_000L) * 1000L));
    }

    private static void appendNode(StringBuilder sql, List<Object> params, Filter.Node n) {
        if (n instanceof Filter.Leaf l) {
            if (l.op.equals("=") && !l.value.isEmpty()) {
                sql.append("tags @> ?::jsonb");
                JsonObject o = new JsonObject();
                o.addProperty(l.key, l.value);
                params.add(o.toString());
            } else if (l.op.equals("=")) {
                sql.append("COALESCE(tags->>?, '') = ''");
                params.add(l.key);
            } else {
                sql.append("COALESCE(tags->>?, '') <> ?");
                params.add(l.key);
                params.add(l.value);
            }
            return;
        }
        Filter.Group g = (Filter.Group) n;
        sql.append("(");
        for (int i = 0; i < g.kids.size(); i++) {
            if (i > 0) {
                sql.append(g.and ? " AND " : " OR ");
            }
            appendNode(sql, params, g.kids.get(i));
        }
        sql.append(")");
    }

    private static void bind(PreparedStatement ps, int from, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object p = params.get(i);
            if (p instanceof Timestamp ts) {
                ps.setTimestamp(from + i, ts);
            } else {
                ps.setString(from + i, String.valueOf(p));
            }
        }
    }

    @Override
    public List<Pt> fetch(String db, String rp, String meas, Schema schema, Filter filter) throws SQLException {
        ensureCatalog();
        List<Pt> out = new ArrayList<>();
        String table = pgTableName(meas);
        for (BackendTarget t : hostTargets()) {
            ensureMeasurement(t, meas);
            StringBuilder sql = new StringBuilder("SELECT time, ns, tags::text, fields::text FROM " + table
                    + " WHERE (db = ? OR db = '') AND rp = ?");
            List<Object> params = new ArrayList<>();
            appendFilter(sql, params, filter);
            try (Connection c = t.open(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
                ps.setString(1, db);
                ps.setString(2, rp);
                bind(ps, 3, params);
                ps.setFetchSize(5000);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Timestamp ts = rs.getTimestamp(1);
                        long nanos = Math.floorDiv(ts.getTime(), 1000L) * 1_000_000_000L + ts.getNanos() + rs.getShort(2);
                        Map<String, String> tags = new TreeMap<>();
                        for (var e : JsonParser.parseString(rs.getString(3)).getAsJsonObject().entrySet()) {
                            tags.put(e.getKey(), e.getValue().getAsString());
                        }
                        Map<String, Object> fields = new LinkedHashMap<>();
                        for (var e : JsonParser.parseString(rs.getString(4)).getAsJsonObject().entrySet()) {
                            fields.put(e.getKey(), fieldValue(e.getValue().getAsJsonPrimitive(), schema.fields.get(e.getKey())));
                        }
                        out.add(new Pt(nanos, tags, fields));
                    }
                }
            }
        }
        return out;
    }

    private static Object fieldValue(JsonPrimitive p, String type) {
        if (p.isBoolean()) {
            return p.getAsBoolean();
        }
        if (p.isString() && !"float".equals(type) && !"integer".equals(type)) {
            return p.getAsString(); // (a schema numeric stored as a very long digit run may come back as a string)
        }
        String t = type != null ? type : inferType(p);
        BigDecimal bd = new BigDecimal(p.getAsString());
        return t.equals("integer") ? (Object) bd.longValue() : (Object) bd.doubleValue();
    }

    @Override
    public List<Map<String, String>> series(String db, String rp, String meas, Filter filter) throws SQLException {
        ensureCatalog();
        Set<Map<String, String>> out = new LinkedHashSet<>();
        String table = pgTableName(meas);
        for (BackendTarget t : hostTargets()) {
            ensureMeasurement(t, meas);
            StringBuilder sql = new StringBuilder("SELECT DISTINCT tags::text FROM " + table + " WHERE (db = ? OR db = '') AND rp = ?");
            List<Object> params = new ArrayList<>();
            appendFilter(sql, params, filter);
            try (Connection c = t.open(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
                ps.setString(1, db);
                ps.setString(2, rp);
                bind(ps, 3, params);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> tags = new TreeMap<>();
                        for (var e : JsonParser.parseString(rs.getString(1)).getAsJsonObject().entrySet()) {
                            tags.put(e.getKey(), e.getValue().getAsString());
                        }
                        out.add(tags);
                    }
                }
            }
        }
        return new ArrayList<>(out);
    }

    // ------------------------------------------------------------------ deletes

    @Override
    public void delete(String db, String rp, String meas, Filter filter) throws SQLException {
        ensureCatalog();
        String table = pgTableName(meas);
        boolean remaining = false;
        for (BackendTarget t : hostTargets()) {
            ensureMeasurement(t, meas);
            StringBuilder sql = new StringBuilder("DELETE FROM " + table + " WHERE db = ? AND rp = ?");
            List<Object> params = new ArrayList<>();
            appendFilter(sql, params, filter);
            try (Connection c = t.open(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
                ps.setString(1, db);
                ps.setString(2, rp);
                bind(ps, 3, params);
                ps.executeUpdate();
            }
            try (Connection c = t.open(); PreparedStatement ps = c.prepareStatement(
                    "SELECT 1 FROM " + table + " WHERE db = ? AND rp = ? LIMIT 1")) {
                ps.setString(1, db);
                ps.setString(2, rp);
                try (ResultSet rs = ps.executeQuery()) {
                    remaining |= rs.next();
                }
            }
        }
        if (!remaining) {
            dropCatalogMeasurement(db, rp, meas);
        }
    }

    private void dropCatalogMeasurement(String db, String rp, String meas) throws SQLException {
        try (Connection c = defaultTarget().open()) {
            for (String t : new String[] {"_warp_influx_tagkeys", "_warp_influx_fields", "_warp_influx_meas"}) {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + t + " WHERE db = ? AND rp = ? AND meas = ?")) {
                    ps.setString(1, db);
                    ps.setString(2, rp);
                    ps.setString(3, meas);
                    ps.executeUpdate();
                }
            }
        }
        schemaCache.remove(schemaKey(db, rp, meas));
        schemaLoadedAt.remove(schemaKey(db, rp, meas));
        knownMeas.remove(schemaKey(db, rp, meas));
    }

    @Override
    public void dropMeasurement(String db, String rp, String meas) throws SQLException {
        ensureCatalog();
        String table = pgTableName(meas);
        for (BackendTarget t : hostTargets()) {
            ensureMeasurement(t, meas);
            try (Connection c = t.open(); PreparedStatement ps = c.prepareStatement("DELETE FROM " + table + " WHERE db = ? AND rp = ?")) {
                ps.setString(1, db);
                ps.setString(2, rp);
                ps.executeUpdate();
            }
        }
        dropCatalogMeasurement(db, rp, meas);
    }

    // ------------------------------------------------------------------ helpers

    private static String toJson(Map<String, ?> map) {
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, ?> e : map.entrySet()) {
            Object v = e.getValue();
            if (v instanceof String s) {
                obj.addProperty(e.getKey(), s);
            } else if (v instanceof Boolean b) {
                obj.addProperty(e.getKey(), b);
            } else if (v instanceof Double d) {
                // force a fractional/exponent form so the JSON number is recognisably a float even without the schema
                obj.add(e.getKey(), new JsonPrimitive(new BigDecimal(Double.toString(d))));
            } else if (v instanceof Number n) {
                obj.addProperty(e.getKey(), n);
            } else {
                obj.addProperty(e.getKey(), String.valueOf(v));
            }
        }
        return obj.toString();
    }
}
