package com.sayonora.wire.influxwire;

import com.sayonora.wire.influxwire.InfluxBackend.Filter;
import com.sayonora.wire.influxwire.InfluxBackend.RetentionPolicy;
import com.sayonora.wire.influxwire.InfluxBackend.Schema;
import com.sayonora.wire.influxwire.InfluxFmt.Series;
import com.sayonora.wire.influxwire.InfluxFmt.StmtResult;
import com.sayonora.wire.influxwire.InfluxFmt.TimeV;
import com.sayonora.wire.influxwire.InfluxQl.*;
import com.sayonora.wire.influxwire.InfluxSelect.StmtError;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Executes parsed InfluxQL statements against an {@link InfluxBackend} (SELECT, SHOW, DDL, DELETE, INTO). */
final class InfluxEngine {

    private static final long HOUR = 3_600_000_000_000L;
    private static final long DAY = 24 * HOUR;

    private final InfluxBackend backend;
    private final boolean autoCreateDb;
    /** CREATE USER / DROP USER bookkeeping (in memory only: influxwire has no user model of its own). */
    private final java.util.concurrent.ConcurrentSkipListMap<String, Boolean> users = new java.util.concurrent.ConcurrentSkipListMap<>();

    InfluxEngine(InfluxBackend backend, boolean autoCreateDb) {
        this.backend = backend;
        this.autoCreateDb = autoCreateDb;
    }

    /**
     * @param post whether the HTTP method allowed write statements (POST); write statements over GET still run but
     *     carry InfluxDB's deprecation warning.
     * @throws InfluxQl.ParseError for syntax errors (the whole request is a 400)
     */
    List<StmtResult> execute(String q, String db, String rp, boolean post, Map<String, Object> params) throws SQLException {
        List<Stmt> stmts = InfluxQl.parse(q, params);
        long now = System.currentTimeMillis() * 1_000_000L + (System.nanoTime() % 1_000_000L + 1_000_000L) % 1_000_000L;
        InfluxSelect.Ctx ctx = new InfluxSelect.Ctx(backend, db, rp, now);
        List<StmtResult> out = new ArrayList<>();
        int id = 0;
        for (Stmt st : stmts) {
            StmtResult r = new StmtResult();
            r.id = id++;
            out.add(r);
            try {
                run(st, ctx, post, r);
            } catch (StmtError e) {
                r.series.clear();
                r.error = e.getMessage();
            } catch (InfluxException e) {
                r.series.clear();
                r.error = e.getMessage();
                if (e.status() >= 500) {
                    throw e;
                }
            }
        }
        return out;
    }

    private void run(Stmt st, InfluxSelect.Ctx ctx, boolean post, StmtResult r) throws SQLException {
        if (st instanceof ErrStmt e) {
            throw new StmtError(e.msg);
        }
        if (st instanceof Select s) {
            if (s.into != null) {
                if (!post) {
                    r.messages = new ArrayList<>();
                    r.messages.add(new String[] {"warning", "deprecated use of '" + selectString(s, ctx) + "' in a read only context, please use a POST request instead"});
                }
                selectInto(s, ctx, r);
                return;
            }
            r.series.addAll(InfluxSelect.run(s, ctx));
            return;
        }
        if (st instanceof Explain ex) {
            Series ser = new Series(null, null, List.of(ex.analyze ? "EXPLAIN ANALYZE" : "QUERY PLAN"));
            ser.values.add(new Object[] {""});
            r.series.add(ser);
            return;
        }
        if (st instanceof Show sh) {
            show(sh, ctx, r);
            return;
        }
        Ddl d = (Ddl) st;
        if (!post && !d.kind.equals("DELETE") && !d.kind.equals("USER") && !d.kind.equals("KILL")) {
            r.messages = new ArrayList<>();
            r.messages.add(new String[] {"warning", "deprecated use of '" + ddlString(d, ctx) + "' in a read only context, please use a POST request instead"});
        }
        ddl(d, ctx, r);
    }

    private void checkDb(InfluxSelect.Ctx ctx, String on) throws SQLException {
        String db = on != null ? on : ctx.db;
        if (db == null || db.isEmpty()) {
            throw new StmtError("database name required");
        }
        if (!backend.databaseExists(db)) {
            throw new StmtError("database not found: " + db);
        }
    }

    /** SHOW statements over a database that does not exist simply show nothing (only a missing name is an error). */
    private String dbOf(InfluxSelect.Ctx ctx, String on) throws SQLException {
        String db = on != null ? on : ctx.db;
        if (db == null || db.isEmpty()) {
            throw new StmtError("database name required");
        }
        return db;
    }

    private String rpOf(InfluxSelect.Ctx ctx, String db) throws SQLException {
        if (ctx.rp != null && !ctx.rp.isEmpty()) {
            return ctx.rp;
        }
        return backend.defaultRetentionPolicy(db);
    }

    // ------------------------------------------------------------------ DDL

    private void ddl(Ddl d, InfluxSelect.Ctx ctx, StmtResult r) throws SQLException {
        switch (d.kind) {
            case "CREATE_DATABASE" -> {
                RetentionPolicy rp = null;
                if (d.hasDuration || d.rpName != null || d.shardDuration != 0) {
                    long dur = d.hasDuration ? d.duration : 0;
                    validateDuration(dur);
                    rp = new RetentionPolicy(d.rpName != null ? d.rpName : "autogen", dur, shardDuration(dur, d.shardDuration), d.replication, true);
                }
                backend.createDatabase(d.name, rp);
            }
            case "DROP_DATABASE" -> backend.dropDatabase(d.name);
            case "CREATE_RP" -> {
                if (!backend.databaseExists(d.on)) {
                    throw new StmtError("database not found: " + d.on);
                }
                validateDuration(d.duration);
                RetentionPolicy rp = new RetentionPolicy(d.rpName, d.duration, shardDuration(d.duration, d.shardDuration), d.replication, d.isDefault);
                for (RetentionPolicy e : backend.retentionPolicies(d.on)) {
                    if (e.name().equals(rp.name())) {
                        if (e.durationNanos() == rp.durationNanos() && e.replication() == rp.replication()
                                && e.shardDurationNanos() == rp.shardDurationNanos()) {
                            return;
                        }
                        throw new StmtError("retention policy already exists");
                    }
                }
                backend.createRetentionPolicy(d.on, rp);
            }
            case "ALTER_RP" -> {
                if (!backend.databaseExists(d.on)) {
                    throw new StmtError("database not found: " + d.on);
                }
                RetentionPolicy cur = null;
                for (RetentionPolicy e : backend.retentionPolicies(d.on)) {
                    if (e.name().equals(d.rpName)) {
                        cur = e;
                    }
                }
                if (cur == null) {
                    throw new StmtError("retention policy not found");
                }
                long dur = d.hasDuration ? d.duration : cur.durationNanos();
                validateDuration(dur);
                backend.alterRetentionPolicy(d.on, new RetentionPolicy(d.rpName, dur,
                        d.shardDuration != 0 ? d.shardDuration : (d.hasDuration ? shardDuration(dur, 0) : cur.shardDurationNanos()),
                        d.replication != 1 ? d.replication : cur.replication(), d.isDefault));
            }
            case "DROP_RP" -> {
                if (backend.databaseExists(d.on)) {
                    backend.dropRetentionPolicy(d.on, d.rpName);
                }
            }
            case "DROP_MEASUREMENT" -> {
                String db = dbOf(ctx, null);
                Source s = d.from.get(0);
                String rp = s.rp != null ? s.rp : rpOf(ctx, db);
                List<String> names = new ArrayList<>();
                if (s.regex != null) {
                    for (String m : backend.measurements(db, rp)) {
                        if (s.regex.matcher(m).find()) {
                            names.add(m);
                        }
                    }
                } else if (backend.measurements(db, rp).contains(s.name)) {
                    names.add(s.name);
                }
                for (String m : names) {
                    backend.dropMeasurement(db, rp, m);
                }
            }
            case "DROP_SERIES", "DELETE" -> deleteStmt(d, ctx);
            case "KILL" -> throw new StmtError("no such query id: " + d.name);
            case "CREATE_USER" -> users.put(d.name, d.isDefault);
            case "DROP_USER" -> users.remove(d.name);
            default -> {
                // grants / continuous queries / subscriptions: accepted, no-ops (influxwire has no auth model of its own)
            }
        }
    }

    private static void validateDuration(long dur) {
        if (dur != 0 && dur < HOUR) {
            throw new StmtError("retention policy duration must be at least 1h0m0s");
        }
    }

    private static long shardDuration(long dur, long explicit) {
        if (explicit != 0) {
            return explicit;
        }
        if (dur == 0 || dur > 180 * DAY) {
            return 7 * DAY;
        }
        if (dur < 2 * DAY) {
            return HOUR;
        }
        return DAY;
    }

    private void deleteStmt(Ddl d, InfluxSelect.Ctx ctx) throws SQLException {
        String db = dbOf(ctx, null);
        boolean isDelete = d.kind.equals("DELETE");
        InfluxSelect.TimeRange tr = new InfluxSelect.TimeRange();
        Expr residual = d.cond == null ? null : InfluxSelect.extractTime(d.cond, tr, System.currentTimeMillis() * 1_000_000L);
        if (!isDelete && (tr.hasLo || tr.hasHi)) {
            throw new StmtError("DROP SERIES doesn't support time in WHERE clause");
        }
        List<String[]> targets = new ArrayList<>(); // {rp, measurement}
        if (d.from.isEmpty()) {
            String rp = rpOf(ctx, db);
            for (String m : backend.measurements(db, rp)) {
                targets.add(new String[] {rp, m});
            }
        } else {
            for (Source s : d.from) {
                String rp = s.rp != null ? s.rp : rpOf(ctx, db);
                for (String m : backend.measurements(db, rp)) {
                    if (s.regex != null ? s.regex.matcher(m).find() : m.equals(s.name)) {
                        targets.add(new String[] {rp, m});
                    }
                }
            }
        }
        for (String[] t : targets) {
            Schema sc = backend.schema(db, t[0], t[1]);
            Filter f = new Filter();
            if (tr.hasLo) {
                f.lo = tr.lo;
            }
            if (tr.hasHi) {
                f.hi = tr.hi;
            }
            if (residual != null) {
                if (referencesField(residual, sc)) {
                    throw new StmtError("fields not supported in WHERE clause during deletion");
                }
                List<Map<String, String>> hit = new ArrayList<>();
                InfluxSelect.Env env = new InfluxSelect.Env(sc, System.currentTimeMillis() * 1_000_000L);
                for (Map<String, String> tags : backend.series(db, t[0], t[1], null)) {
                    InfluxBackend.Pt p = new InfluxBackend.Pt(0, tags, new LinkedHashMap<>());
                    if (Boolean.TRUE.equals(InfluxSelect.eval(residual, p, env, true))) {
                        hit.add(tags);
                    }
                }
                f.exact = hit;
            }
            if (isDelete) {
                backend.delete(db, t[0], t[1], f);
            } else {
                backend.delete(db, t[0], t[1], f);
            }
        }
    }

    private static boolean referencesField(Expr e, Schema sc) {
        if (e instanceof VarRef v) {
            return sc.fields.containsKey(v.name) || v.type.equals("field");
        }
        if (e instanceof Paren p) {
            return referencesField(p.e, sc);
        }
        if (e instanceof Bin b) {
            return referencesField(b.l, sc) || referencesField(b.r, sc);
        }
        return false;
    }

    // ------------------------------------------------------------------ SELECT ... INTO

    private void selectInto(Select s, InfluxSelect.Ctx ctx, StmtResult r) throws SQLException {
        String db = s.into.db != null ? s.into.db : ctx.db;
        if (db == null || db.isEmpty()) {
            throw new StmtError("database name required");
        }
        checkDb(ctx, null);
        if (!backend.databaseExists(db)) {
            throw new StmtError("database not found: " + db);
        }
        Select copy = s;
        List<Series> res = InfluxSelect.run(copy, ctx);
        String rp = s.into.rp != null ? s.into.rp : rpOf(new InfluxSelect.Ctx(backend, db, null, 0), db);
        List<InfluxPoint> pts = new ArrayList<>();
        for (Series ser : res) {
            String meas = s.into.name != null ? s.into.name : ser.name;
            for (Object[] row : ser.values) {
                Map<String, Object> fields = new LinkedHashMap<>();
                for (int i = 1; i < ser.columns.size(); i++) {
                    Object v = row[i];
                    if (v != null) {
                        fields.put(ser.columns.get(i), v);
                    }
                }
                if (fields.isEmpty()) {
                    continue;
                }
                pts.add(new InfluxPoint(meas, ser.tags == null ? new TreeMap<>() : new TreeMap<>(ser.tags), fields, ((TimeV) row[0]).nanos()));
            }
        }
        InfluxBackend.WriteOutcome o = backend.write(db, rp, pts, false);
        Series ser = new Series("result", null, List.of("time", "written"));
        ser.values.add(new Object[] {new TimeV(0), (long) (pts.size() - o.dropped())});
        r.series.add(ser);
    }

    // ------------------------------------------------------------------ SHOW

    private List<String> measurementsFor(List<Source> from, String db, String rp) throws SQLException {
        List<String> all = backend.measurements(db, rp);
        if (from.isEmpty()) {
            return all;
        }
        List<String> out = new ArrayList<>();
        for (Source s : from) {
            for (String m : all) {
                if ((s.regex != null ? s.regex.matcher(m).find() : m.equals(s.name)) && !out.contains(m)) {
                    out.add(m);
                }
            }
        }
        return out;
    }

    private static List<Object[]> page(List<Object[]> rows, int offset, int limit) {
        int from = Math.min(offset, rows.size());
        int to = limit > 0 ? Math.min(rows.size(), from + limit) : rows.size();
        return new ArrayList<>(rows.subList(from, to));
    }

    private static <T> List<T> pageList(List<T> rows, int offset, int limit) {
        int from = Math.min(offset, rows.size());
        int to = limit > 0 ? Math.min(rows.size(), from + limit) : rows.size();
        return new ArrayList<>(rows.subList(from, to));
    }

    /** Tag sets of a measurement that satisfy the WHERE clause of a SHOW statement. */
    private List<Map<String, String>> matchingSeries(String db, String rp, String m, Expr cond, Schema sc) throws SQLException {
        Filter f = null;
        Expr residual = null;
        if (cond != null) {
            InfluxSelect.TimeRange tr = new InfluxSelect.TimeRange();
            residual = InfluxSelect.extractTime(cond, tr, System.currentTimeMillis() * 1_000_000L);
            f = new Filter();
            if (tr.hasLo) {
                f.lo = tr.lo;
            }
            if (tr.hasHi) {
                f.hi = tr.hi;
            }
        }
        List<Map<String, String>> all = backend.series(db, rp, m, f);
        if (residual == null) {
            return all;
        }
        List<Map<String, String>> out = new ArrayList<>();
        InfluxSelect.Env env = new InfluxSelect.Env(sc, System.currentTimeMillis() * 1_000_000L);
        for (Map<String, String> tags : all) {
            if (Boolean.TRUE.equals(InfluxSelect.eval(residual, new InfluxBackend.Pt(0, tags, new LinkedHashMap<>()), env, true))) {
                out.add(tags);
            }
        }
        return out;
    }

    private static String escKey(String s, boolean tag) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == ',' || c == ' ' || (tag && c == '=')) {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private void show(Show sh, InfluxSelect.Ctx ctx, StmtResult r) throws SQLException {
        switch (sh.kind) {
            case "DATABASES" -> {
                Series s = new Series("databases", null, List.of("name"));
                for (String d : backend.databases()) {
                    s.values.add(new Object[] {d});
                }
                if (!s.values.isEmpty()) {
                    r.series.add(s);
                }
            }
            case "RETENTION_POLICIES" -> {
                String db = sh.on != null ? sh.on : ctx.db;
                if (db == null || db.isEmpty()) {
                    throw new StmtError("database name required");
                }
                if (!backend.databaseExists(db)) {
                    throw new StmtError("database not found: " + db);
                }
                Series s = new Series(null, null, List.of("name", "duration", "shardGroupDuration", "replicaN", "default"));
                for (RetentionPolicy p : backend.retentionPolicies(db)) {
                    s.values.add(new Object[] {p.name(), InfluxFmt.goDuration(p.durationNanos()), InfluxFmt.goDuration(p.shardDurationNanos()),
                        (long) p.replication(), p.isDefault()});
                }
                r.series.add(s);
            }
            case "MEASUREMENTS" -> {
                String db = dbOf(ctx, sh.on);
                String rp = rpOf(ctx, db);
                List<Object[]> rows = new ArrayList<>();
                for (String m : backend.measurements(db, rp)) {
                    if (sh.withMeasOp != null) {
                        boolean ok = switch (sh.withMeasOp) {
                            case "=" -> m.equals(sh.withMeas);
                            case "!=" -> !m.equals(sh.withMeas);
                            case "=~" -> sh.withMeasRegex.matcher(m).find();
                            default -> !sh.withMeasRegex.matcher(m).find();
                        };
                        if (!ok) {
                            continue;
                        }
                    }
                    if (sh.cond != null && matchingSeries(db, rp, m, sh.cond, backend.schema(db, rp, m)).isEmpty()) {
                        continue;
                    }
                    rows.add(new Object[] {m});
                }
                rows = page(rows, sh.offset, sh.limit);
                if (!rows.isEmpty()) {
                    Series s = new Series("measurements", null, List.of("name"));
                    s.values = rows;
                    r.series.add(s);
                }
            }
            case "MEASUREMENT_CARDINALITY" -> {
                String db = dbOf(ctx, sh.on);
                Series s = new Series(null, null, List.of("cardinality estimation"));
                s.values.add(new Object[] {(long) backend.measurements(db, rpOf(ctx, db)).size()});
                r.series.add(s);
            }
            case "SERIES" -> {
                String db = dbOf(ctx, sh.on);
                String rp = rpOf(ctx, db);
                TreeSet<String> keys = new TreeSet<>();
                for (String m : measurementsFor(sh.from, db, rp)) {
                    Schema sc = backend.schema(db, rp, m);
                    for (Map<String, String> tags : matchingSeries(db, rp, m, sh.cond, sc)) {
                        StringBuilder k = new StringBuilder(escKey(m, false));
                        for (var e : new TreeMap<>(tags).entrySet()) {
                            k.append(',').append(escKey(e.getKey(), true)).append('=').append(escKey(e.getValue(), true));
                        }
                        keys.add(k.toString());
                    }
                }
                if (sh.cardinality) {
                    Series s = new Series(null, null, List.of("cardinality estimation"));
                    s.values.add(new Object[] {(long) keys.size()});
                    r.series.add(s);
                    return;
                }
                List<Object[]> rows = new ArrayList<>();
                for (String k : keys) {
                    rows.add(new Object[] {k});
                }
                rows = page(rows, sh.offset, sh.limit);
                if (!rows.isEmpty()) {
                    Series s = new Series(null, null, List.of("key"));
                    s.values = rows;
                    r.series.add(s);
                }
            }
            case "TAG_KEYS" -> {
                String db = dbOf(ctx, sh.on);
                String rp = rpOf(ctx, db);
                List<Series> all = new ArrayList<>();
                for (String m : measurementsFor(sh.from, db, rp)) {
                    TreeSet<String> keys = new TreeSet<>();
                    Schema sc = backend.schema(db, rp, m);
                    for (Map<String, String> tags : matchingSeries(db, rp, m, sh.cond, sc)) {
                        keys.addAll(tags.keySet());
                    }
                    if (keys.isEmpty()) {
                        continue;
                    }
                    List<Object[]> rows = new ArrayList<>();
                    for (String k : keys) {
                        rows.add(new Object[] {k});
                    }
                    rows = page(rows, sh.offset, sh.limit);
                    if (rows.isEmpty()) {
                        continue;
                    }
                    Series s = new Series(m, null, List.of("tagKey"));
                    s.values = rows;
                    all.add(s);
                }
                r.series.addAll(pageList(all, sh.soffset, sh.slimit));
            }
            case "TAG_VALUES" -> {
                String db = dbOf(ctx, sh.on);
                String rp = rpOf(ctx, db);
                List<Series> all = new ArrayList<>();
                for (String m : measurementsFor(sh.from, db, rp)) {
                    Schema sc = backend.schema(db, rp, m);
                    TreeSet<String> pairs = new TreeSet<>();
                    for (Map<String, String> tags : matchingSeries(db, rp, m, sh.cond, sc)) {
                        for (var e : tags.entrySet()) {
                            if (keyMatches(sh, e.getKey())) {
                                pairs.add(e.getKey() + "\u0000" + e.getValue());
                            }
                        }
                    }
                    if (pairs.isEmpty()) {
                        continue;
                    }
                    if (sh.cardinality) {
                        Series s = new Series(m, null, List.of("count"));
                        s.values.add(new Object[] {(long) pairs.size()});
                        all.add(s);
                        continue;
                    }
                    List<Object[]> rows = new ArrayList<>();
                    for (String p : pairs) {
                        int i = p.indexOf('\u0000');
                        rows.add(new Object[] {p.substring(0, i), p.substring(i + 1)});
                    }
                    rows = page(rows, sh.offset, sh.limit);
                    if (rows.isEmpty()) {
                        continue;
                    }
                    Series s = new Series(m, null, List.of("key", "value"));
                    s.values = rows;
                    all.add(s);
                }
                r.series.addAll(pageList(all, sh.soffset, sh.slimit));
            }
            case "FIELD_KEYS" -> {
                String db = dbOf(ctx, sh.on);
                String rp = rpOf(ctx, db);
                List<Series> all = new ArrayList<>();
                for (String m : measurementsFor(sh.from, db, rp)) {
                    Schema sc = backend.schema(db, rp, m);
                    List<Object[]> rows = new ArrayList<>();
                    for (var e : new TreeMap<>(sc.fields).entrySet()) {
                        rows.add(new Object[] {e.getKey(), e.getValue()});
                    }
                    rows = page(rows, sh.offset, sh.limit);
                    if (rows.isEmpty()) {
                        continue;
                    }
                    Series s = new Series(m, null, List.of("fieldKey", "fieldType"));
                    s.values = rows;
                    all.add(s);
                }
                r.series.addAll(pageList(all, sh.soffset, sh.slimit));
            }
            case "TAG_KEY_CARDINALITY", "FIELD_KEY_CARDINALITY" -> {
                String db = dbOf(ctx, sh.on);
                String rp = rpOf(ctx, db);
                for (String m : backend.measurements(db, rp)) {
                    Schema sc = backend.schema(db, rp, m);
                    Series s = new Series(m, null, List.of("count"));
                    s.values.add(new Object[] {(long) (sh.kind.startsWith("TAG") ? sc.tagKeys.size() : sc.fields.size())});
                    r.series.add(s);
                }
            }
            case "USERS" -> {
                Series s = new Series(null, null, List.of("user", "admin"));
                users.forEach((u, admin) -> s.values.add(new Object[] {u, admin}));
                r.series.add(s);
            }
            case "GRANTS" -> throw new StmtError("user not found");
            case "QUERIES" -> {
                Series s = new Series(null, null, List.of("qid", "query", "database", "duration", "status"));
                s.values.add(new Object[] {1L, "SHOW QUERIES", ctx.db == null ? "" : ctx.db, "0s", "running"});
                r.series.add(s);
            }
            case "CONTINUOUS_QUERIES" -> {
                for (String d : backend.databases()) {
                    r.series.add(new Series(d, null, List.of("name", "query")));
                }
            }
            case "STATS", "DIAGNOSTICS", "SUBSCRIPTIONS", "SHARDS", "SHARD_GROUPS" -> {
                // not modelled
            }
            default -> throw new StmtError("unsupported SHOW " + sh.kind);
        }
    }

    private static boolean keyMatches(Show sh, String key) {
        return switch (sh.withOp) {
            case "=" -> key.equals(sh.withKeys.get(0));
            case "!=" -> !key.equals(sh.withKeys.get(0));
            case "IN" -> sh.withKeys.contains(key);
            case "=~" -> sh.withRegex.matcher(key).find();
            default -> !sh.withRegex.matcher(key).find();
        };
    }

    // ------------------------------------------------------------------ statement text (GET deprecation warnings)

    private String ddlString(Ddl d, InfluxSelect.Ctx ctx) {
        switch (d.kind) {
            case "CREATE_DATABASE":
                return "CREATE DATABASE " + InfluxQl.quoteIdent(d.name);
            case "DROP_DATABASE":
                return "DROP DATABASE " + InfluxQl.quoteIdent(d.name);
            case "DROP_MEASUREMENT":
                return "DROP MEASUREMENT " + (d.from.get(0).name != null ? InfluxQl.quoteIdent(d.from.get(0).name) : "/" + d.from.get(0).regex + "/");
            case "CREATE_RP":
                return "CREATE RETENTION POLICY " + InfluxQl.quoteIdent(d.rpName) + " ON " + InfluxQl.quoteIdent(d.on) + " DURATION "
                        + shortDuration(d.duration) + " REPLICATION " + d.replication + (d.isDefault ? " DEFAULT" : "");
            case "ALTER_RP":
                return "ALTER RETENTION POLICY " + InfluxQl.quoteIdent(d.rpName) + " ON " + InfluxQl.quoteIdent(d.on)
                        + (d.hasDuration ? " DURATION " + InfluxFmt.goDuration(d.duration) : "") + (d.isDefault ? " DEFAULT" : "");
            case "DROP_RP":
                return "DROP RETENTION POLICY " + InfluxQl.quoteIdent(d.rpName) + " ON " + InfluxQl.quoteIdent(d.on);
            case "DROP_SERIES":
                return "DROP SERIES" + (d.from.isEmpty() ? "" : " FROM " + d.from.get(0).name)
                        + (d.cond == null ? "" : " WHERE " + InfluxSelect.exprString(d.cond));
            default:
                return d.kind;
        }
    }

    /** influxql.FormatDuration: the largest whole unit (w d h m s ms u ns). */
    private static String shortDuration(long nanos) {
        if (nanos == 0) {
            return "0s";
        }
        long[] mult = {604_800_000_000_000L, DAY, HOUR, 60_000_000_000L, 1_000_000_000L, 1_000_000L, 1_000L, 1L};
        String[] unit = {"w", "d", "h", "m", "s", "ms", "u", "ns"};
        for (int i = 0; i < mult.length; i++) {
            if (nanos % mult[i] == 0) {
                return (nanos / mult[i]) + unit[i];
            }
        }
        return nanos + "ns";
    }

    private String selectString(Select s, InfluxSelect.Ctx ctx) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < s.fields.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(InfluxSelect.exprString(s.fields.get(i).expr));
            if (s.fields.get(i).alias != null) {
                sb.append(" AS ").append(InfluxQl.quoteIdent(s.fields.get(i).alias));
            }
        }
        String db = ctx.db == null ? "" : ctx.db;
        sb.append(" INTO ").append(qual(s.into, db)).append(" FROM ");
        for (int i = 0; i < s.sources.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(s.sources.get(i).sub != null ? "(...)" : qual(s.sources.get(i), db));
        }
        if (s.cond != null) {
            sb.append(" WHERE ").append(InfluxSelect.exprString(s.cond));
        }
        return sb.toString();
    }

    private static String qual(Source s, String db) {
        String name = s.regex != null ? "/" + s.regex.pattern() + "/" : InfluxQl.quoteIdent(s.name);
        return InfluxQl.quoteIdent(s.db != null ? s.db : db) + "." + InfluxQl.quoteIdent(s.rp != null ? s.rp : "autogen") + "." + name;
    }
}
