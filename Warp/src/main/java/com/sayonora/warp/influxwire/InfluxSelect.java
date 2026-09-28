package com.sayonora.warp.influxwire;

import com.sayonora.warp.influxwire.InfluxBackend.Filter;
import com.sayonora.warp.influxwire.InfluxBackend.Pt;
import com.sayonora.warp.influxwire.InfluxBackend.Schema;
import com.sayonora.warp.influxwire.InfluxFmt.Series;
import com.sayonora.warp.influxwire.InfluxFmt.TimeV;
import com.sayonora.warp.influxwire.InfluxQl.*;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * InfluxQL SELECT execution over points fetched from an {@link InfluxBackend}. Semantics follow InfluxDB 1.x:
 * series per (measurement, GROUP BY tags), time-bucketed aggregation aligned to the epoch, fill(), selectors
 * reporting the selected point's time, transformations, arithmetic, subqueries.
 *
 * <p>Points from every shard are merged before evaluation, so every function is exact across shards.
 */
final class InfluxSelect {

    static final class StmtError extends RuntimeException {
        StmtError(String m) {
            super(m, null, false, false);
        }
    }

    static final class Ctx {
        final InfluxBackend backend;
        final String db;
        final String rp;
        final long now;

        Ctx(InfluxBackend backend, String db, String rp, long now) {
            this.backend = backend;
            this.db = db;
            this.rp = rp;
            this.now = now;
        }
    }

    // ------------------------------------------------------------------ function tables

    static final Set<String> AGG = Set.of("count", "distinct", "integral", "mean", "median", "mode", "spread", "stddev", "sum",
            "first", "last", "max", "min", "percentile", "sample", "top", "bottom");
    static final Set<String> SELECTORS = Set.of("first", "last", "max", "min", "percentile", "sample", "top", "bottom");
    static final Set<String> TRANSFORMS = Set.of("derivative", "non_negative_derivative", "difference", "non_negative_difference",
            "elapsed", "moving_average", "cumulative_sum");
    static final Set<String> MATH = Set.of("abs", "acos", "asin", "atan", "atan2", "ceil", "cos", "exp", "floor", "ln", "log",
            "log10", "log2", "pow", "round", "sin", "sqrt", "tan");

    private static int[] arity(String fn) {
        return switch (fn) {
            case "count", "distinct", "mean", "median", "mode", "spread", "stddev", "sum", "first", "last", "max", "min",
                    "difference", "non_negative_difference", "cumulative_sum", "abs", "acos", "asin", "atan", "ceil", "cos",
                    "exp", "floor", "ln", "log10", "log2", "round", "sin", "sqrt", "tan" -> new int[] {1, 1};
            case "integral", "derivative", "non_negative_derivative", "elapsed" -> new int[] {1, 2};
            case "percentile", "sample", "moving_average", "atan2", "log", "pow" -> new int[] {2, 2};
            case "top", "bottom" -> new int[] {2, Integer.MAX_VALUE};
            default -> null;
        };
    }

    // ------------------------------------------------------------------ data structures

    static final class DataSet {
        String name;
        List<Pt> pts;
        Schema schema;
    }

    static final class TimeRange {
        long lo = Long.MIN_VALUE;
        long hi = Long.MAX_VALUE;
        boolean hasLo;
        boolean hasHi;
    }

    static final class Cell {
        final long t;
        final Object v;
        final Pt pt;

        Cell(long t, Object v, Pt pt) {
            this.t = t;
            this.v = v;
            this.pt = pt;
        }
    }

    static final class Sample {
        final long t;
        final Object v;
        final Pt pt;

        Sample(long t, Object v, Pt pt) {
            this.t = t;
            this.v = v;
            this.pt = pt;
        }
    }

    /** One output column of the select list. */
    static final class Col {
        String name;
        Expr expr;
        boolean raw; // plain variable (or expression of variables only)
    }

    // ------------------------------------------------------------------ entry point

    static List<Series> run(Select s, Ctx ctx) throws SQLException {
        return run(s, ctx, null);
    }

    static List<Series> run(Select s, Ctx ctx, TimeRange outer) throws SQLException {
        // --- time range / residual condition
        TimeRange tr = new TimeRange();
        Expr residual = s.cond == null ? null : extractTime(s.cond, tr, ctx.now);
        if (outer != null) {
            if (outer.hasLo && (!tr.hasLo || outer.lo > tr.lo)) {
                tr.lo = outer.lo;
                tr.hasLo = true;
            }
            if (outer.hasHi && (!tr.hasHi || outer.hi < tr.hi)) {
                tr.hi = outer.hi;
                tr.hasHi = true;
            }
        }
        if (residual != null) {
            validateCondition(residual);
        }
        boolean groupByTime = false;
        Dim timeDim = null;
        for (Dim d : s.dims) {
            if (d.kind == Dim.TIME) {
                if (timeDim != null) {
                    throw new StmtError("multiple time dimensions not allowed");
                }
                timeDim = d;
                groupByTime = true;
                if (d.interval <= 0) {
                    throw new StmtError("GROUP BY interval must be greater than zero");
                }
            }
        }
        // --- sources
        List<DataSet> sets = loadSources(s, ctx, tr, residual);
        // --- union schema for wildcard expansion / classification
        Schema union = new Schema();
        for (DataSet ds : sets) {
            for (var e : ds.schema.fields.entrySet()) {
                union.fields.putIfAbsent(e.getKey(), e.getValue());
            }
            union.tagKeys.addAll(ds.schema.tagKeys);
        }
        // dims -> tag names
        List<String> dimTags = new ArrayList<>();
        for (Dim d : s.dims) {
            if (d.kind == Dim.TAG) {
                dimTags.add(d.name);
            } else if (d.kind == Dim.WILDCARD) {
                dimTags.addAll(union.tagKeys);
            } else if (d.kind == Dim.REGEX) {
                for (String k : union.tagKeys) {
                    if (d.regex.matcher(k).find()) {
                        dimTags.add(k);
                    }
                }
            }
        }
        List<String> dimNames = new ArrayList<>(new java.util.TreeSet<>(dimTags));
        // --- analysis of the select list
        Analysis an = analyse(s, union, dimNames, groupByTime);
        an.nested = outer != null;
        List<Series> out = new ArrayList<>();
        for (DataSet ds : sets) {
            out.addAll(runDataset(ds, s, an, tr, residual, timeDim, dimNames, ctx));
        }
        // SLIMIT / SOFFSET over the series list
        if (s.soffset > 0 || s.hasSlimit) {
            int from = Math.min(s.soffset, out.size());
            int to;
            if (s.hasSlimit && s.slimit > 0) {
                to = Math.min(out.size(), from + s.slimit);
            } else if (s.hasSlimit) {
                to = out.size(); // SLIMIT 0 = unlimited
            } else {
                to = from; // SOFFSET without SLIMIT yields nothing (InfluxDB quirk)
            }
            out = new ArrayList<>(out.subList(from, to));
        }
        return out;
    }

    // ------------------------------------------------------------------ sources

    private static List<DataSet> loadSources(Select s, Ctx ctx, TimeRange tr, Expr residual) throws SQLException {
        List<DataSet> out = new ArrayList<>();
        for (Source src : s.sources) {
            if (src.sub != null) {
                List<Series> inner = run(src.sub, ctx, tr);
                for (Series ser : inner) {
                    DataSet ds = new DataSet();
                    ds.name = ser.name;
                    ds.schema = new Schema();
                    ds.pts = new ArrayList<>();
                    if (ser.tags != null) {
                        ds.schema.tagKeys.addAll(ser.tags.keySet());
                    }
                    for (int ri = 0; ri < ser.values.size(); ri++) {
                        Object[] row = ser.values.get(ri);
                        Map<String, Object> f = new LinkedHashMap<>();
                        for (int i = 1; i < ser.columns.size(); i++) {
                            if (row[i] != null) {
                                f.put(ser.columns.get(i), row[i]);
                                ds.schema.fields.putIfAbsent(ser.columns.get(i), typeName(row[i]));
                            }
                        }
                        for (String c : ser.columns.subList(1, ser.columns.size())) {
                            ds.schema.fields.putIfAbsent(c, "float");
                        }
                        Map<String, String> tags = new TreeMap<>();
                        if (ser.rowTags != null) {
                            tags.putAll(ser.rowTags.get(ri));
                        }
                        if (ser.tags != null) {
                            tags.putAll(ser.tags);
                        }
                        ds.pts.add(new Pt(((TimeV) row[0]).nanos(), tags, f));
                    }
                    ds.pts.sort(Comparator.comparingLong(p -> p.time));
                    mergeInto(out, ds);
                }
                continue;
            }
            String db = src.db != null ? src.db : ctx.db;
            if (db == null || db.isEmpty()) {
                throw new StmtError("database name required");
            }
            if (!ctx.backend.databaseExists(db)) {
                throw new StmtError("database not found: " + db);
            }
            String rp = src.rp != null ? src.rp : rpOf(ctx, db);
            if (src.rp != null && !hasRp(ctx.backend, db, src.rp)) {
                throw new StmtError("retention policy not found: " + src.rp);
            }
            if (src.rp == null && ctx.rp != null && !ctx.rp.isEmpty() && !hasRp(ctx.backend, db, ctx.rp)) {
                throw new StmtError("retention policy not found: " + ctx.rp);
            }
            List<String> names = new ArrayList<>();
            if (src.regex != null) {
                for (String m : ctx.backend.measurements(db, rp)) {
                    if (src.regex.matcher(m).find()) {
                        names.add(m);
                    }
                }
            } else {
                names.add(src.name);
            }
            for (String m : names) {
                Schema sc = ctx.backend.schema(db, rp, m);
                if (sc.fields.isEmpty() && sc.tagKeys.isEmpty()) {
                    continue;
                }
                Filter f = new Filter();
                if (tr.hasLo) {
                    f.lo = tr.lo;
                }
                if (tr.hasHi) {
                    f.hi = tr.hi;
                }
                f.tree = pushdown(residual, sc);
                DataSet ds = new DataSet();
                ds.name = m;
                ds.schema = sc;
                ds.pts = ctx.backend.fetch(db, rp, m, sc, f);
                ds.pts.sort(Comparator.<Pt>comparingLong(p -> p.time).thenComparing(p -> p.tags.toString()));
                mergeInto(out, ds);
            }
        }
        return out;
    }

    private static void mergeInto(List<DataSet> out, DataSet ds) {
        for (DataSet e : out) {
            if (e.name.equals(ds.name) && e.schema != null && ds.schema != null) {
                // same measurement name from several subquery series: merge points (kept sorted)
                e.pts.addAll(ds.pts);
                e.pts.sort(Comparator.comparingLong(p -> p.time));
                for (var en : ds.schema.fields.entrySet()) {
                    e.schema.fields.putIfAbsent(en.getKey(), en.getValue());
                }
                e.schema.tagKeys.addAll(ds.schema.tagKeys);
                return;
            }
        }
        out.add(ds);
    }

    private static String rpOf(Ctx ctx, String db) throws SQLException {
        if (ctx.rp != null && !ctx.rp.isEmpty()) {
            return ctx.rp;
        }
        return ctx.backend.defaultRetentionPolicy(db);
    }

    private static boolean hasRp(InfluxBackend b, String db, String rp) throws SQLException {
        for (InfluxBackend.RetentionPolicy r : b.retentionPolicies(db)) {
            if (r.name().equals(rp)) {
                return true;
            }
        }
        return false;
    }

    static String typeName(Object v) {
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

    // ------------------------------------------------------------------ condition handling

    /**
     * Pulls every {@code time} comparison (at any depth, like InfluxQL's condition splitter) into {@code tr} and returns the
     * remaining condition (or null).
     */
    static Expr extractTime(Expr cond, TimeRange tr, long now) {
        return strip(cond, tr, now);
    }

    private static Expr strip(Expr e, TimeRange tr, long now) {
        if (e instanceof Paren p) {
            Expr in = strip(p.e, tr, now);
            return in == null ? null : new Paren(in);
        }
        if (e instanceof Bin b) {
            if (b.op.equals("AND") || b.op.equals("OR")) {
                Expr l = strip(b.l, tr, now);
                Expr r = strip(b.r, tr, now);
                if (l == null) {
                    return r;
                }
                if (r == null) {
                    return l;
                }
                return new Bin(b.op, l, r);
            }
            if (isCmp(b.op) && (isTimeVar(b.l) || isTimeVar(b.r))) {
                if (b.op.equals("=~") || b.op.equals("!~")) {
                    throw new StmtError("invalid operation: time and *influxql.RegexLiteral are not compatible");
                }
                if (b.op.equals("!=")) {
                    throw new StmtError("invalid time comparison operator: " + b.op);
                }
                boolean left = isTimeVar(b.l);
                Expr other = left ? b.r : b.l;
                Long v = timeValueOf(other, now);
                if (v == null) {
                    throw new StmtError("invalid operation: time and " + goType(other) + " are not compatible");
                }
                String op = left ? b.op : flip(b.op);
                switch (op) {
                    case ">" -> {
                        long lo = v == Long.MAX_VALUE ? v : v + 1;
                        if (!tr.hasLo || lo > tr.lo) {
                            tr.lo = lo;
                        }
                        tr.hasLo = true;
                    }
                    case ">=" -> {
                        if (!tr.hasLo || v > tr.lo) {
                            tr.lo = v;
                        }
                        tr.hasLo = true;
                    }
                    case "<" -> {
                        long hi = v == Long.MIN_VALUE ? v : v - 1;
                        if (!tr.hasHi || hi < tr.hi) {
                            tr.hi = hi;
                        }
                        tr.hasHi = true;
                    }
                    case "<=" -> {
                        if (!tr.hasHi || v < tr.hi) {
                            tr.hi = v;
                        }
                        tr.hasHi = true;
                    }
                    default -> {
                        tr.lo = Math.max(tr.hasLo ? tr.lo : Long.MIN_VALUE, v);
                        tr.hi = Math.min(tr.hasHi ? tr.hi : Long.MAX_VALUE, v);
                        tr.hasLo = true;
                        tr.hasHi = true;
                    }
                }
                return null;
            }
        }
        return e;
    }

    private static String goType(Expr e) {
        e = unparen(e);
        if (e instanceof StrLit) {
            return "*influxql.StringLiteral";
        }
        if (e instanceof IntLit) {
            return "*influxql.IntegerLiteral";
        }
        if (e instanceof NumLit) {
            return "*influxql.NumberLiteral";
        }
        if (e instanceof BoolLit) {
            return "*influxql.BooleanLiteral";
        }
        if (e instanceof VarRef) {
            return "*influxql.VarRef";
        }
        if (e instanceof RegexLit) {
            return "*influxql.RegexLiteral";
        }
        if (e instanceof Bin) {
            return "*influxql.BinaryExpr";
        }
        return "*influxql.Call";
    }

    private static void flattenAnd(Expr e, List<Expr> out) {
        Expr u = e;
        while (u instanceof Paren p && unparen(p.e) instanceof Bin bb && bb.op.equals("AND")) {
            u = p.e;
        }
        if (u instanceof Bin b && b.op.equals("AND")) {
            flattenAnd(b.l, out);
            flattenAnd(b.r, out);
        } else {
            out.add(e);
        }
    }

    static Expr unparen(Expr e) {
        while (e instanceof Paren p) {
            e = p.e;
        }
        return e;
    }

    private static boolean isCmp(String op) {
        return switch (op) {
            case "=", "!=", "<", "<=", ">", ">=", "=~", "!~" -> true;
            default -> false;
        };
    }

    private static boolean isTimeVar(Expr e) {
        e = unparen(e);
        return e instanceof VarRef v && v.name.equalsIgnoreCase("time") && (v.type.isEmpty());
    }

    private static String flip(String op) {
        return switch (op) {
            case ">" -> "<";
            case ">=" -> "<=";
            case "<" -> ">";
            case "<=" -> ">=";
            default -> op;
        };
    }

    /** Evaluates a time-valued literal expression to epoch nanoseconds, or null if it is not one. */
    static Long timeValueOf(Expr e, long now) {
        e = unparen(e);
        if (e instanceof TimeLit t) {
            return t.nanos == -1 ? now : t.nanos;
        }
        if (e instanceof IntLit i) {
            return i.v;
        }
        if (e instanceof DurLit d) {
            return d.nanos;
        }
        if (e instanceof StrLit s) {
            return parseTimeString(s.v);
        }
        if (e instanceof Bin b && (b.op.equals("+") || b.op.equals("-"))) {
            Long l = timeValueOf(b.l, now);
            Long r = timeValueOf(b.r, now);
            if (l == null || r == null || !(unparen(b.l) instanceof TimeLit || unparen(b.r) instanceof DurLit || unparen(b.l) instanceof DurLit)) {
                return null;
            }
            return b.op.equals("+") ? l + r : l - r;
        }
        return null;
    }

    static Long parseTimeString(String s) {
        try {
            OffsetDateTime o = OffsetDateTime.parse(s);
            return o.toEpochSecond() * 1_000_000_000L + o.getNano();
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            String t = s.replace(' ', 'T');
            if (t.length() == 10) {
                t += "T00:00:00";
            }
            java.time.LocalDateTime l = java.time.LocalDateTime.parse(t);
            return l.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000_000L + l.getNano();
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private static void validateCondition(Expr e) {
        Expr u = unparen(e);
        if (u instanceof BoolLit) {
            return;
        }
        if (u instanceof Bin b) {
            if (b.op.equals("AND") || b.op.equals("OR")) {
                validateCondition(b.l);
                validateCondition(b.r);
                return;
            }
            if (isCmp(b.op)) {
                return;
            }
        }
        throw new StmtError("invalid condition expression: " + exprString(e));
    }

    static String exprString(Expr e) {
        if (e instanceof VarRef v) {
            return InfluxQl.quoteIdent(v.name) + (v.type.isEmpty() ? "" : "::" + v.type);
        }
        if (e instanceof StrLit s) {
            return "'" + s.v.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n") + "'";
        }
        if (e instanceof IntLit i) {
            return Long.toString(i.v);
        }
        if (e instanceof NumLit n) {
            String x = InfluxFmt.jsonFloat(n.v);
            return x.contains(".") || x.contains("e") ? x : x + ".000";
        }
        if (e instanceof BoolLit b) {
            return Boolean.toString(b.v);
        }
        if (e instanceof DurLit d) {
            return InfluxFmt.goDuration(d.nanos);
        }
        if (e instanceof TimeLit) {
            return "now()";
        }
        if (e instanceof RegexLit r) {
            return "/" + r.source.replace("/", "\\/") + "/";
        }
        if (e instanceof Wildcard w) {
            return "*" + (w.type.isEmpty() ? "" : "::" + w.type);
        }
        if (e instanceof Paren p) {
            return "(" + exprString(p.e) + ")";
        }
        if (e instanceof Call c) {
            StringBuilder sb = new StringBuilder(c.name).append('(');
            for (int i = 0; i < c.args.size(); i++) {
                sb.append(i > 0 ? ", " : "").append(exprString(c.args.get(i)));
            }
            return sb.append(')').toString();
        }
        Bin b = (Bin) e;
        return exprString(b.l) + " " + b.op + " " + exprString(b.r);
    }

    /** Superset-safe SQL pushdown of tag equality conditions. */
    private static Filter.Node pushdown(Expr cond, Schema sc) {
        if (cond == null) {
            return null;
        }
        Expr u = unparen(cond);
        if (u instanceof Bin b) {
            if (b.op.equals("AND") || b.op.equals("OR")) {
                Filter.Node l = pushdown(b.l, sc);
                Filter.Node r = pushdown(b.r, sc);
                if (b.op.equals("AND")) {
                    if (l == null) {
                        return r;
                    }
                    if (r == null) {
                        return l;
                    }
                    return new Filter.Group(true, List.of(l, r));
                }
                if (l == null || r == null) {
                    return null;
                }
                return new Filter.Group(false, List.of(l, r));
            }
            if ((b.op.equals("=") || b.op.equals("!=")) && unparen(b.l) instanceof VarRef v && unparen(b.r) instanceof StrLit lit
                    && (v.type.isEmpty() || v.type.equals("tag")) && !v.name.equalsIgnoreCase("time")
                    && !sc.fields.containsKey(v.name) && sc.tagKeys.contains(v.name)) {
                return new Filter.Leaf(v.name, b.op, lit.v);
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ select-list analysis

    static final class Analysis {
        List<Col> cols = new ArrayList<>();
        boolean aggregate;
        boolean hasTransform;
        boolean singleSelector;
        boolean hasRawRef; // plain variables in an aggregate query (selector passthrough)
        boolean fillIsMeaningful;
        String timeAlias;
        boolean nested;
    }

    private static boolean containsCall(Expr e, java.util.function.Predicate<String> p) {
        if (e instanceof Call c) {
            if (p.test(c.name)) {
                return true;
            }
            for (Expr a : c.args) {
                if (containsCall(a, p)) {
                    return true;
                }
            }
            return false;
        }
        if (e instanceof Bin b) {
            return containsCall(b.l, p) || containsCall(b.r, p);
        }
        if (e instanceof Paren pp) {
            return containsCall(pp.e, p);
        }
        return false;
    }

    private static boolean hasVar(Expr e) {
        if (e instanceof VarRef) {
            return true;
        }
        if (e instanceof Call c) {
            return c.args.stream().anyMatch(InfluxSelect::hasVar);
        }
        if (e instanceof Bin b) {
            return hasVar(b.l) || hasVar(b.r);
        }
        if (e instanceof Paren p) {
            return hasVar(p.e);
        }
        return e instanceof Wildcard || e instanceof RegexLit;
    }

    private static boolean hasBareVar(Expr e) {
        if (e instanceof VarRef) {
            return true;
        }
        if (e instanceof Bin b) {
            return hasBareVar(b.l) || hasBareVar(b.r);
        }
        if (e instanceof Paren p) {
            return hasBareVar(p.e);
        }
        return false;
    }

    private static Analysis analyse(Select s, Schema union, List<String> dimNames, boolean groupByTime) {
        Analysis an = new Analysis();
        List<Field> expanded = new ArrayList<>();
        for (Field f : s.fields) {
            expandField(f, union, dimNames, expanded, s);
        }
        // `time` in the select list is not a column of its own (an alias renames the time column)
        List<Field> kept = new ArrayList<>();
        for (Field f : expanded) {
            if (f.expr instanceof VarRef v && v.name.equalsIgnoreCase("time") && v.type.isEmpty()) {
                if (f.alias != null) {
                    an.timeAlias = f.alias;
                }
                continue;
            }
            kept.add(f);
        }
        if (kept.isEmpty() && !expanded.isEmpty()) {
            throw new StmtError("at least 1 non-time field must be queried");
        }
        expanded = kept;
        for (Field f : expanded) {
            validateExpr(f.expr, 0);
        }
        for (Field f : expanded) {
            if (!hasVar(f.expr) && !(f.expr instanceof Call c && c.name.equals("now"))) {
                throw new StmtError("field must contain at least one variable");
            }
        }
        Set<String> used = new LinkedHashSet<>();
        Map<String, Integer> dup = new HashMap<>();
        for (Field f : expanded) {
            Col c = new Col();
            c.expr = f.expr;
            String base = f.alias != null ? f.alias : defaultName(f.expr);
            String name = base;
            if (f.alias == null && used.contains(name)) {
                int n = dup.getOrDefault(base, 0);
                do {
                    n++;
                    name = base + "_" + n;
                } while (used.contains(name));
                dup.put(base, n);
            }
            used.add(name);
            c.name = name;
            c.raw = !containsCall(f.expr, fn -> AGG.contains(fn) || TRANSFORMS.contains(fn));
            an.cols.add(c);
        }
        int aggCalls = 0;
        int selectorCalls = 0;
        int nonSelectorAgg = 0;
        boolean rawVar = false;
        Set<String> distinctCalls = new LinkedHashSet<>();
        for (Col c : an.cols) {
            if (containsCall(c.expr, TRANSFORMS::contains)) {
                an.hasTransform = true;
            }
            if (containsCall(c.expr, AGG::contains)) {
                an.aggregate = true;
                collectCalls(c.expr, distinctCalls);
                if (c.expr instanceof Bin || c.expr instanceof Paren) {
                    if (hasBareVar(c.expr)) {
                        throw new StmtError("mixing aggregate and non-aggregate queries is not supported");
                    }
                }
            } else if (c.raw && !(c.expr instanceof Call cc && cc.name.equals("now"))) {
                rawVar |= hasVar(c.expr);
            }
        }
        for (String call : distinctCalls) {
            aggCalls++;
            String fn = call.substring(0, call.indexOf('('));
            if (SELECTORS.contains(fn)) {
                selectorCalls++;
            } else {
                nonSelectorAgg++;
            }
        }
        // distinct() / top() / bottom() / sample() have special combination rules
        for (Col c : an.cols) {
            if (c.expr instanceof Call cc && cc.name.equals("distinct") && an.cols.size() > 1) {
                throw new StmtError("aggregate function distinct() cannot be combined with other functions or fields");
            }
            if (c.expr instanceof Call cc && (cc.name.equals("top") || cc.name.equals("bottom") || cc.name.equals("sample"))) {
                for (Col o : an.cols) {
                    if (o != c && containsCall(o.expr, fn -> AGG.contains(fn) || TRANSFORMS.contains(fn))) {
                        throw new StmtError("selector function " + cc.name + "() cannot be combined with other functions");
                    }
                }
            }
        }
        if (an.aggregate && rawVar) {
            if (nonSelectorAgg > 0) {
                throw new StmtError("mixing aggregate and non-aggregate queries is not supported");
            }
            if (selectorCalls > 1) {
                throw new StmtError("mixing multiple selector functions with tags or fields is not supported");
            }
        }
        if (!an.aggregate && an.hasTransform && rawVar) {
            throw new StmtError("mixing aggregate and non-aggregate queries is not supported");
        }
        an.hasRawRef = an.aggregate && rawVar;
        an.singleSelector = selectorCalls == 1 && aggCalls == 1;
        if (groupByTime && !an.aggregate) {
            throw new StmtError("GROUP BY requires at least one aggregate function");
        }
        return an;
    }

    private static void collectCalls(Expr e, Set<String> out) {
        if (e instanceof Call c) {
            if (AGG.contains(c.name)) {
                out.add(c.name + "(" + c.args.stream().map(InfluxSelect::exprString).collect(java.util.stream.Collectors.joining(",")) + ")");
            }
            for (Expr a : c.args) {
                collectCalls(a, out);
            }
        } else if (e instanceof Bin b) {
            collectCalls(b.l, out);
            collectCalls(b.r, out);
        } else if (e instanceof Paren p) {
            collectCalls(p.e, out);
        }
    }

    private static void validateExpr(Expr e, int depth) {
        if (e instanceof Call c) {
            String fn = c.name;
            int[] ar = arity(fn);
            if (ar == null) {
                if (fn.equals("holt_winters") || fn.equals("holt_winters_with_fit")) {
                    throw new StmtError("holt_winters is not supported");
                }
                throw new StmtError("undefined function " + fn + "()");
            }
            int n = c.args.size();
            if (fn.equals("distinct") && n > 1) {
                throw new StmtError("distinct function can only have one argument");
            }
            if (n < ar[0] || n > ar[1]) {
                if (ar[0] == ar[1]) {
                    throw new StmtError("invalid number of arguments for " + fn + ", expected " + ar[0] + ", got " + n);
                }
                if (ar[1] == Integer.MAX_VALUE) {
                    throw new StmtError("invalid number of arguments for " + fn + ", expected at least " + ar[0] + ", got " + n);
                }
                throw new StmtError("invalid number of arguments for " + fn + ", expected at least " + ar[0] + " but no more than " + ar[1]
                        + ", got " + n);
            }
            for (Expr a : c.args) {
                if (a instanceof Call || a instanceof Bin || a instanceof Paren) {
                    validateExpr(a, depth + 1);
                }
            }
            if (AGG.contains(fn) && !c.args.isEmpty() && !(c.args.get(0) instanceof VarRef) && !(c.args.get(0) instanceof Wildcard)
                    && !(c.args.get(0) instanceof RegexLit)) {
                boolean countDistinct = fn.equals("count") && c.args.get(0) instanceof Call ic && ic.name.equals("distinct");
                if (!countDistinct) {
                    throw new StmtError("expected field argument in " + fn + "()");
                }
            }
            if (fn.equals("distinct") && n > 1) {
                throw new StmtError("distinct function can only have one argument");
            }
        } else if (e instanceof Bin b) {
            validateExpr(b.l, depth);
            validateExpr(b.r, depth);
        } else if (e instanceof Paren p) {
            validateExpr(p.e, depth);
        }
    }

    private static String defaultName(Expr e) {
        if (e instanceof VarRef v) {
            return v.name;
        }
        if (e instanceof Call c) {
            return c.name;
        }
        if (e instanceof Paren p) {
            return defaultName(p.e);
        }
        if (e instanceof Bin b) {
            List<String> parts = new ArrayList<>();
            binNames(b, parts);
            return String.join("_", parts);
        }
        if (e instanceof Wildcard) {
            return "*";
        }
        return "";
    }

    private static void binNames(Expr e, List<String> parts) {
        if (e instanceof Bin b) {
            binNames(b.l, parts);
            binNames(b.r, parts);
        } else if (e instanceof Paren p) {
            binNames(p.e, parts);
        } else if (e instanceof VarRef v) {
            parts.add(v.name);
        } else if (e instanceof Call c) {
            parts.add(c.name);
        }
    }

    private static boolean wildcardAllows(String fn, String type) {
        return switch (fn) {
            case "count", "first", "last", "mode", "elapsed" -> true;
            case "min", "max" -> type.equals("float") || type.equals("integer") || type.equals("boolean");
            default -> type.equals("float") || type.equals("integer");
        };
    }

    private static void expandField(Field f, Schema union, List<String> dimNames, List<Field> out, Select s) {
        Expr e = f.expr;
        if (e instanceof Wildcard w) {
            for (String[] nt : allNames(union, dimNames, w.type, null)) {
                out.add(new Field(new VarRef(nt[0], nt[1]), nt[2]));
            }
            return;
        }
        if (e instanceof RegexLit r) {
            for (String[] nt : allNames(union, dimNames, "", r.pattern)) {
                out.add(new Field(new VarRef(nt[0], nt[1]), nt[2]));
            }
            return;
        }
        if (e instanceof Call c && !c.args.isEmpty() && (c.args.get(0) instanceof Wildcard || c.args.get(0) instanceof RegexLit)) {
            if (c.name.equals("distinct")) {
                throw new StmtError("expected field argument in distinct()");
            }
            java.util.regex.Pattern pat = c.args.get(0) instanceof RegexLit rl ? rl.pattern : null;
            for (var fe : new TreeMap<>(union.fields).entrySet()) {
                if (pat != null && !pat.matcher(fe.getKey()).find()) {
                    continue;
                }
                if (!wildcardAllows(c.name, fe.getValue())) {
                    continue;
                }
                List<Expr> args = new ArrayList<>(c.args);
                args.set(0, new VarRef(fe.getKey(), ""));
                out.add(new Field(new Call(c.name, args), f.alias != null ? f.alias : c.name + "_" + fe.getKey()));
            }
            return;
        }
        out.add(f);
        if (e instanceof Call c && (c.name.equals("top") || c.name.equals("bottom")) && c.args.size() > 2) {
            for (int i = 1; i < c.args.size() - 1; i++) {
                if (c.args.get(i) instanceof VarRef tv) {
                    out.add(new Field(new VarRef(tv.name, ""), null));
                }
            }
        }
    }

    /** {name, cast type, output name} of every field and tag (tags clashing with a field become name_1). */
    private static List<String[]> allNames(Schema union, List<String> dimNames, String type, java.util.regex.Pattern re) {
        List<String[]> out = new ArrayList<>();
        TreeMap<String, String[]> sorted = new TreeMap<>();
        if (!type.equals("tag")) {
            for (String f : union.fields.keySet()) {
                sorted.put(f, new String[] {f, "field", f});
            }
        }
        if (!type.equals("field")) {
            for (String t : union.tagKeys) {
                if (dimNames.contains(t)) {
                    continue;
                }
                if (union.fields.containsKey(t) && !type.equals("tag")) {
                    sorted.put(t + "_1", new String[] {t, "tag", t + "_1"});
                } else {
                    sorted.put(t, new String[] {t, "tag", t});
                }
            }
        }
        for (var e : sorted.entrySet()) {
            if (re != null && !re.matcher(e.getValue()[0]).find()) {
                continue;
            }
            String[] v = e.getValue();
            out.add(new String[] {v[0], v[1].equals("field") ? "" : v[1], v[2]});
        }
        return out;
    }

    // ------------------------------------------------------------------ per-dataset execution

    private static List<Series> runDataset(DataSet ds, Select s, Analysis an, TimeRange tr, Expr residual, Dim timeDim,
            List<String> dimNames, Ctx ctx) {
        // filter
        List<Pt> pts = new ArrayList<>(ds.pts.size());
        Env env = new Env(ds.schema, ctx.now);
        for (Pt p : ds.pts) {
            if (tr.hasLo && p.time < tr.lo) {
                continue;
            }
            if (tr.hasHi && p.time > tr.hi) {
                continue;
            }
            if (timeDim != null && !tr.hasHi && p.time > ctx.now) {
                continue;
            }
            if (residual != null && !truthy(eval(residual, p, env, true))) {
                continue;
            }
            pts.add(p);
        }
        if (pts.isEmpty()) {
            return List.of();
        }
        // group
        TreeMap<String, List<Pt>> groups = new TreeMap<>();
        Map<String, Map<String, String>> groupTags = new HashMap<>();
        for (Pt p : pts) {
            StringBuilder key = new StringBuilder();
            Map<String, String> tags = new TreeMap<>();
            for (String d : dimNames) {
                String v = p.tags.getOrDefault(d, "");
                tags.put(d, v);
                key.append(d).append('=').append(v).append('\u0000');
            }
            String k = key.toString();
            groups.computeIfAbsent(k, x -> new ArrayList<>()).add(p);
            groupTags.putIfAbsent(k, tags);
        }
        List<String> columns = new ArrayList<>();
        columns.add(an.timeAlias != null ? an.timeAlias : "time");
        if (an.cols.isEmpty()) {
            return List.of();
        }
        for (Col c : an.cols) {
            columns.add(c.name);
        }
        List<Series> out = new ArrayList<>();
        long firstT = pts.get(0).time;
        List<Map.Entry<String, List<Pt>>> ordered = new ArrayList<>(groups.entrySet());
        if (s.desc) {
            java.util.Collections.reverse(ordered);
        }
        for (var g : ordered) {
            List<Map<String, String>> rowTags = null;
            List<Object[]> rows;
            if (an.aggregate) {
                rows = aggregateRows(g.getValue(), s, an, tr, timeDim, env, ctx, firstT);
            } else if (an.hasTransform) {
                rows = transformRows(g.getValue(), s, an, env, ctx);
            } else {
                rowTags = new ArrayList<>();
                rows = rawRows(g.getValue(), an, env, rowTags);
            }
            if (rows.isEmpty()) {
                continue;
            }
            if (s.desc) {
                java.util.Collections.reverse(rows);
                if (rowTags != null) {
                    java.util.Collections.reverse(rowTags);
                }
            }
            if (s.offset > 0 || (s.hasLimit && s.limit > 0)) {
                int from = Math.min(s.offset, rows.size());
                int limit = s.hasLimit ? (s.limit > 0 ? s.limit : Integer.MAX_VALUE) : 1; // OFFSET alone returns one row (InfluxDB quirk)
                int to = (int) Math.min((long) rows.size(), (long) from + limit);
                rows = new ArrayList<>(rows.subList(from, to));
                if (rowTags != null) {
                    rowTags = new ArrayList<>(rowTags.subList(from, to));
                }
                if (rows.isEmpty()) {
                    continue;
                }
            }
            Series ser = new Series(ds.name, dimNames.isEmpty() ? null : groupTags.get(g.getKey()), columns);
            ser.values = rows;
            ser.rowTags = rowTags;
            out.add(ser);
        }
        return out;
    }

    // ------------------------------------------------------------------ evaluation environment

    static final class Env {
        final Schema schema;
        final long now;

        Env(Schema schema, long now) {
            this.schema = schema;
            this.now = now;
        }
    }

    private static boolean truthy(Object o) {
        return o instanceof Boolean b && b;
    }

    /** Value of a variable at a point. {@code where}: missing tags read as "" (InfluxQL tag semantics). */
    private static Object varValue(VarRef v, Pt p, Env env, boolean where) {
        if (v.name.equalsIgnoreCase("time") && v.type.isEmpty() && !env.schema.fields.containsKey("time")) {
            return null;
        }
        String type = v.type;
        boolean isField = env.schema.fields.containsKey(v.name);
        if (type.equals("tag") || (!type.equals("field") && !isField && (env.schema.tagKeys.contains(v.name) || !isField))) {
            if (type.isEmpty() || type.equals("tag")) {
                if (isField && type.isEmpty()) {
                    return fieldOf(v, p);
                }
                String t = p.tags.get(v.name);
                if (t == null) {
                    return where ? "" : null;
                }
                return t;
            }
        }
        return fieldOf(v, p);
    }

    private static Object fieldOf(VarRef v, Pt p) {
        Object x = p.fields.get(v.name);
        if (x == null) {
            return null;
        }
        switch (v.type) {
            case "float":
                return x instanceof Number nn ? (Object) nn.doubleValue() : null;
            case "integer":
                return x instanceof Long ? x : (x instanceof Double dd ? (Object) (long) (double) dd : null);
            case "string":
                return x instanceof String ? x : null;
            case "boolean":
                return x instanceof Boolean ? x : null;
            default:
                return x;
        }
    }

    static Object eval(Expr e, Pt p, Env env, boolean where) {
        if (e instanceof Paren pp) {
            return eval(pp.e, p, env, where);
        }
        if (e instanceof VarRef v) {
            return varValue(v, p, env, where);
        }
        if (e instanceof StrLit s) {
            return s.v;
        }
        if (e instanceof IntLit i) {
            return i.v;
        }
        if (e instanceof NumLit n) {
            return n.v;
        }
        if (e instanceof BoolLit b) {
            return b.v;
        }
        if (e instanceof DurLit d) {
            return d.nanos;
        }
        if (e instanceof TimeLit) {
            return env.now;
        }
        if (e instanceof Call c) {
            if (MATH.contains(c.name)) {
                List<Object> args = new ArrayList<>();
                for (Expr a : c.args) {
                    args.add(eval(a, p, env, where));
                }
                return applyMath(c.name, args);
            }
            return null;
        }
        Bin b = (Bin) e;
        if (b.op.equals("AND") || b.op.equals("OR")) {
            boolean l = truthy(eval(b.l, p, env, where));
            if (b.op.equals("AND")) {
                return l && truthy(eval(b.r, p, env, where));
            }
            return l || truthy(eval(b.r, p, env, where));
        }
        if (isCmp(b.op)) {
            Expr le = unparen(b.l);
            Expr re = unparen(b.r);
            String op = b.op;
            if (!(le instanceof VarRef || le instanceof Bin || le instanceof Call) && (re instanceof VarRef || re instanceof Bin)) {
                Expr t = le;
                le = re;
                re = t;
                op = flip(op);
            }
            if (re instanceof RegexLit rx) {
                Object lv = eval(le, p, env, where);
                if (!(lv instanceof String ls)) {
                    return false;
                }
                boolean m = rx.pattern.matcher(ls).find();
                return op.equals("=~") == m;
            }
            if (isTimeVar(le)) {
                Long tv = timeValueOf(re, env.now);
                if (tv != null) {
                    return cmp(op, Long.compare(p.time, tv));
                }
            }
            Object lv = eval(le, p, env, where);
            Object rv = eval(re, p, env, where);
            return compare(op, lv, rv);
        }
        Object l = eval(b.l, p, env, where);
        Object r = eval(b.r, p, env, where);
        return arith(b.op, l, r);
    }

    private static Boolean cmp(String op, int c) {
        return switch (op) {
            case "=" -> c == 0;
            case "!=" -> c != 0;
            case "<" -> c < 0;
            case "<=" -> c <= 0;
            case ">" -> c > 0;
            case ">=" -> c >= 0;
            default -> false;
        };
    }

    private static Object compare(String op, Object l, Object r) {
        if (l == null || r == null) {
            return false;
        }
        if (l instanceof Number a && r instanceof Number b) {
            if (a instanceof Long x && b instanceof Long y) {
                return cmp(op, Long.compare(x, y));
            }
            return cmp(op, Double.compare(a.doubleValue(), b.doubleValue()));
        }
        if (l instanceof String a && r instanceof String b) {
            if (op.equals("=") || op.equals("!=")) {
                return cmp(op, a.compareTo(b));
            }
            return false; // InfluxQL does not order strings/tags
        }
        if (l instanceof Boolean a && r instanceof Boolean b) {
            if (op.equals("=")) {
                return a.equals(b);
            }
            if (op.equals("!=")) {
                return !a.equals(b);
            }
        }
        return false;
    }

    static Object arith(String op, Object l, Object r) {
        if (l == null || r == null) {
            return null;
        }
        if (!(l instanceof Number a) || !(r instanceof Number b)) {
            return null;
        }
        if (a instanceof Long x && b instanceof Long y) {
            switch (op) {
                case "+":
                    return x + y;
                case "-":
                    return x - y;
                case "*":
                    return x * y;
                case "/":
                    return y == 0 ? (Object) 0.0 : (Object) ((double) x / (double) y);
                case "%":
                    return y == 0 ? (Object) 0L : (Object) (x % y);
                case "&":
                    return x & y;
                case "|":
                    return x | y;
                case "^":
                    return x ^ y;
                default:
                    return null;
            }
        }
        double x = a.doubleValue();
        double y = b.doubleValue();
        switch (op) {
            case "+":
                return x + y;
            case "-":
                return x - y;
            case "*":
                return x * y;
            case "/":
                return y == 0 ? 0.0 : x / y;
            case "%":
                return y == 0 ? 0.0 : x % y;
            default:
                return null;
        }
    }

    static Object applyMath(String fn, List<Object> args) {
        for (Object a : args) {
            if (!(a instanceof Number)) {
                return null;
            }
        }
        double x = ((Number) args.get(0)).doubleValue();
        boolean isInt = args.get(0) instanceof Long;
        switch (fn) {
            case "abs":
                return isInt ? (Object) Math.abs((Long) args.get(0)) : (Object) Math.abs(x);
            case "ceil":
                return isInt ? args.get(0) : (Object) Math.ceil(x);
            case "floor":
                return isInt ? args.get(0) : (Object) Math.floor(x);
            case "round":
                return isInt ? args.get(0) : (Object) (double) Math.round(x < 0 ? -Math.round(-x) - 0.0 : x) ;
            case "acos":
                return StrictMath.acos(x);
            case "asin":
                return StrictMath.asin(x);
            case "atan":
                return StrictMath.atan(x);
            case "atan2":
                return StrictMath.atan2(x, ((Number) args.get(1)).doubleValue());
            case "cos":
                return StrictMath.cos(x);
            case "cot":
                return 1.0 / Math.tan(x);
            case "exp":
                return StrictMath.exp(x);
            case "ln":
                return StrictMath.log(x);
            case "log":
                return StrictMath.log(x) / StrictMath.log(((Number) args.get(1)).doubleValue());
            case "log10":
                return StrictMath.log10(x);
            case "log2":
                return StrictMath.log(x) / StrictMath.log(2);
            case "pow":
                return StrictMath.pow(x, ((Number) args.get(1)).doubleValue());
            case "sin":
                return StrictMath.sin(x);
            case "sqrt":
                return Math.sqrt(x);
            case "tan":
                return StrictMath.tan(x);
            default:
                return null;
        }
    }

    // ------------------------------------------------------------------ raw rows

    private static List<Object[]> rawRows(List<Pt> pts, Analysis an, Env env, List<Map<String, String>> tagsOut) {
        List<Object[]> rows = new ArrayList<>(pts.size());
        int n = an.cols.size();
        for (Pt p : pts) {
            Object[] row = new Object[n + 1];
            row[0] = new TimeV(p.time);
            boolean any = false;
            for (int i = 0; i < n; i++) {
                Col c = an.cols.get(i);
                Object v = evalRowExpr(c.expr, p, env);
                row[i + 1] = normalise(v);
                if (refPresent(c.expr, p, env)) {
                    any = true;
                }
            }
            if (any) {
                rows.add(row);
                tagsOut.add(p.tags);
            }
        }
        return rows;
    }

    private static Object normalise(Object v) {
        if (v instanceof Double d && d.isNaN()) {
            return null;
        }
        return v;
    }

    private static Object evalRowExpr(Expr e, Pt p, Env env) {
        checkTypes(e, env);
        return eval(e, p, env, false);
    }

    private static boolean refPresent(Expr e, Pt p, Env env) {
        if (e instanceof VarRef v) {
            boolean isField = v.type.equals("field") || (!v.type.equals("tag") && env.schema.fields.containsKey(v.name))
                    || (!v.type.isEmpty() && !v.type.equals("tag"));
            return isField && fieldOf(v, p) != null;
        }
        if (e instanceof Paren pp) {
            return refPresent(pp.e, p, env);
        }
        if (e instanceof Bin b) {
            return refPresent(b.l, p, env) || refPresent(b.r, p, env);
        }
        if (e instanceof Call c) {
            for (Expr a : c.args) {
                if (refPresent(a, p, env)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Static type check of arithmetic (e.g. {@code usage + msg}) using the measurement schema. */
    private static void checkTypes(Expr e, Env env) {
        if (e instanceof Bin b && !isCmp(b.op) && !b.op.equals("AND") && !b.op.equals("OR")) {
            checkTypes(b.l, env);
            checkTypes(b.r, env);
            String lt = staticType(b.l, env);
            String rt = staticType(b.r, env);
            if (lt != null && rt != null) {
                boolean ln = lt.equals("float") || lt.equals("integer");
                boolean rn = rt.equals("float") || rt.equals("integer");
                boolean bitOp = b.op.equals("&") || b.op.equals("|") || b.op.equals("^");
                boolean bad = !ln || !rn || (bitOp && (lt.equals("float") || rt.equals("float")));
                if (bad) {
                    throw new StmtError("type error: " + typedString(b.l, env) + " " + b.op + " " + typedString(b.r, env)
                            + ": incompatible types: " + lt + " and " + rt);
                }
            }
        } else if (e instanceof Paren p) {
            checkTypes(p.e, env);
        } else if (e instanceof Call c) {
            for (Expr a : c.args) {
                checkTypes(a, env);
            }
        }
    }

    private static String staticType(Expr e, Env env) {
        e = unparen(e);
        if (e instanceof VarRef v) {
            if (!v.type.isEmpty() && !v.type.equals("field") && !v.type.equals("tag")) {
                return v.type;
            }
            if (v.type.equals("tag")) {
                return "tag";
            }
            String t = env.schema.fields.get(v.name);
            if (t != null) {
                return t;
            }
            if (env.schema.tagKeys.contains(v.name)) {
                return "tag";
            }
            return null;
        }
        if (e instanceof IntLit) {
            return "integer";
        }
        if (e instanceof NumLit) {
            return "float";
        }
        if (e instanceof StrLit) {
            return "string";
        }
        if (e instanceof BoolLit) {
            return "boolean";
        }
        if (e instanceof Bin b) {
            String l = staticType(b.l, env);
            String r = staticType(b.r, env);
            if (l == null || r == null) {
                return null;
            }
            if (b.op.equals("/")) {
                return "float";
            }
            return l.equals("integer") && r.equals("integer") ? "integer" : "float";
        }
        return null;
    }

    private static String typedString(Expr e, Env env) {
        if (e instanceof VarRef v) {
            String t = staticType(v, env);
            return InfluxQl.quoteIdent(v.name) + "::" + (t == null ? "float" : t);
        }
        if (e instanceof Paren p) {
            return "(" + typedString(p.e, env) + ")";
        }
        if (e instanceof Bin b) {
            return typedString(b.l, env) + " " + b.op + " " + typedString(b.r, env);
        }
        return exprString(e);
    }

    // ------------------------------------------------------------------ transformations over raw points

    private static List<Object[]> transformRows(List<Pt> pts, Select s, Analysis an, Env env, Ctx ctx) {
        int n = an.cols.size();
        List<Pt> ordered = pts;
        if (s.desc) {
            ordered = new ArrayList<>(pts);
            java.util.Collections.reverse(ordered);
        }
        List<List<Cell>> cols = new ArrayList<>();
        for (Col c : an.cols) {
            cols.add(evalSeries(c.expr, ordered, env, 1e9));
        }
        return assemble(cols, n, false, null, false);
    }

    /** Cells of an expression over raw points (transformations and math over per-point values), in the given order. */
    private static List<Cell> evalSeries(Expr e, List<Pt> pts, Env env, double defaultUnit) {
        e = unparen(e);
        if (e instanceof Call c && TRANSFORMS.contains(c.name)) {
            List<Cell> in = evalSeries(c.args.get(0), pts, env, defaultUnit);
            return transform(c, in, defaultUnit);
        }
        List<Cell> out = new ArrayList<>();
        for (Pt p : pts) {
            Object v = normalise(eval(e, p, env, false));
            if (refPresent(e, p, env)) {
                out.add(new Cell(p.time, v, p));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ aggregate rows

    private static List<Object[]> aggregateRows(List<Pt> pts, Select s, Analysis an, TimeRange tr, Dim timeDim, Env env, Ctx ctx,
            long firstT) {
        AggState st = new AggState();
        st.pts = pts;
        st.env = env;
        st.timeDim = timeDim;
        st.an = an;
        st.fill = s.fill;
        st.fillValue = s.fillValue;
        st.desc = s.desc;
        st.selectorTime = timeDim == null && an.singleSelector;
        if (timeDim != null) {
            long interval = timeDim.interval;
            long off = timeDim.offsetNow ? Math.floorMod(ctx.now, interval) : timeDim.offset;
            off = Math.floorMod(off, interval);
            long lo = tr.hasLo ? tr.lo : firstT;
            long hi = tr.hasHi ? tr.hi : ctx.now;
            if (hi < lo) {
                return new ArrayList<>();
            }
            long start = Math.floorDiv(lo - off, interval) * interval + off;
            long last = Math.floorDiv(hi - off, interval) * interval + off;
            long count = (last - start) / interval + 1;
            if (count > 5_000_000L) {
                throw new StmtError("too many points in the group by interval. maybe you forgot to specify a where time clause?");
            }
            st.starts = new long[(int) count];
            for (int i = 0; i < count; i++) {
                st.starts[i] = start + i * interval;
            }
            st.interval = interval;
            st.windowStartTime = start;
        } else {
            st.windowStartTime = tr.hasLo ? tr.lo : 0L;
        }
        int n = an.cols.size();
        List<List<Cell>> cols = new ArrayList<>();
        for (Col c : an.cols) {
            cols.add(evalAgg(c.expr, st, c));
        }
        if (an.hasRawRef) {
            // plain fields/tags next to a selector take their value from the selected point
            List<Cell> sel = null;
            for (int i = 0; i < n && sel == null; i++) {
                if (!an.cols.get(i).raw) {
                    sel = cols.get(i);
                }
            }
            for (int i = 0; i < n; i++) {
                if (an.cols.get(i).raw && sel != null) {
                    List<Cell> mapped = new ArrayList<>();
                    for (Cell sc : sel) {
                        mapped.add(new Cell(sc.t, sc.pt == null ? null : normalise(eval(an.cols.get(i).expr, sc.pt, env, false)), sc.pt));
                    }
                    cols.set(i, mapped);
                }
            }
        }
        boolean multiRow = false;
        for (Col c : an.cols) {
            multiRow |= containsCall(c.expr, fn -> fn.equals("distinct") || fn.equals("top") || fn.equals("bottom") || fn.equals("sample")
                    || fn.equals("integral"));
        }
        boolean keepEmptyRows = timeDim != null && !an.hasTransform && s.fill != FillKind.NONE && !multiRow;
        List<Object[]> rows = assemble(cols, n, keepEmptyRows, timeDim != null ? st.starts : null, an.hasRawRef);
        if (timeDim != null && s.fill == FillKind.NULL && !an.nested) {
            // an empty window reports count() = 0 (but only at the top level: a subquery sees no value there)
            for (int i = 0; i < n; i++) {
                if (an.cols.get(i).expr instanceof Call cc && cc.name.equals("count")) {
                    for (Object[] r : rows) {
                        if (r[i + 1] == null) {
                            r[i + 1] = 0L;
                        }
                    }
                }
            }
        }
        return rows;
    }

    static final class AggState {
        List<Pt> pts;
        Env env;
        Dim timeDim;
        Analysis an;
        FillKind fill;
        Object fillValue;
        boolean desc;
        boolean selectorTime;
        long[] starts;
        long interval;
        long windowStartTime;
    }

    private static List<Cell> evalAgg(Expr e, AggState st, Col col) {
        e = unparen(e);
        if (e instanceof Call c) {
            if (AGG.contains(c.name)) {
                boolean multi = c.name.equals("distinct") || c.name.equals("top") || c.name.equals("bottom") || c.name.equals("sample")
                        || c.name.equals("integral");
                return multi ? aggCall(c, st) : densify(aggCall(c, st), st, c.name.equals("count"));
            }
            if (TRANSFORMS.contains(c.name)) {
                Expr a0 = unparen(c.args.get(0));
                boolean overAgg = containsCall(a0, fn -> AGG.contains(fn) || TRANSFORMS.contains(fn));
                List<Cell> in;
                if (overAgg) {
                    in = evalAgg(a0, st, col);
                    if (st.timeDim != null && st.fill == FillKind.NUMBER && st.starts.length > 0) {
                        in = new ArrayList<>(in);
                        in.add(0, new Cell(st.starts[0] - st.interval, st.fillValue, null));
                    }
                } else {
                    in = evalSeries(a0, st.pts, st.env, 1e9);
                }
                if (st.desc) {
                    in = new ArrayList<>(in);
                    java.util.Collections.reverse(in);
                }
                double defUnit = overAgg && st.timeDim != null ? st.interval : 1e9;
                List<Cell> tr = transform(c, in, defUnit);
                if (overAgg && st.timeDim != null && st.fill == FillKind.NUMBER && st.starts.length > 0) {
                    long first = st.starts[0];
                    tr.removeIf(x -> x.t < first);
                }
                return tr;
            }
            if (MATH.contains(c.name)) {
                List<Cell> a = evalAgg(c.args.get(0), st, col);
                List<Cell> out = new ArrayList<>();
                for (Cell x : a) {
                    List<Object> args = new ArrayList<>();
                    args.add(x.v);
                    for (int i = 1; i < c.args.size(); i++) {
                        args.add(literal(c.args.get(i)));
                    }
                    out.add(new Cell(x.t, x.v == null ? null : applyMath(c.name, args), x.pt));
                }
                return out;
            }
        }
        if (e instanceof Bin b) {
            Object ll = literal(b.l);
            Object rl = literal(b.r);
            if (ll != null && rl != null) {
                return List.of();
            }
            if (rl != null) {
                List<Cell> a = evalAgg(b.l, st, col);
                List<Cell> out = new ArrayList<>();
                for (Cell x : a) {
                    out.add(new Cell(x.t, arith(b.op, x.v, rl), x.pt));
                }
                return out;
            }
            if (ll != null) {
                List<Cell> a = evalAgg(b.r, st, col);
                List<Cell> out = new ArrayList<>();
                for (Cell x : a) {
                    out.add(new Cell(x.t, arith(b.op, ll, x.v), x.pt));
                }
                return out;
            }
            List<Cell> a = evalAgg(b.l, st, col);
            List<Cell> c2 = evalAgg(b.r, st, col);
            Map<Long, Cell> byT = new HashMap<>();
            for (Cell x : c2) {
                byT.putIfAbsent(x.t, x);
            }
            List<Cell> out = new ArrayList<>();
            for (Cell x : a) {
                Cell y = byT.get(x.t);
                out.add(new Cell(x.t, y == null ? null : arith(b.op, x.v, y.v), x.pt));
            }
            return out;
        }
        return List.of(); // plain variables next to a selector are resolved during assembly
    }

    private static Object literal(Expr e) {
        e = unparen(e);
        if (e instanceof IntLit i) {
            return i.v;
        }
        if (e instanceof NumLit n) {
            return n.v;
        }
        if (e instanceof DurLit d) {
            return d.nanos;
        }
        if (e instanceof StrLit s) {
            return s.v;
        }
        if (e instanceof BoolLit b) {
            return b.v;
        }
        return null;
    }

    /** Field samples (value present) of the argument, in time order. */
    private static List<Sample> samples(Expr arg, AggState st, String fn) {
        arg = unparen(arg);
        List<Sample> out = new ArrayList<>();
        if (arg instanceof VarRef v) {
            String type = st.env.schema.fields.get(v.name);
            if (type == null && st.env.schema.tagKeys.contains(v.name) || v.type.equals("tag")) {
                return out; // tags carry no field values: aggregates over them yield nothing (as in InfluxDB)
            }
            for (Pt p : st.pts) {
                Object x = fieldOf(v, p);
                if (x != null) {
                    out.add(new Sample(p.time, x, p));
                }
            }
            return out;
        }
        for (Pt p : st.pts) {
            Object x = normalise(eval(arg, p, st.env, false));
            if (x != null && refPresent(arg, p, st.env)) {
                out.add(new Sample(p.time, x, p));
            }
        }
        return out;
    }

    /** Per-window results of an aggregate/selector call (sparse: windows without data yield no cell). */
    private static List<Cell> aggCall(Call c, AggState st) {
        String fn = c.name;
        Expr arg = c.args.get(0);
        Expr inner = unparen(arg);
        if (inner instanceof Call ic && fn.equals("count") && ic.name.equals("distinct")) {
            List<Cell> distinct = aggCall(ic, st);
            Map<Long, Long> perWindow = new TreeMap<>();
            for (Cell d : distinct) {
                perWindow.merge(d.t, 1L, Long::sum);
            }
            List<Cell> out = new ArrayList<>();
            perWindow.forEach((t, n) -> out.add(new Cell(t, n, null)));
            return out;
        }
        List<Sample> all = samples(arg, st, fn);
        List<Cell> out = new ArrayList<>();
        if (st.timeDim == null) {
            out.addAll(reduce(fn, c.args, all, st.windowStartTime, st.selectorTime, st));
            return out;
        }
        int idx = 0;
        for (long ws : st.starts) {
            long we = ws + st.interval - 1;
            int from = idx;
            while (idx < all.size() && all.get(idx).t <= we) {
                idx++;
            }
            if (from == idx) {
                continue;
            }
            List<Sample> xs = all.subList(from, idx);
            if (fn.equals("integral")) {
                requireNumeric(fn, xs);
                double unit = c.args.size() > 1 && c.args.get(1) instanceof DurLit d ? d.nanos : 1e9;
                out.add(new Cell(ws, integrateWindow(all, ws, ws + st.interval, unit), null));
                continue;
            }
            out.addAll(reduce(fn, c.args, xs, ws, false, st));
        }
        return out;
    }

    /** Trapezoid integral of the sample series. */
    private static double integrate(List<Sample> xs, double unit) {
        double area = 0;
        for (int i = 1; i < xs.size(); i++) {
            area += (num(xs.get(i).v) + num(xs.get(i - 1).v)) / 2.0 * ((xs.get(i).t - xs.get(i - 1).t) / unit);
        }
        return area;
    }

    /** Integral over [from, to] of the piecewise-linear curve through {@code all}, interpolating at both edges. */
    private static double integrateWindow(List<Sample> all, long from, long to, double unit) {
        long start = Math.max(from, all.get(0).t);
        long end = Math.min(to, all.get(all.size() - 1).t);
        if (end <= start) {
            return 0;
        }
        double area = 0;
        for (int i = 1; i < all.size(); i++) {
            Sample a = all.get(i - 1);
            Sample b = all.get(i);
            long lo = Math.max(a.t, start);
            long hi = Math.min(b.t, end);
            if (hi <= lo) {
                continue;
            }
            double va = num(a.v);
            double vb = num(b.v);
            double span = b.t - a.t;
            double y0 = va + (vb - va) * ((lo - a.t) / span);
            double y1 = va + (vb - va) * ((hi - a.t) / span);
            area += (y0 + y1) / 2.0 * ((hi - lo) / unit);
        }
        return area;
    }

    private static double num(Object o) {
        return ((Number) o).doubleValue();
    }

    private static String goIter(Object v) {
        return v instanceof String ? "*query.stringInterruptIterator" : v instanceof Boolean ? "*query.booleanInterruptIterator"
                : v instanceof Long ? "*query.integerInterruptIterator" : "*query.floatInterruptIterator";
    }

    private static void requireNumeric(String fn, List<Sample> xs) {
        for (Sample s : xs) {
            if (!(s.v instanceof Number)) {
                throw new StmtError("unsupported " + fn + " iterator type: " + goIter(s.v));
            }
        }
    }

    private static double boolNum(Object o) {
        return o instanceof Boolean b ? (b ? 1 : 0) : ((Number) o).doubleValue();
    }

    private static List<Cell> reduce(String fn, List<Expr> args, List<Sample> xs, long wt, boolean selectorTime, AggState st) {
        List<Cell> out = new ArrayList<>();
        if (xs.isEmpty()) {
            return out;
        }
        int n = xs.size();
        switch (fn) {
            case "count" -> out.add(new Cell(wt, (long) n, null));
            case "sum" -> {
                requireNumeric(fn, xs);
                boolean allInt = xs.stream().allMatch(x -> x.v instanceof Long);
                if (allInt) {
                    long t = 0;
                    for (Sample x : xs) {
                        t += (Long) x.v;
                    }
                    out.add(new Cell(wt, t, null));
                } else {
                    double t = 0;
                    for (Sample x : xs) {
                        t += num(x.v);
                    }
                    out.add(new Cell(wt, t, null));
                }
            }
            case "mean" -> {
                requireNumeric(fn, xs);
                double t = 0;
                for (Sample x : xs) {
                    t += num(x.v);
                }
                out.add(new Cell(wt, t / n, null));
            }
            case "median" -> {
                requireNumeric(fn, xs);
                double[] v = xs.stream().mapToDouble(x -> num(x.v)).sorted().toArray();
                out.add(new Cell(wt, n % 2 == 0 ? (v[n / 2 - 1] + v[n / 2]) / 2.0 : v[n / 2], null));
            }
            case "mode" -> {
                if (n == 1) {
                    out.add(new Cell(wt, xs.get(0).v, null));
                    break;
                }
                List<Sample> sorted = new ArrayList<>(xs);
                sorted.sort((p1, p2) -> compareVals(p1.v, p2.v));
                int mostFreq = 0;
                int currFreq = 0;
                Object currMode = sorted.get(0).v;
                Object mostMode = sorted.get(0).v;
                long mostTime = sorted.get(0).t;
                long currTime = sorted.get(0).t;
                for (Sample p : sorted) {
                    if (compareVals(p.v, currMode) != 0) {
                        currFreq = 1;
                        currMode = p.v;
                        currTime = p.t;
                        continue;
                    }
                    currFreq++;
                    if (mostFreq > currFreq || (currFreq == mostFreq && currTime > mostTime)) {
                        continue;
                    }
                    mostFreq = currFreq;
                    mostMode = p.v;
                    mostTime = p.t;
                }
                out.add(new Cell(wt, mostMode, null));
            }
            case "spread" -> {
                requireNumeric(fn, xs);
                boolean allInt = xs.stream().allMatch(x -> x.v instanceof Long);
                double mn = Double.MAX_VALUE;
                double mx = -Double.MAX_VALUE;
                for (Sample x : xs) {
                    mn = Math.min(mn, num(x.v));
                    mx = Math.max(mx, num(x.v));
                }
                out.add(new Cell(wt, allInt ? (Object) (long) (mx - mn) : (Object) (mx - mn), null));
            }
            case "stddev" -> {
                requireNumeric(fn, xs);
                if (n < 2) {
                    out.add(new Cell(wt, null, null));
                    break;
                }
                double m = 0;
                for (Sample x : xs) {
                    m += num(x.v);
                }
                m /= n;
                double ss = 0;
                for (Sample x : xs) {
                    ss += (num(x.v) - m) * (num(x.v) - m);
                }
                out.add(new Cell(wt, Math.sqrt(ss / (n - 1)), null));
            }
            case "integral" -> {
                requireNumeric(fn, xs);
                double unit = 1e9;
                if (args.size() > 1) {
                    if (args.get(1) instanceof DurLit d) {
                        unit = d.nanos;
                    } else {
                        throw new StmtError("second argument must be a duration");
                    }
                }
                out.add(new Cell(wt, integrate(xs, unit), null));
            }
            case "first" -> {
                Sample b = xs.get(0);
                for (Sample x : xs) {
                    if (x.t < b.t) {
                        b = x;
                    }
                }
                out.add(new Cell(selectorTime ? b.t : wt, b.v, b.pt));
            }
            case "last" -> {
                Sample b = xs.get(0);
                for (Sample x : xs) {
                    if (x.t >= b.t) {
                        b = x;
                    }
                }
                out.add(new Cell(selectorTime ? b.t : wt, b.v, b.pt));
            }
            case "min", "max" -> {
                for (Sample x : xs) {
                    if (!(x.v instanceof Number) && !(x.v instanceof Boolean)) {
                        throw new StmtError("unsupported " + fn + " iterator type: " + goIter(x.v));
                    }
                }
                Sample b = xs.get(0);
                for (Sample x : xs) {
                    int c = Double.compare(boolNum(x.v), boolNum(b.v));
                    if (fn.equals("min") ? c < 0 : c > 0) {
                        b = x;
                    }
                }
                out.add(new Cell(selectorTime ? b.t : wt, b.v, b.pt));
            }
            case "percentile" -> {
                requireNumeric(fn, xs);
                Object pArg = literal(args.get(1));
                if (!(pArg instanceof Number pn)) {
                    throw new StmtError("expected float argument in percentile()");
                }
                double p = pn.doubleValue();
                List<Sample> sorted = new ArrayList<>(xs);
                sorted.sort((a, b) -> Double.compare(num(a.v), num(b.v)));
                int idx = (int) Math.floor(n * p / 100.0 + 0.5) - 1;
                if (idx < 0 || idx >= n) {
                    break;
                }
                Sample b = sorted.get(idx);
                out.add(new Cell(selectorTime ? b.t : wt, b.v, b.pt));
            }
            case "distinct" -> {
                java.util.LinkedHashSet<Object> set = new java.util.LinkedHashSet<>();
                for (Sample x : xs) {
                    set.add(x.v);
                }
                for (Object v : set) {
                    out.add(new Cell(wt, v, null));
                }
            }
            case "top", "bottom" -> {
                requireNumeric(fn, xs);
                Object nArg = literal(args.get(args.size() - 1));
                if (!(nArg instanceof Long nn)) {
                    throw new StmtError("expected integer argument as last arg in " + fn + "()");
                }
                if (nn < 1) {
                    throw new StmtError("limit (" + nn + ") in " + fn + " function must be at least 1");
                }
                List<String> tagArgs = new ArrayList<>();
                for (int i = 1; i < args.size() - 1; i++) {
                    if (args.get(i) instanceof VarRef tv) {
                        tagArgs.add(tv.name);
                    } else {
                        throw new StmtError("expected tag argument in " + fn + "()");
                    }
                }
                List<Sample> cand = new ArrayList<>(xs);
                Comparator<Sample> byVal = (a, b) -> {
                    int c = Double.compare(num(a.v), num(b.v));
                    if (fn.equals("top")) {
                        c = -c;
                    }
                    return c != 0 ? c : Long.compare(a.t, b.t);
                };
                cand.sort(byVal);
                List<Sample> pick = new ArrayList<>();
                if (tagArgs.isEmpty()) {
                    pick.addAll(cand.subList(0, (int) Math.min(nn, cand.size())));
                } else {
                    Set<String> seen = new java.util.HashSet<>();
                    for (Sample x : cand) {
                        StringBuilder k = new StringBuilder();
                        for (String t : tagArgs) {
                            k.append(x.pt.tags.getOrDefault(t, "")).append('\u0000');
                        }
                        if (seen.add(k.toString()) && pick.size() < nn) {
                            pick.add(x);
                        }
                    }
                }
                pick.sort((a, b) -> Long.compare(a.t, b.t));
                for (Sample x : pick) {
                    out.add(new Cell(x.t, x.v, x.pt));
                }
            }
            case "sample" -> {
                Object nArg = literal(args.get(1));
                if (!(nArg instanceof Long nn) || nn < 1) {
                    throw new StmtError("expected integer argument in sample()");
                }
                for (Sample x : new ArrayList<>(xs.subList(0, (int) Math.min(nn, xs.size())))) {
                    out.add(new Cell(x.t, x.v, x.pt));
                }
            }
            default -> throw new StmtError("undefined function " + fn + "()");
        }
        return out;
    }

    static int compareVals(Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            return Double.compare(x.doubleValue(), y.doubleValue());
        }
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return Boolean.compare(x, y);
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    // ------------------------------------------------------------------ fill

    private static List<Cell> densify(List<Cell> cells, AggState st, boolean isCount) {
        if (st.timeDim == null) {
            return cells;
        }
        Map<Long, Cell> byT = new HashMap<>();
        for (Cell c : cells) {
            byT.putIfAbsent(c.t, c);
        }
        int n = st.starts.length;
        List<Cell> dense = new ArrayList<>(n);
        boolean desc = st.desc;
        Object prev = null;
        int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = desc ? n - 1 - i : i;
        }
        Object[] vals = new Object[n];
        boolean[] present = new boolean[n];
        for (int k = 0; k < n; k++) {
            int i = order[k];
            Cell c = byT.get(st.starts[i]);
            if (c != null) {
                vals[i] = c.v;
                present[i] = true;
                prev = c.v;
            } else {
                switch (st.fill) {
                    case NULL -> vals[i] = null;
                    case NONE -> vals[i] = null;
                    case PREVIOUS -> vals[i] = prev;
                    case NUMBER -> vals[i] = st.fillValue;
                    case LINEAR -> vals[i] = null;
                }
            }
        }
        if (st.fill == FillKind.LINEAR) {
            for (int k = 0; k < n; k++) {
                int i = order[k];
                if (present[i]) {
                    continue;
                }
                int pk = k - 1;
                while (pk >= 0 && !present[order[pk]]) {
                    pk--;
                }
                int nk = k + 1;
                while (nk < n && !present[order[nk]]) {
                    nk++;
                }
                if (pk >= 0 && nk < n) {
                    int pi = order[pk];
                    int ni = order[nk];
                    Object a = vals[pi];
                    Object b = vals[ni];
                    if (a instanceof Number an && b instanceof Number bn) {
                        double t0 = st.starts[pi];
                        double t1 = st.starts[ni];
                        double t = st.starts[i];
                        vals[i] = an.doubleValue() + (bn.doubleValue() - an.doubleValue()) * ((t - t0) / (t1 - t0));
                    }
                }
            }
        }
        for (int i = 0; i < n; i++) {
            if (st.fill == FillKind.NONE && !present[i]) {
                continue;
            }
            Cell c = byT.get(st.starts[i]);
            dense.add(new Cell(st.starts[i], vals[i], c == null ? null : c.pt));
        }
        return dense;
    }

    // ------------------------------------------------------------------ transformations

    /** {@code in} is in iteration order (descending for ORDER BY time DESC), as InfluxDB computes them. */
    private static List<Cell> transform(Call c, List<Cell> in, double defaultUnit) {
        String fn = c.name;
        List<Cell> nn = new ArrayList<>();
        for (Cell x : in) {
            if (x.v != null) {
                nn.add(x);
            }
        }
        List<Cell> out = new ArrayList<>();
        switch (fn) {
            case "derivative", "non_negative_derivative" -> {
                double unit = defaultUnit;
                if (c.args.size() > 1) {
                    if (c.args.get(1) instanceof DurLit d) {
                        unit = d.nanos;
                    } else {
                        throw new StmtError("second argument to " + fn + " must be a duration, got " + goType(c.args.get(1)));
                    }
                    if (unit <= 0) {
                        throw new StmtError("duration argument must be positive, got " + InfluxFmt.goDuration((long) unit));
                    }
                }
                for (int i = 1; i < nn.size(); i++) {
                    Cell a = nn.get(i - 1);
                    Cell b = nn.get(i);
                    if (!(a.v instanceof Number) || !(b.v instanceof Number)) {
                        throw new StmtError("unsupported derivative iterator type: " + goIter(b.v));
                    }
                    double dt = Math.abs(b.t - a.t) / unit;
                    if (dt == 0) {
                        continue;
                    }
                    double d = (num(b.v) - num(a.v)) / dt;
                    if (fn.startsWith("non_negative") && d < 0) {
                        continue;
                    }
                    out.add(new Cell(b.t, d, null));
                }
            }
            case "difference", "non_negative_difference" -> {
                for (int i = 1; i < nn.size(); i++) {
                    Cell a = nn.get(i - 1);
                    Cell b = nn.get(i);
                    if (!(a.v instanceof Number) || !(b.v instanceof Number)) {
                        throw new StmtError("unsupported difference iterator type: " + goIter(b.v));
                    }
                    Object d = a.v instanceof Long x && b.v instanceof Long y ? (Object) (y - x) : (Object) (num(b.v) - num(a.v));
                    if (fn.startsWith("non_negative") && num(d) < 0) {
                        continue;
                    }
                    out.add(new Cell(b.t, d, null));
                }
            }
            case "elapsed" -> {
                long unit = 1;
                if (c.args.size() > 1) {
                    if (c.args.get(1) instanceof DurLit d) {
                        unit = d.nanos;
                    } else {
                        throw new StmtError("second argument must be a duration");
                    }
                    if (unit <= 0) {
                        throw new StmtError("duration argument must be positive, got " + InfluxFmt.goDuration(unit));
                    }
                }
                for (int i = 1; i < nn.size(); i++) {
                    out.add(new Cell(nn.get(i).t, (nn.get(i).t - nn.get(i - 1).t) / unit, null));
                }
            }
            case "moving_average" -> {
                Object w = literal(c.args.get(1));
                if (!(w instanceof Long win)) {
                    throw new StmtError("second argument for moving_average must be an integer, got " + exprString(c.args.get(1)));
                }
                if (win <= 1) {
                    throw new StmtError("moving_average window must be greater than 1, got " + win);
                }
                for (int i = 0; i < nn.size(); i++) {
                    if (i + 1 < win) {
                        continue;
                    }
                    double t = 0;
                    for (int k = i - (int) (long) win + 1; k <= i; k++) {
                        if (!(nn.get(k).v instanceof Number)) {
                            throw new StmtError("unsupported moving average iterator type: " + goIter(nn.get(k).v));
                        }
                        t += num(nn.get(k).v);
                    }
                    out.add(new Cell(nn.get(i).t, t / win, null));
                }
            }
            case "cumulative_sum" -> {
                Object acc = null;
                for (Cell x : nn) {
                    if (!(x.v instanceof Number)) {
                        throw new StmtError("unsupported cumulative sum iterator type: " + goIter(x.v));
                    }
                    if (acc == null) {
                        acc = x.v;
                    } else if (acc instanceof Long a && x.v instanceof Long b) {
                        acc = a + b;
                    } else {
                        acc = num(acc) + num(x.v);
                    }
                    out.add(new Cell(x.t, acc, null));
                }
            }
            default -> throw new StmtError("undefined function " + fn + "()");
        }
        return out;
    }

    // ------------------------------------------------------------------ row assembly

    /**
     * Joins per-column cells by time into rows. {@code passthrough}: raw variables next to a single selector take
     * their value from the selector's point.
     */
    private static List<Object[]> assemble(List<List<Cell>> cols, int n, boolean keepEmpty, long[] grid, boolean passthrough) {
        // row key: (time, k-th occurrence of that time within the column)
        TreeMap<Long, List<Object[]>> byTime = new TreeMap<>();
        Map<Long, List<Pt>> refPts = new HashMap<>();
        for (int ci = 0; ci < n; ci++) {
            Map<Long, Integer> seen = new HashMap<>();
            for (Cell c : cols.get(ci)) {
                int k = seen.merge(c.t, 1, Integer::sum) - 1;
                List<Object[]> rows = byTime.computeIfAbsent(c.t, x -> new ArrayList<>());
                while (rows.size() <= k) {
                    Object[] r = new Object[n + 1];
                    r[0] = new TimeV(c.t);
                    rows.add(r);
                }
                rows.get(k)[ci + 1] = normalise(c.v);
                if (c.pt != null) {
                    List<Pt> rp = refPts.computeIfAbsent(c.t, x -> new ArrayList<>());
                    while (rp.size() <= k) {
                        rp.add(null);
                    }
                    if (rp.get(k) == null) {
                        rp.set(k, c.pt);
                    }
                }
            }
        }
        if (keepEmpty && grid != null) {
            for (long t : grid) {
                if (!byTime.containsKey(t)) {
                    Object[] r = new Object[n + 1];
                    r[0] = new TimeV(t);
                    List<Object[]> one = new ArrayList<>();
                    one.add(r);
                    byTime.put(t, one);
                }
            }
        }
        List<Object[]> out = new ArrayList<>();
        for (var e : byTime.entrySet()) {
            List<Pt> rp = refPts.get(e.getKey());
            for (int k = 0; k < e.getValue().size(); k++) {
                Object[] r = e.getValue().get(k);
                boolean any = false;
                for (int i = 1; i <= n; i++) {
                    if (r[i] != null) {
                        any = true;
                    }
                }
                out.add(r);
            }
        }
        return out;
    }

    /** Fills raw-variable columns (tags/fields next to a selector) from the selector's point; called by the engine. */
    static void fillPassthrough(List<Object[]> rows, List<List<Cell>> cols, List<Col> colDefs, Env env) {
        // (kept for API symmetry; passthrough columns are resolved in resolvePassthrough)
    }
}
