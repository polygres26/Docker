package com.sayonora.wire.cqlwire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.cqlwire.Ast.*;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** INSERT / UPDATE / DELETE / BATCH: compiled into per-partition mutations that run as one local transaction on the owning host. */
final class Dml {

    private final Engine e;
    private final CqlStore store;
    private final CqlShards shards;

    Dml(Engine e) {
        this.e = e;
        this.store = e.store;
        this.shards = e.shards;
    }

    @FunctionalInterface
    interface Action {
        void run(Connection c) throws SQLException;
    }

    /** A mutation of one partition. */
    static final class Mut {
        Schema.Table t;
        long token;
        byte[] pk;
        byte[] ck;
        String host;
        Action action;
        /** null, "NOT_EXISTS", "EXISTS" or "IF". */
        String lwt;
        List<Cond> conds = List.of();
        Eval.Binds binds;
        boolean counter;
        boolean insertRow;
    }

    // ------------------------------------------------------------------ table lookup

    Schema.Table table(String ks, String name, ClientState cs, boolean write) {
        String k = ks != null ? ks : cs.keyspace;
        if (k == null) {
            throw CqlError.invalid("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        }
        Schema s = e.catalog.schema();
        if (s.ks(k) == null) {
            throw CqlError.invalid("keyspace " + k + " does not exist");
        }
        Schema.Table t = s.table(k, name);
        if (t == null) {
            throw CqlError.invalid("table " + name + " does not exist");
        }
        return t;
    }

    // ------------------------------------------------------------------ key restrictions

    static final class Keys {
        List<Object[]> pks = new ArrayList<>();
        /** Full clustering keys (or prefixes when {@link #prefixLen} is smaller than the clustering size). */
        List<Object[]> cks = new ArrayList<>();
        int prefixLen;
        /** Optional slice on the clustering column after the prefix: {lo, loIncl, hi, hiIncl}. */
        Object[] slice;
    }

    private static List<List<Object>> product(List<List<Object>> lists) {
        List<List<Object>> out = new ArrayList<>();
        out.add(new ArrayList<>());
        for (List<Object> l : lists) {
            List<List<Object>> next = new ArrayList<>();
            for (List<Object> pre : out) {
                for (Object v : l) {
                    List<Object> n = new ArrayList<>(pre);
                    n.add(v);
                    next.add(n);
                }
            }
            out = next;
        }
        return out;
    }

    /** Distinct values in the type's order: Cassandra reads IN restrictions sorted, whatever order they are written in. */
    private static List<Object> dedupe(CqlType t, List<Object> vals) {
        TreeSet<Object> s = new TreeSet<>(t);
        s.addAll(vals);
        return new ArrayList<>(s);
    }

    private static void checkNoIn(List<Relation> where, Schema.Table t) {
        for (Relation r : where) {
            if (r.op().equals("in") && !r.token()) {
                Schema.Column c = t.col(r.cols().get(0));
                if (c != null && c.kind == Schema.Kind.CLUSTERING) {
                    throw CqlError.invalid("IN on the clustering key columns is not supported with conditional updates");
                }
                throw CqlError.invalid("IN on the partition key is not supported with conditional updates");
            }
        }
    }

    static String undefined(Schema.Table t, String col) {
        return "Undefined column name " + col + " in table " + Ddl.qual(t.ks, t.name);
    }

    /** Values of an EQ/IN relation for the given column(s) as a list of alternatives. */
    private List<Object> alternatives(Relation r, Schema.Column c, Eval.Binds b) {
        if (r.op().equals("=")) {
            Object v = Eval.coerce(r.rhs(), c.type, b, c.name);
            if (v == Eval.UNSET_VALUE) {
                throw CqlError.invalid("Invalid unset value for column " + c.name);
            }
            if (v == null) {
                throw CqlError.invalid("Invalid null value in condition for column " + c.name);
            }
            return new ArrayList<>(List.of(v));
        }
        List<Object> out = new ArrayList<>();
        if (r.inList() != null) {
            for (Term x : r.inList()) {
                Object v = Eval.coerce(x, c.type, b, c.name);
                if (v == null || v == Eval.UNSET_VALUE) {
                    throw CqlError.invalid("Invalid null value in condition for column " + c.name);
                }
                out.add(v);
            }
        } else {
            Object v = Eval.coerce(r.rhs(), CqlType.list(c.type, false), b, c.name);
            if (v == null || v == Eval.UNSET_VALUE) {
                throw CqlError.invalid("Invalid null value for IN restriction on " + c.name);
            }
            out.addAll((List<?>) v);
        }
        return out;
    }

    /**
     * Restrictions of an UPDATE / DELETE WHERE clause. {@code forDelete}: clustering may be a prefix or carry a slice (range delete).
     */
    Keys keys(Schema.Table t, List<Relation> where, Eval.Binds b, boolean forDelete, boolean needFullCk) {
        return keys(t, where, b, forDelete, needFullCk, null);
    }

    Keys keys(Schema.Table t, List<Relation> where, Eval.Binds b, boolean forDelete, boolean needFullCk, String verb) {
        List<String> nonKey = new ArrayList<>();
        Map<String, List<Object>> eq = new LinkedHashMap<>();
        Map<String, Relation> slices = new LinkedHashMap<>();
        for (Relation r : where) {
            if (r.token()) {
                throw CqlError.invalid("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements");
            }
            if (r.multi()) {
                // multi-column clustering relation: (c1, c2) = (?, ?) / IN ((..), (..))
                List<Schema.Column> cs = new ArrayList<>();
                for (String n : r.cols()) {
                    Schema.Column c = t.col(n);
                    if (c == null) {
                        throw CqlError.invalid(undefined(t, n));
                    }
                    if (c.kind != Schema.Kind.CLUSTERING) {
                        throw CqlError.invalid("Multi-column relations can only be applied to clustering columns but was applied to: " + n);
                    }
                    cs.add(c);
                }
                if (!r.op().equals("=") && !r.op().equals("in")) {
                    throw CqlError.invalid("Multi-column slice restrictions are not supported for UPDATE/DELETE");
                }
                CqlType tt = CqlType.tuple(cs.stream().map(c -> c.type).toList(), false);
                List<Object[]> tuples = new ArrayList<>();
                if (r.op().equals("=")) {
                    tuples.add((Object[]) Eval.coerce(r.rhs() instanceof TupleLit || r.rhs() instanceof Bind ? r.rhs() : new TupleLit(List.of(r.rhs())), tt, b, "tuple"));
                } else if (r.inList() != null) {
                    for (Term x : r.inList()) {
                        tuples.add((Object[]) Eval.coerce(x instanceof TupleLit || x instanceof Bind ? x : new TupleLit(List.of(x)), tt, b, "tuple"));
                    }
                } else {
                    for (Object o : (List<?>) Eval.coerce(r.rhs(), CqlType.list(tt, false), b, "tuple")) {
                        tuples.add((Object[]) o);
                    }
                }
                for (int i = 0; i < cs.size(); i++) {
                    final int ii = i;
                    List<Object> vals = new ArrayList<>();
                    for (Object[] tup : tuples) {
                        vals.add(tup[ii]);
                    }
                    if (eq.put(cs.get(i).name, vals) != null) {
                        throw CqlError.invalid(cs.get(i).name + " cannot be restricted by more than one relation if it includes an Equal");
                    }
                }
                continue;
            }
            Schema.Column c = t.col(r.cols().get(0));
            if (c == null) {
                throw CqlError.invalid(undefined(t, r.cols().get(0)));
            }
            if (!c.isKey()) {
                nonKey.add(c.name);
                continue;
            }
            if (c.kind == Schema.Kind.PARTITION_KEY) {
                if (!r.op().equals("=") && !r.op().equals("in")) {
                    throw CqlError.invalid("Only EQ and IN relation are supported on the partition key (unless you use the token() function)"
                            + (verb == null ? "" : " for " + verb + " statements"));
                }
                if (eq.put(c.name, alternatives(r, c, b)) != null) {
                    throw CqlError.invalid(c.name + " cannot be restricted by more than one relation if it includes an Equal");
                }
            } else if (r.op().equals("=") || r.op().equals("in")) {
                if (eq.put(c.name, alternatives(r, c, b)) != null || slices.containsKey(c.name)) {
                    throw CqlError.invalid(c.name + " cannot be restricted by more than one relation if it includes an Equal");
                }
            } else if (List.of("<", "<=", ">", ">=").contains(r.op())) {
                slices.put(c.name + "\u0000" + r.op(), r);
            } else {
                throw CqlError.invalid("Unsupported operator " + r.op() + " on column " + c.name);
            }
        }
        Keys k = new Keys();
        List<String> missing = new ArrayList<>();
        List<List<Object>> pkLists = new ArrayList<>();
        for (Schema.Column c : t.partition) {
            List<Object> v = eq.get(c.name);
            if (v == null) {
                missing.add(c.name);
            } else {
                pkLists.add(dedupe(c.type, v));
            }
        }
        if (!missing.isEmpty()) {
            throw CqlError.invalid("Some partition key parts are missing: " + String.join(",", missing));
        }
        if (!nonKey.isEmpty()) {
            throw CqlError.invalid("Non PRIMARY KEY columns found in where clause: " + String.join(", ", nonKey));
        }
        for (List<Object> l : product(pkLists)) {
            k.pks.add(l.toArray());
        }
        List<List<Object>> ckLists = new ArrayList<>();
        int n = 0;
        for (Schema.Column c : t.clustering) {
            List<Object> v = eq.get(c.name);
            if (v == null) {
                break;
            }
            ckLists.add(dedupe(c.type, v));
            n++;
        }
        final int nn = n;
        boolean sliceAtN = n < t.clustering.size() && slices.keySet().stream().anyMatch(kk -> kk.startsWith(t.clustering.get(nn).name + "\u0000"));
        for (Schema.Column c : sliceAtN ? List.<Schema.Column>of() : t.clustering.subList(n, t.clustering.size())) {
            if (eq.containsKey(c.name)) {
                throw CqlError.invalid("PRIMARY KEY column \"" + c.name + "\" cannot be restricted as preceding column \""
                        + t.clustering.get(n).name + "\" is not restricted");
            }
        }
        // a slice on a clustering column forbids restrictions on the columns after it
        for (var sl : slices.values()) {
            Schema.Column sc = t.col(sl.cols().get(0));
            for (Schema.Column c : t.clustering.subList(sc.position + 1, t.clustering.size())) {
                if (eq.containsKey(c.name) || slices.values().stream().anyMatch(x -> x.cols().get(0).equals(c.name))) {
                    throw CqlError.invalid("Clustering column \"" + c.name + "\" cannot be restricted (preceding column \"" + sc.name
                            + "\" is restricted by a non-EQ relation)");
                }
            }
        }
        k.prefixLen = n;
        if (!forDelete && !slices.isEmpty()) {
            throw CqlError.invalid("Slice restrictions are not supported on the clustering columns in UPDATE statements");
        }
        if (n < t.clustering.size()) {
            if (!forDelete && needFullCk) {
                List<String> miss = new ArrayList<>();
                for (Schema.Column c : t.clustering.subList(n, t.clustering.size())) {
                    miss.add(c.name);
                }
                throw CqlError.invalid("Some clustering keys are missing: " + String.join(", ", miss));
            }
            // slice on the next clustering column
            if (!slices.isEmpty()) {
                Schema.Column sc = t.clustering.get(n);
                Object lo = null, hi = null;
                boolean loIncl = true, hiIncl = true, hasLo = false, hasHi = false;
                for (Relation r : slices.values()) {
                    if (!r.cols().get(0).equals(sc.name)) {
                        throw CqlError.invalid("Clustering column \"" + r.cols().get(0) + "\" cannot be restricted (preceding column \""
                                + sc.name + "\" is restricted by a non-EQ relation)");
                    }
                    Object v = Eval.coerce(r.rhs(), sc.type, b, sc.name);
                    if (v == null || v == Eval.UNSET_VALUE) {
                        throw CqlError.invalid("Invalid null value for clustering key part " + sc.name);
                    }
                    if (r.op().startsWith(">")) {
                        lo = v;
                        loIncl = r.op().equals(">=");
                        hasLo = true;
                    } else {
                        hi = v;
                        hiIncl = r.op().equals("<=");
                        hasHi = true;
                    }
                }
                k.slice = new Object[] {hasLo ? lo : null, loIncl, hasHi ? hi : null, hiIncl};
            }
        } else if (!slices.isEmpty()) {
            throw CqlError.invalid("Invalid restrictions on clustering columns");
        }
        for (List<Object> l : product(ckLists)) {
            k.cks.add(l.toArray());
        }
        return k;
    }

    // ------------------------------------------------------------------ helpers

    private long timestampOf(Term ts, Eval.Binds b, QueryOpts o, long batchTs) {
        if (ts != null) {
            if (batchTs != 0) {
                throw CqlError.invalid("Timestamp must be set either on BATCH or individual statements");
            }
            Object v = Eval.coerce(ts, CqlType.BIGINT, b, "[timestamp]");
            if (v == null || v == Eval.UNSET_VALUE) {
                throw CqlError.invalid("Invalid null value of timestamp");
            }
            return (Long) v;
        }
        if (batchTs != 0) {
            return batchTs;
        }
        if (o.timestamp != Long.MIN_VALUE) {
            return o.timestamp;
        }
        return Eval.nowMicros();
    }

    private int ttlOf(Schema.Table t, Term ttl, Eval.Binds b) {
        long v = 0;
        if (ttl != null) {
            Object x = Eval.coerce(ttl, CqlType.INT, b, "[ttl]");
            if (x == null || x == Eval.UNSET_VALUE) {
                throw CqlError.invalid("Invalid null value of TTL");
            }
            v = (Integer) x;
            if (v < 0) {
                throw CqlError.invalid("A TTL must be greater or equal to 0, but was " + v);
            }
            if (v > 630720000) {
                throw CqlError.invalid("ttl is too large. requested (" + v + ") maximum (630720000)");
            }
        } else if (t.options.get("default_time_to_live") instanceof Number n) {
            v = n.longValue();
        }
        return (int) v;
    }

    private static void checkKey(Schema.Table t, Object[] pkv) {
        for (int i = 0; i < pkv.length; i++) {
            byte[] s = t.partition.get(i).type.serialize(pkv[i]);
            if (t.partition.size() == 1 && s.length == 0) {
                throw CqlError.invalid("Key may not be empty");
            }
            if (s.length > 65535) {
                throw CqlError.invalid("Key length of " + s.length + " is longer than maximum of 65535");
            }
        }
    }

    private Mut newMut(Schema.Table t, Object[] pkv, Eval.Binds b) {
        checkKey(t, pkv);
        Mut m = new Mut();
        m.t = t;
        m.pk = t.partitionBytes(pkv);
        m.token = Murmur3.token(m.pk);
        m.host = shards.owner(shards.hosts(), m.pk);
        m.binds = b;
        return m;
    }

    private static byte[] ckBytes(Schema.Table t, Object[] ckv) {
        return t.clustering.isEmpty() ? Rows.SINGLE_CK : t.clusteringBytes(ckv, t.clustering.size());
    }

    private static byte[] ckValues(Schema.Table t, Object[] ckv) {
        return t.clustering.isEmpty() ? new byte[0] : t.clusteringValues(ckv);
    }

    // ------------------------------------------------------------------ cell writers

    /** Tombstones live for gc_grace_seconds (10 days): the cell's expiry then sweeps them, whatever write timestamp they carry. */
    static final int GC_GRACE = 864000;

    private static final java.util.concurrent.atomic.AtomicInteger LIST_SEQ = new java.util.concurrent.atomic.AtomicInteger();
    /** 2010-01-01 in microseconds: Cassandra reverses the clock around it to place prepended list elements before appended ones. */
    private static final long LIST_REFERENCE_MICROS = 1_262_304_000_000L * 1000;

    /**
     * Position of a list element, ordered like Cassandra's time UUID cell path: appended elements sort by the server clock (NOT by the write
     * timestamp), prepended elements by the reversed clock, so a prepend always lands before every append; {@code i} keeps the elements of one
     * statement in order and a sequence number breaks ties between statements.
     */
    private static byte[] listPath(long nowMicros, int i, boolean prepend) {
        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.putLong((prepend ? 2 * LIST_REFERENCE_MICROS - nowMicros : nowMicros) * 10 + i);
        bb.putInt(LIST_SEQ.incrementAndGet());
        return bb.array();
    }

    /** Cells that set a whole column value (not null). */
    private List<CqlStore.Cell> valueCells(Schema.Column c, Object v, byte[] ck, byte[] ckv, long ts, int ttl) {
        List<CqlStore.Cell> out = new ArrayList<>();
        if (c.type.isMultiCell()) {
            CqlType et = c.type.args.get(0);
            switch (c.type.k) {
                case LIST -> {
                    int i = 0;
                    long now = Eval.nowMicros();
                    for (Object x : (List<?>) v) {
                        out.add(new CqlStore.Cell(ck, ckv, c.name, listPath(now, i++, false), et.serialize(x), ts, ttl));
                    }
                }
                case SET -> {
                    for (Object x : (java.util.Collection<?>) v) {
                        out.add(new CqlStore.Cell(ck, ckv, c.name, et.serialize(x), CqlType.EMPTY, ts, ttl));
                    }
                }
                default -> {
                    for (var en : ((Map<?, ?>) v).entrySet()) {
                        out.add(new CqlStore.Cell(ck, ckv, c.name, et.serialize(en.getKey()), c.type.args.get(1).serialize(en.getValue()), ts, ttl));
                    }
                }
            }
        } else {
            out.add(new CqlStore.Cell(ck, ckv, c.name, CqlType.EMPTY, c.type.serialize(v), ts, ttl));
        }
        return out;
    }

    private void writeValue(Connection c, Mut m, Schema.Column col, Object v, byte[] ck, byte[] ckv, long ts, int ttl, boolean explicit) throws SQLException {
        boolean emptyValue = v == null || col.type.isMultiCell() && ((v instanceof java.util.Collection<?> cc && cc.isEmpty()) || (v instanceof Map<?, ?> mm && mm.isEmpty()));
        if (col.type.isMultiCell()) {
            // overwriting a collection is a range tombstone just before the write (a delete: at the write): it shadows every older element
            long tomb = v == null ? ts : ts - 1;
            store.delete(c, m.t.id, m.token, m.pk, ck, ck.length == 0 ? new byte[] {0} : Rows.succ(ck), col.name, null, tomb);
            store.upsert(c, m.t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, ckv, col.name + Rows.CTOMB, CqlType.EMPTY, CqlType.EMPTY, tomb, GC_GRACE)));
        }
        if (emptyValue) {
            if (!col.type.isMultiCell()) {
                store.delete(c, m.t.id, m.token, m.pk, ck, ck.length == 0 ? new byte[] {0} : Rows.succ(ck), col.name, null, ts);
            }
            if (explicit && !col.type.isMultiCell()) {
                // with an explicit timestamp a delete must keep shadowing older writes: leave a cell tombstone
                store.upsert(c, m.t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, ckv, col.name, CqlType.EMPTY, null, ts, GC_GRACE)));
            }
            return;
        }
        store.upsert(c, m.t.id, m.token, m.pk, valueCells(col, v, ck, ckv, ts, ttl));
    }

    // ------------------------------------------------------------------ INSERT

    List<Mut> insert(Insert st, Eval.Binds b, ClientState cs, QueryOpts o, long batchTs) {
        Schema.Table t = table(st.ks(), st.table(), cs, true);
        if (t.virtual != null) {
            return List.of();
        }
        if (t.counter) {
            throw CqlError.invalid("INSERT statements are not allowed on counter tables, use UPDATE instead");
        }
        List<String> names = st.cols();
        List<Object> vals = new ArrayList<>();
        if (st.json() != null) {
            String js = st.json();
            if (js.equals("\u0000bind")) {
                Object v = Eval.coerce(st.values().get(0), CqlType.TEXT, b, "[json]");
                js = (String) v;
            }
            JsonElement je;
            try {
                je = JsonParser.parseString(js);
            } catch (RuntimeException ex) {
                throw CqlError.invalid("Could not decode JSON string as a map: " + ex.getMessage());
            }
            if (!je.isJsonObject()) {
                throw CqlError.invalid("Could not decode JSON string as a map");
            }
            JsonObject jo = je.getAsJsonObject();
            names = new ArrayList<>();
            Map<String, JsonElement> lower = new LinkedHashMap<>();
            for (var en : jo.entrySet()) {
                String k = en.getKey();
                lower.put(k.startsWith("\"") ? k.substring(1, k.length() - 1) : k.toLowerCase(), en.getValue());
            }
            for (var en : lower.entrySet()) {
                Schema.Column c = t.col(en.getKey());
                if (c == null) {
                    throw CqlError.invalid("JSON values map contains unrecognized column: " + en.getKey());
                }
                names.add(c.name);
                try {
                    vals.add(Eval.fromJson(en.getValue(), c.type));
                } catch (CqlError ce) {
                    throw CqlError.invalid("Error decoding JSON value for " + c.name + ": " + ce.getMessage().replaceFirst("^Error decoding JSON value for type [^:]*: ", ""));
                }
            }
            if (st.defaultUnset()) {
                // omitted columns stay untouched
            } else {
                for (Schema.Column c : t.columns) {
                    if (!c.isKey() && !names.contains(c.name)) {
                        names.add(c.name);
                        vals.add(null);
                    }
                }
            }
        } else {
            Set<String> dups = new LinkedHashSet<>();
            for (String n : names) {
                if (!dups.add(n)) {
                    throw CqlError.invalid("The column names contains duplicates");
                }
            }
            for (int i = 0; i < names.size(); i++) {
                Schema.Column c = t.col(names.get(i));
                if (c == null) {
                    throw CqlError.invalid(undefined(t, names.get(i)));
                }
                vals.add(Eval.coerce(st.values().get(i), c.type, b, c.name));
            }
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String n : names) {
            if (t.col(n) == null) {
                throw CqlError.invalid(undefined(t, n));
            }
            if (!seen.add(n)) {
                throw CqlError.invalid("The column names contains duplicates");
            }
        }
        Object[] pkv = new Object[t.partition.size()];
        Object[] ckv = new Object[t.clustering.size()];
        boolean anyRegular = false, anyStatic = false, anyNonKey = false;
        List<Object[]> setCols = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            Schema.Column c = t.col(names.get(i));
            Object v = vals.get(i);
            if (c.kind == Schema.Kind.PARTITION_KEY) {
                if (v == null || v == Eval.UNSET_VALUE) {
                    throw CqlError.invalid(v == null ? "Invalid null value in condition for column " + c.name : "Invalid unset value for column " + c.name);
                }
                pkv[c.position] = v;
            } else if (c.kind == Schema.Kind.CLUSTERING) {
                if (v == null || v == Eval.UNSET_VALUE) {
                    throw CqlError.invalid(v == null ? "Invalid null value in condition for column " + c.name : "Invalid unset value for column " + c.name);
                }
                ckv[c.position] = v;
            } else {
                anyNonKey = true;
                if (c.kind == Schema.Kind.STATIC) {
                    anyStatic = true;
                } else {
                    anyRegular = true;
                }
                if (v != Eval.UNSET_VALUE) {
                    setCols.add(new Object[] {c, v});
                }
            }
        }
        List<String> missingPk = new ArrayList<>();
        for (Schema.Column c : t.partition) {
            if (pkv[c.position] == null) {
                missingPk.add(c.name);
            }
        }
        if (!missingPk.isEmpty()) {
            throw CqlError.invalid(st.json() != null ? "Invalid null value in condition for column " + missingPk.get(0)
                    : "Some partition key parts are missing: " + String.join(",", missingPk));
        }
        boolean staticOnly = anyStatic && !anyRegular && !t.clustering.isEmpty() && Arrays.stream(ckv).allMatch(x -> x == null);
        if (!staticOnly) {
            List<String> missingCk = new ArrayList<>();
            for (Schema.Column c : t.clustering) {
                if (ckv[c.position] == null) {
                    missingCk.add(c.name);
                }
            }
            if (!missingCk.isEmpty()) {
                throw CqlError.invalid(st.json() != null ? "Invalid null value in condition for column " + missingCk.get(0)
                        : "Some clustering keys are missing: " + String.join(", ", missingCk));
            }
        }
        if (st.ine() && staticOnly) {
            // IF NOT EXISTS on a static-only insert checks the static row
        }
        long ts = timestampOf(st.ts(), b, o, batchTs);
        final boolean explicit = true; // deletes always leave tombstones (the drivers send client timestamps anyway)
        int ttl = ttlOf(t, st.ttl(), b);
        if (st.ine() && st.ts() != null) {
            throw CqlError.invalid("Cannot provide custom timestamp for conditional updates");
        }
        Mut m = newMut(t, pkv, b);
        m.insertRow = !staticOnly;
        byte[] ck = staticOnly ? Rows.STATIC_CK : ckBytes(t, ckv);
        byte[] ckvb = staticOnly ? new byte[0] : ckValues(t, ckv);
        m.ck = ck;
        if (st.ine()) {
            m.lwt = "NOT_EXISTS";
        }
        final boolean marker = !staticOnly;
        m.action = c -> {
            if (marker) {
                store.upsert(c, t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, ckvb, "", CqlType.EMPTY, CqlType.EMPTY, ts, ttl)));
            }
            for (Object[] sv : setCols) {
                Schema.Column col = (Schema.Column) sv[0];
                boolean st2 = col.kind == Schema.Kind.STATIC;
                writeValue(c, m, col, sv[1], st2 ? Rows.STATIC_CK : ck, st2 ? new byte[0] : ckvb, ts, ttl, explicit);
            }
        };
        if (!anyNonKey && !marker) {
            throw CqlError.invalid("Missing mandatory PRIMARY KEY part");
        }
        return List.of(m);
    }

    // ------------------------------------------------------------------ UPDATE

    List<Mut> update(Update st, Eval.Binds b, ClientState cs, QueryOpts o, long batchTs) {
        Schema.Table t = table(st.ks(), st.table(), cs, true);
        if (t.virtual != null) {
            return List.of();
        }
        boolean anyStatic = false, anyRegular = false;
        for (Assign a : st.assigns()) {
            Schema.Column c = t.col(a.col());
            if (c == null) {
                throw CqlError.invalid(undefined(t, a.col()));
            }
            if (c.isKey()) {
                throw CqlError.invalid("PRIMARY KEY part " + c.name + " found in SET part");
            }
            if (c.kind == Schema.Kind.STATIC) {
                anyStatic = true;
            } else {
                anyRegular = true;
            }
            if (c.type.isCounter() != (a.arith() == '+' || a.arith() == '-')) {
                if (c.type.isCounter()) {
                    throw CqlError.invalid("Cannot set the value of counter column " + c.name + " (counters can only be incremented/decremented, not set)");
                }
                if (!c.type.isCollection()) {
                    throw CqlError.invalid("Invalid operation (" + c.name + " = " + c.name + " " + a.arith() + " " + Select.describe(a.value())
                            + ") for non counter column " + c.name);
                }
            }
            if (c.type.isCollection() && c.type.frozen && (a.arith() == '+' || a.arith() == '-') || a.index() != null && c.type.frozen) {
                throw CqlError.invalid("Invalid operation (" + (a.index() != null ? c.name + "[" + Select.describe(a.index()) + "] = " + Select.describe(a.value())
                        : a.prepend() ? c.name + " = " + Select.describe(a.value()) + " + " + c.name
                        : c.name + " = " + c.name + " " + a.arith() + " " + Select.describe(a.value())) + ") for frozen collection column " + c.name);
            }
            if (c.type.isCounter() && st.ttl() != null) {
                throw CqlError.invalid("Cannot provide custom TTL for counter updates");
            }
        }
        boolean staticOnly = anyStatic && !anyRegular;
        Keys k = keys(t, st.where(), b, false, !staticOnly, "UPDATE");
        if (staticOnly && k.prefixLen > 0) {
            throw CqlError.invalid("Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns");
        }
        if (k.prefixLen < t.clustering.size() && !staticOnly) {
            throw CqlError.invalid("Some clustering keys are missing: " + t.clustering.get(k.prefixLen).name);
        }
        if (st.ifExists() || !st.conds().isEmpty()) {
            checkNoIn(st.where(), t);
            if (t.counter) {
                throw CqlError.invalid("Conditions on counters are not supported");
            }
        }
        if (t.counter && st.ts() != null) {
            throw CqlError.invalid("Cannot provide custom timestamp for counter updates");
        }
        long ts = timestampOf(st.ts(), b, o, batchTs);
        final boolean explicit = true; // deletes always leave tombstones (the drivers send client timestamps anyway)
        int ttl = ttlOf(t, st.ttl(), b);
        if ((st.ifExists() || !st.conds().isEmpty()) && st.ts() != null) {
            throw CqlError.invalid("Cannot provide custom timestamp for conditional updates");
        }
        // evaluate the assignment values once
        List<Object[]> ops = new ArrayList<>();
        for (Assign a : st.assigns()) {
            Schema.Column c = t.col(a.col());
            ops.add(new Object[] {c, a, evalAssign(c, a, b)});
        }
        List<Mut> muts = new ArrayList<>();
        List<Object[]> cks = staticOnly ? java.util.Collections.singletonList(new Object[0]) : k.cks;
        for (Object[] pkv : k.pks) {
            for (Object[] ckv : cks) {
                Mut m = newMut(t, pkv, b);
                byte[] ck = staticOnly ? Rows.STATIC_CK : ckBytes(t, ckv);
                byte[] ckvb = staticOnly ? new byte[0] : ckValues(t, ckv);
                m.ck = ck;
                m.counter = t.counter;
                if (st.ifExists()) {
                    m.lwt = "EXISTS";
                } else if (!st.conds().isEmpty()) {
                    m.lwt = "IF";
                    m.conds = st.conds();
                }
                m.action = c -> {
                    for (Object[] op : ops) {
                        Schema.Column col = (Schema.Column) op[0];
                        boolean isStatic = col.kind == Schema.Kind.STATIC;
                        applyAssign(c, m, col, (Assign) op[1], op[2], isStatic ? Rows.STATIC_CK : ck, isStatic ? new byte[0] : ckvb, ts, ttl, explicit);
                    }
                };
                muts.add(m);
            }
        }
        return muts;
    }

    /** Evaluated right hand side of an assignment; for index assignments {index, value}. */
    private Object evalAssign(Schema.Column c, Assign a, Eval.Binds b) {
        CqlType t = c.type;
        if (a.field() != null) {
            throw CqlError.invalid("Assignment to a field of the user defined type column " + c.name + " is not supported by Warp's cqlwire; "
                    + "replace the whole value");
        }
        if (a.index() != null) {
            if (t.k == CqlType.K.LIST) {
                Object i = Eval.coerce(a.index(), CqlType.INT, b, c.name);
                return new Object[] {i, Eval.coerce(a.value(), t.args.get(0), b, c.name)};
            }
            if (t.k == CqlType.K.MAP && t.isMultiCell()) {
                Object key = Eval.coerce(a.index(), t.args.get(0), b, c.name);
                return new Object[] {key, Eval.coerce(a.value(), t.args.get(1), b, c.name)};
            }
            throw CqlError.invalid("Invalid operation (" + c.name + "[...] = ...) for " + t.cql() + " column " + c.name);
        }
        if (t.isCounter()) {
            Object v = Eval.coerce(a.value(), CqlType.COUNTER, b, c.name);
            if (a.arith() == '=') {
                throw CqlError.invalid("Invalid operation for counter column " + c.name + ": only increments and decrements are supported");
            }
            if (v == null || v == Eval.UNSET_VALUE) {
                throw CqlError.invalid("Invalid null value for counter increment/decrement");
            }
            return v;
        }
        if (a.arith() == '-' && t.isCollection()) {
            // subtracting from a set removes the elements, from a map removes the keys, from a list removes the values
            CqlType st = t.k == CqlType.K.MAP ? CqlType.set(t.args.get(0), false) : t.k == CqlType.K.SET ? t : CqlType.list(t.args.get(0), false);
            return Eval.coerce(a.value(), st, b, c.name);
        }
        Object v = Eval.coerce(a.value(), t, b, c.name);
        return v;
    }

    private void applyAssign(Connection c, Mut m, Schema.Column col, Assign a, Object v, byte[] ck, byte[] ckv, long ts, int ttl, boolean explicit)
            throws SQLException {
        CqlType t = col.type;
        if (v == Eval.UNSET_VALUE) {
            return;
        }
        if (t.isCounter()) {
            long d = (Long) v;
            store.counterAdd(c, m.t.id, m.token, m.pk, ck, ckv, col.name, a.arith() == '-' ? -d : d, ts);
            return;
        }
        if (a.index() != null) {
            Object[] iv = (Object[]) v;
            if (t.k == CqlType.K.LIST) {
                int idx = (Integer) iv[0];
                store.lockPartition(c, m.t.id, m.pk);
                List<CqlStore.Rec> cells = listCells(c, m, col, ck);
                if (cells.isEmpty()) {
                    throw CqlError.invalid("Attempted to set an element on a list which is null");
                }
                if (idx < 0 || idx >= cells.size()) {
                    throw CqlError.invalid("List index " + idx + " out of bound, list has size " + cells.size());
                }
                byte[] path = cells.get(idx).path;
                if (iv[1] == null) {
                    elemTomb(c, m.t, m, col, ck, ckv, path, ts);
                } else {
                    store.upsert(c, m.t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, ckv, col.name, path, t.args.get(0).serialize(iv[1]), ts, ttl)));
                }
            } else {
                byte[] path = t.args.get(0).serialize(iv[0]);
                if (iv[1] == null) {
                    elemTomb(c, m.t, m, col, ck, ckv, path, ts);
                } else {
                    store.upsert(c, m.t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, ckv, col.name, path, t.args.get(1).serialize(iv[1]), ts, ttl)));
                }
            }
            return;
        }
        if (a.arith() == '+' && t.isMultiCell()) {
            if (v == null) {
                return;
            }
            List<CqlStore.Cell> cells;
            if (t.k == CqlType.K.LIST) {
                cells = new ArrayList<>();
                int i = 0;
                long now = Eval.nowMicros();
                for (Object x : (List<?>) v) {
                    cells.add(new CqlStore.Cell(ck, ckv, col.name, listPath(now, i++, a.prepend()), t.args.get(0).serialize(x), ts, ttl));
                }
            } else {
                cells = valueCells(col, v, ck, ckv, ts, ttl);
            }
            store.upsert(c, m.t.id, m.token, m.pk, cells);
            return;
        }
        if (a.arith() == '-' && t.isMultiCell()) {
            if (v == null) {
                return;
            }
            CqlType et = t.args.get(0);
            if (t.k == CqlType.K.LIST) {
                store.lockPartition(c, m.t.id, m.pk);
                List<CqlStore.Rec> cells = listCells(c, m, col, ck);
                for (CqlStore.Rec r : cells) {
                    Object ev = et.deserialize(r.val);
                    for (Object x : (List<?>) v) {
                        if (et.compare(ev, x) == 0) {
                            elemTomb(c, m.t, m, col, ck, ckv, r.path, ts);
                            break;
                        }
                    }
                }
            } else {
                for (Object x : (java.util.Collection<?>) v) {
                    elemTomb(c, m.t, m, col, ck, ckv, et.serialize(x), ts);
                }
            }
            return;
        }
        writeValue(c, m, col, v, ck, ckv, ts, ttl, explicit);
    }

    /** Deletes one collection element and leaves a tombstone so an older write cannot bring it back. */
    private void elemTomb(Connection c, Schema.Table t, Mut m, Schema.Column col, byte[] ck, byte[] ckv, byte[] path, long ts) throws SQLException {
        store.delete(c, t.id, m.token, m.pk, ck, ck.length == 0 ? new byte[] {0} : Rows.succ(ck), col.name, path, ts);
        store.upsert(c, t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, ckv, col.name, path, null, ts, GC_GRACE)));
    }

    private List<CqlStore.Rec> listCells(Connection c, Mut m, Schema.Column col, byte[] ck) throws SQLException {
        List<CqlStore.Rec> out = new ArrayList<>();
        for (CqlStore.Rec r : store.partition(c, m.t.id, m.token, m.pk, ck)) {
            if (r.col.equals(col.name)) {
                out.add(r);
            }
        }
        out.sort((x, y) -> Arrays.compareUnsigned(x.path, y.path));
        return out;
    }

    // ------------------------------------------------------------------ DELETE

    List<Mut> delete(Delete st, Eval.Binds b, ClientState cs, QueryOpts o, long batchTs) {
        Schema.Table t = table(st.ks(), st.table(), cs, true);
        if (t.virtual != null) {
            return List.of();
        }
        boolean anyStatic = false, anyRegular = false;
        for (DelTarget d : st.targets()) {
            Schema.Column c = t.col(d.col());
            if (c == null) {
                throw CqlError.invalid(undefined(t, d.col()));
            }
            if (c.isKey()) {
                throw CqlError.invalid("Invalid identifier " + c.name + " for deletion (should not be a PRIMARY KEY part)");
            }
            if (d.field() != null) {
                throw CqlError.invalid("Deletion of a user type field is not supported by Warp's cqlwire");
            }
            if (d.index() != null && !c.type.isMultiCell()) {
                throw CqlError.invalid("Invalid deletion operation for non collection column " + c.name);
            }
            if (c.kind == Schema.Kind.STATIC) {
                anyStatic = true;
            } else {
                anyRegular = true;
            }
        }
        boolean staticOnly = !st.targets().isEmpty() && anyStatic && !anyRegular;
        Keys k = keys(t, st.where(), b, true, !staticOnly && !st.targets().isEmpty(), "DELETE");
        if (!st.targets().isEmpty() && !staticOnly && (k.prefixLen < t.clustering.size() || k.slice != null)) {
            throw CqlError.invalid("Range deletions are not supported for specific columns");
        }
        boolean conditional = st.ifExists() || !st.conds().isEmpty();
        if (conditional && !staticOnly && (k.prefixLen < t.clustering.size() || k.slice != null)) {
            throw CqlError.invalid("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns");
        }
        if (conditional) {
            checkNoIn(st.where(), t);
        }
        long ts = timestampOf(st.ts(), b, o, batchTs);
        final boolean explicit = true; // deletes always leave tombstones (the drivers send client timestamps anyway)
        if ((st.ifExists() || !st.conds().isEmpty()) && st.ts() != null) {
            throw CqlError.invalid("Cannot provide custom timestamp for conditional updates");
        }
        List<Object[]> targets = new ArrayList<>();
        for (DelTarget d : st.targets()) {
            Schema.Column c = t.col(d.col());
            Object idx = null;
            if (d.index() != null) {
                idx = Eval.coerce(d.index(), c.type.k == CqlType.K.LIST ? CqlType.INT : c.type.args.get(0), b, c.name);
            }
            targets.add(new Object[] {c, idx});
        }
        List<Mut> muts = new ArrayList<>();
        List<Object[]> cks = staticOnly ? java.util.Collections.singletonList(new Object[0]) : k.cks.isEmpty() ? java.util.Collections.singletonList(new Object[0]) : k.cks;
        for (Object[] pkv : k.pks) {
            for (Object[] ckv : cks) {
                Mut m = newMut(t, pkv, b);
                boolean fullCk = ckv.length == t.clustering.size() && k.slice == null;
                byte[] prefix = staticOnly ? Rows.STATIC_CK : t.clustering.isEmpty() ? Rows.SINGLE_CK : t.clusteringBytes(ckv, ckv.length);
                m.ck = fullCk || staticOnly ? prefix : null;
                if (st.ifExists()) {
                    m.lwt = "EXISTS";
                } else if (!st.conds().isEmpty()) {
                    m.lwt = "IF";
                    m.conds = st.conds();
                }
                final Object[] sliceRef = k.slice;
                m.action = c -> {
                    if (targets.isEmpty()) {
                        byte[] lo = null, hi = null;
                        if (t.clustering.isEmpty()) {
                            store.delete(c, t.id, m.token, m.pk, null, null, null, null, ts);
                            if (explicit) {
                                store.upsert(c, t.id, m.token, m.pk, List.of(new CqlStore.Cell(Rows.SINGLE_CK, new byte[0], Rows.ROW_TOMB, CqlType.EMPTY,
                                        CqlType.EMPTY, ts, GC_GRACE)));
                            }
                            return;
                        }
                        if (sliceRef != null) {
                            Schema.Column sc = t.clustering.get(ckv.length);
                            lo = prefix;
                            hi = Rows.succ(prefix);
                            if (ckv.length == 0) {
                                lo = new byte[] {0};
                                hi = null;
                            }
                            // bounds in encoded space: descending columns swap the direction of the comparison
                            byte[] loKey = sliceRef[0] == null ? null : concat(prefix, sc.type.keyEncode(sliceRef[0], sc.desc));
                            byte[] hiKey = sliceRef[2] == null ? null : concat(prefix, sc.type.keyEncode(sliceRef[2], sc.desc));
                            boolean loIncl = (Boolean) sliceRef[1], hiIncl = (Boolean) sliceRef[3];
                            byte[] from, to;
                            if (!sc.desc) {
                                from = loKey == null ? lo : loIncl ? loKey : Rows.succ(loKey);
                                to = hiKey == null ? hi : hiIncl ? Rows.succ(hiKey) : hiKey;
                            } else {
                                from = hiKey == null ? lo : hiIncl ? hiKey : Rows.succ(hiKey);
                                to = loKey == null ? hi : loIncl ? Rows.succ(loKey) : loKey;
                            }
                            store.delete(c, t.id, m.token, m.pk, from, to, null, null, ts);
                            store.rangeTombstone(c, t.id, m.token, m.pk, from, to, ts);
                            return;
                        }
                        if (ckv.length == 0) {
                            store.delete(c, t.id, m.token, m.pk, null, null, null, null, ts);
                            store.rangeTombstone(c, t.id, m.token, m.pk, new byte[0], null, ts);
                        } else {
                            store.delete(c, t.id, m.token, m.pk, prefix, Rows.succ(prefix), null, null, ts);
                            if (!fullCk) {
                                store.rangeTombstone(c, t.id, m.token, m.pk, prefix, Rows.succ(prefix), ts);
                            }
                            if (explicit && fullCk) {
                                store.upsert(c, t.id, m.token, m.pk, List.of(new CqlStore.Cell(prefix, ckValues(t, ckv), Rows.ROW_TOMB, CqlType.EMPTY,
                                        CqlType.EMPTY, ts, GC_GRACE)));
                            }
                        }
                        return;
                    }
                    for (Object[] tg : targets) {
                        Schema.Column col = (Schema.Column) tg[0];
                        boolean isStatic = col.kind == Schema.Kind.STATIC;
                        byte[] ck = isStatic ? Rows.STATIC_CK : prefix;
                        byte[] hi = Rows.succ(ck);
                        if (isStatic) {
                            hi = new byte[] {0};
                        }
                        if (tg[1] == null) {
                            store.delete(c, t.id, m.token, m.pk, ck, hi, col.name, null, ts);
                            if (explicit && !col.type.isMultiCell()) {
                                store.upsert(c, t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, isStatic ? new byte[0] : ckValues(t, ckv), col.name,
                                        CqlType.EMPTY, null, ts, GC_GRACE)));
                            }
                            if (col.type.isMultiCell()) {
                                store.upsert(c, t.id, m.token, m.pk, List.of(new CqlStore.Cell(ck, isStatic ? new byte[0] : ckValues(t, ckv),
                                        col.name + Rows.CTOMB, CqlType.EMPTY, CqlType.EMPTY, ts, GC_GRACE)));
                            }
                        } else if (col.type.k == CqlType.K.LIST) {
                            store.lockPartition(c, t.id, m.pk);
                            List<CqlStore.Rec> cells = listCells(c, m, col, ck);
                            int idx = (Integer) tg[1];
                            if (idx < 0 || idx >= cells.size()) {
                                throw CqlError.invalid("List index " + idx + " out of bound, list has size " + cells.size());
                            }
                            elemTomb(c, t, m, col, ck, isStatic ? new byte[0] : ckValues(t, ckv), cells.get(idx).path, ts);
                        } else {
                            elemTomb(c, t, m, col, ck, isStatic ? new byte[0] : ckValues(t, ckv), col.type.args.get(0).serialize(tg[1]), ts);
                        }
                    }
                };
                muts.add(m);
            }
        }
        return muts;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    // ------------------------------------------------------------------ execution

    Result insertStmt(Insert st, Eval.Binds b, ClientState cs, QueryOpts o) {
        return run(insert(st, b, cs, o, 0), false, o);
    }

    Result updateStmt(Update st, Eval.Binds b, ClientState cs, QueryOpts o) {
        return run(update(st, b, cs, o, 0), false, o);
    }

    Result deleteStmt(Delete st, Eval.Binds b, ClientState cs, QueryOpts o) {
        return run(delete(st, b, cs, o, 0), false, o);
    }

    Result batch(Batch st, Eval.Binds b, ClientState cs, QueryOpts o) {
        return batch(st.kind(), st.ts(), st.stmts(), List.of(), b, cs, o);
    }

    /** {@code binds} per statement when executing a protocol BATCH; a text batch shares {@code common}. */
    Result batch(String kind, Term tsTerm, List<Stmt> stmts, List<Eval.Binds> binds, Eval.Binds common, ClientState cs, QueryOpts o) {
        o.batchTsExplicit = tsTerm != null || o.timestamp != Long.MIN_VALUE;
        long batchTs = 0;
        if (tsTerm != null) {
            Object v = Eval.coerce(tsTerm, CqlType.BIGINT, common, "[timestamp]");
            batchTs = (Long) v;
        } else if (o.timestamp != Long.MIN_VALUE) {
            batchTs = o.timestamp;
        } else {
            batchTs = Eval.nowMicros();
        }
        List<Mut> all = new ArrayList<>();
        boolean counters = false, nonCounters = false;
        for (int i = 0; i < stmts.size(); i++) {
            Stmt s = stmts.get(i);
            Eval.Binds b = binds.isEmpty() ? common : binds.get(i);
            long bts = batchTs;
            if (tsTerm != null && (s instanceof Insert i0 && i0.ts() != null || s instanceof Update u0 && u0.ts() != null
                    || s instanceof Delete d0 && d0.ts() != null)) {
                throw CqlError.invalid("Timestamp must be set either on BATCH or individual statements");
            }
            List<Mut> ms;
            if (s instanceof Insert in) {
                ms = insert(in, b, cs, o, in.ts() != null ? 0 : bts);
            } else if (s instanceof Update up) {
                ms = update(up, b, cs, o, up.ts() != null ? 0 : bts);
            } else if (s instanceof Delete de) {
                ms = delete(de, b, cs, o, de.ts() != null ? 0 : bts);
            } else {
                throw CqlError.invalid("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
            }
            for (Mut m : ms) {
                counters |= m.counter;
                nonCounters |= !m.counter;
            }
            all.addAll(ms);
        }
        if (kind.equals("COUNTER")) {
            if (nonCounters) {
                throw CqlError.invalid("Cannot include non-counter statement in a counter batch");
            }
        } else if (counters && kind.equals("LOGGED")) {
            throw CqlError.invalid("Cannot include a counter statement in a logged batch");
        }
        return run(all, kind.equals("UNLOGGED"), o);
    }

    private Result run(List<Mut> muts, boolean unlogged, QueryOpts o) {
        if (muts.isEmpty()) {
            return Result.VOID;
        }
        boolean lwt = muts.stream().anyMatch(m -> m.lwt != null);
        if (lwt) {
            return runConditional(muts);
        }
        // group by host, keep order; one transaction per host
        Map<String, List<Mut>> byHost = new LinkedHashMap<>();
        for (Mut m : muts) {
            byHost.computeIfAbsent(m.host, h -> new ArrayList<>()).add(m);
        }
        if (byHost.size() == 1 && muts.size() == 1) {
            Mut m = muts.get(0);
            e.timed(m.host, "write", () -> shards.tx(m.host, c -> {
                m.action.run(c);
                return null;
            }));
            return Result.VOID;
        }
        for (var en : byHost.entrySet()) {
            e.timed(en.getKey(), "write", () -> shards.tx(en.getKey(), c -> {
                for (Mut m : en.getValue()) {
                    m.action.run(c);
                }
                return null;
            }));
        }
        return Result.VOID;
    }

    // ------------------------------------------------------------------ lightweight transactions

    private Result runConditional(List<Mut> muts) {
        Mut first = muts.get(0);
        for (Mut m : muts) {
            if (!m.host.equals(first.host) || !Arrays.equals(m.pk, first.pk) || m.t != first.t && !m.t.id.equals(first.t.id)) {
                throw CqlError.invalid("Batch with conditions cannot span multiple partitions");
            }
        }
        Schema.Table t = first.t;
        Object[] out = e.timed(first.host, "cas", () -> shards.tx(first.host, c -> {
            store.lockPartition(c, t.id, first.pk);
            boolean applied = true;
            Rows.RowData failing = null;
            boolean failNoRow = false;
            List<String> reportCols = new ArrayList<>();
            boolean notExists = false;
            for (Mut m : muts) {
                if (m.lwt == null) {
                    continue;
                }
                Rows.RowData row = readRow(c, m);
                boolean ok;
                switch (m.lwt) {
                    case "NOT_EXISTS" -> {
                        ok = row == null;
                        notExists = true;
                    }
                    case "EXISTS" -> ok = row != null;
                    default -> {
                        ok = checkConds(t, row, m.conds, m.binds);
                        for (Cond cd : m.conds) {
                            if (!reportCols.contains(cd.col())) {
                                reportCols.add(cd.col());
                            }
                        }
                    }
                }
                if (!ok && applied) {
                    applied = false;
                    failing = row;
                    failNoRow = row == null;
                }
            }
            if (applied) {
                for (Mut m : muts) {
                    m.action.run(c);
                }
            }
            return new Object[] {applied, failing, failNoRow, reportCols, notExists};
        }));
        boolean applied = (Boolean) out[0];
        Rows.RowData failing = (Rows.RowData) out[1];
        @SuppressWarnings("unchecked")
        List<String> reportCols = (List<String>) out[3];
        boolean notExists = (Boolean) out[4];
        List<Result.ColSpec> cols = new ArrayList<>();
        cols.add(new Result.ColSpec(t.ks, t.name, "[applied]", CqlType.BOOLEAN));
        byte[][] row;
        if (applied) {
            row = new byte[][] {new byte[] {1}};
            return Result.rows(cols, java.util.Collections.singletonList(row), null);
        }
        List<Schema.Column> extra = new ArrayList<>();
        if (notExists && failing != null) {
            extra.addAll(t.selectOrder);
        } else if (!reportCols.isEmpty() && !muts.stream().anyMatch(m -> "NOT_EXISTS".equals(m.lwt))) {
            for (String n : reportCols) {
                extra.add(t.col(n));
            }
            extra.sort((x, y) -> x.kind == y.kind ? x.name.compareTo(y.name) : x.kind == Schema.Kind.STATIC ? 1 : -1);
        }
        if (failing == null) {
            extra.clear();
        }
        byte[][] r = new byte[1 + extra.size()][];
        r[0] = new byte[] {0};
        for (int i = 0; i < extra.size(); i++) {
            Schema.Column c = extra.get(i);
            cols.add(new Result.ColSpec(t.ks, t.name, c.name, c.type));
            r[i + 1] = failing == null ? null : c.type.serialize(failing.vals[c.ordinal]);
        }
        return Result.rows(cols, java.util.Collections.singletonList(r), null);
    }

    private Rows.RowData readRow(Connection c, Mut m) throws SQLException {
        Schema.Table t = m.t;
        byte[] ck = m.ck;
        if (ck == null) {
            throw CqlError.invalid("Conditional statements require the full primary key");
        }
        List<CqlStore.Rec> recs = store.partition(c, t.id, m.token, m.pk, ck);
        if (recs.isEmpty()) {
            return null;
        }
        List<CqlStore.Rec> statics = t.hasStatic && ck.length > 0 ? store.partition(c, t.id, m.token, m.pk, Rows.STATIC_CK) : List.of();
        List<Rows.RowData> rows = Rows.assemble(t, recs, statics, store.rangeTombstones(c, t.id, List.of(new long[] {m.token}), List.of(m.pk)));
        return rows.isEmpty() || rows.get(0).dead ? null : rows.get(0);
    }

    private boolean checkConds(Schema.Table t, Rows.RowData row, List<Cond> conds, Eval.Binds b) {
        for (Cond cd : conds) {
            Schema.Column col = t.col(cd.col());
            if (col == null) {
                throw CqlError.invalid(undefined(t, cd.col()));
            }
            if (col.isKey()) {
                throw CqlError.invalid("PRIMARY KEY column '" + col.name + "' cannot have IF conditions");
            }
            Object have = row == null ? null : row.vals[col.ordinal];
            CqlType type = col.type;
            if (cd.index() != null) {
                if (col.type.k == CqlType.K.LIST) {
                    Object i = Eval.coerce(cd.index(), CqlType.INT, b, col.name);
                    List<?> l = (List<?>) have;
                    have = l == null || (Integer) i < 0 || (Integer) i >= l.size() ? null : l.get((Integer) i);
                    type = col.type.args.get(0);
                } else if (col.type.k == CqlType.K.MAP) {
                    Object key = Eval.coerce(cd.index(), col.type.args.get(0), b, col.name);
                    have = have == null ? null : ((Map<?, ?>) have).get(key);
                    type = col.type.args.get(1);
                } else {
                    throw CqlError.invalid("Invalid element access for column " + col.name);
                }
            } else if (cd.field() != null) {
                int fi = col.type.k == CqlType.K.UDT ? col.type.fieldNames.indexOf(cd.field()) : -1;
                if (fi < 0) {
                    throw CqlError.invalid("Unknown field " + cd.field() + " for column " + col.name);
                }
                have = have == null ? null : ((Object[]) have)[fi];
                type = col.type.args.get(fi);
            }
            boolean ok;
            if (cd.op().equals("in")) {
                ok = false;
                List<Term> terms = cd.inList();
                if (terms == null) {
                    for (Object x : (List<?>) Eval.coerce(cd.rhs(), CqlType.list(type, false), b, col.name)) {
                        ok |= sameNullable(type, have, x);
                    }
                } else {
                    for (Term x : terms) {
                        ok |= sameNullable(type, have, Eval.coerce(x, type, b, col.name));
                    }
                }
            } else {
                Object want = Eval.coerce(cd.rhs(), type, b, col.name);
                ok = switch (cd.op()) {
                    case "=" -> sameNullable(type, have, want);
                    case "!=" -> !sameNullable(type, have, want);
                    default -> have != null && want != null && cmpOp(cd.op(), type.compare(have, want));
                };
            }
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    static boolean sameNullable(CqlType t, Object a, Object b) {
        if (a == null || b == null) {
            return a == null && b == null;
        }
        return t.compare(a, b) == 0;
    }

    static boolean cmpOp(String op, int c) {
        return switch (op) {
            case "<" -> c < 0;
            case "<=" -> c <= 0;
            case ">" -> c > 0;
            case ">=" -> c >= 0;
            default -> false;
        };
    }

}
