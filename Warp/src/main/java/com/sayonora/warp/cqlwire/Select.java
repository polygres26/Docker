package com.sayonora.warp.cqlwire;

import com.sayonora.warp.cqlwire.Ast.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;

/** SELECT: planning (with Cassandra's restriction rules), row fetching with paging, filtering and projection. */
final class Select {

    private static final String FILTERING_ERR = "Cannot execute this query as it might involve data filtering and thus may have unpredictable "
            + "performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";

    private final Engine e;
    private final CqlStore store;
    private final CqlShards shards;

    Select(Engine e) {
        this.e = e;
        this.store = e.store;
        this.shards = e.shards;
    }

    // ------------------------------------------------------------------ selection

    /** One output column. */
    private static final class Item {
        String name;
        CqlType type;
        Function<Rows.RowData, Object> eval;
        boolean aggregate;
        String aggFn;
        Term aggArg;
        boolean star;
    }

    private static final class RowView implements Eval.RowCtx {
        final Schema.Table t;
        final Rows.RowData r;

        RowView(Schema.Table t, Rows.RowData r) {
            this.t = t;
            this.r = r;
        }

        @Override
        public Eval.TV column(String name, Term index, String field) {
            Schema.Column c = t.col(name);
            if (c == null) {
                throw CqlError.invalid(Dml.undefined(t, name));
            }
            Object v = r == null ? null : r.vals[c.ordinal];
            if (index != null) {
                if (c.type.k == CqlType.K.MAP) {
                    Object k = Eval.coerce(index, c.type.args.get(0), Eval.Binds.NONE, name);
                    return new Eval.TV(v == null ? null : ((Map<?, ?>) v).get(k), c.type.args.get(1));
                }
                if (c.type.k == CqlType.K.LIST) {
                    Object k = Eval.coerce(index, CqlType.INT, Eval.Binds.NONE, name);
                    List<?> l = (List<?>) v;
                    return new Eval.TV(l == null || (Integer) k < 0 || (Integer) k >= l.size() ? null : l.get((Integer) k), c.type.args.get(0));
                }
                throw CqlError.invalid("Element selection is only allowed on sets and maps");
            }
            if (field != null) {
                Object cur = v;
                CqlType ct = c.type;
                for (String f : field.split("\\.")) {
                    int i = ct.k == CqlType.K.UDT ? ct.fieldNames.indexOf(f) : -1;
                    if (i < 0) {
                        throw CqlError.invalid(name + " of type " + ct.cql() + " has no field " + f);
                    }
                    cur = cur == null ? null : ((Object[]) cur)[i];
                    ct = ct.args.get(i);
                }
                return new Eval.TV(cur, ct);
            }
            return new Eval.TV(v, c.type);
        }

        @Override
        public Eval.TV meta(String fn, String col) {
            Schema.Column c = t.col(col);
            if (c == null) {
                throw CqlError.invalid(Dml.undefined(t, col));
            }
            if (c.isKey()) {
                throw CqlError.invalid("Cannot use selection function " + fn + " on PRIMARY KEY part " + col);
            }
            boolean ttl = fn.equals("ttl");
            if (c.type.isMultiCell()) {
                CqlType lt = CqlType.list(ttl ? CqlType.INT : CqlType.BIGINT, false);
                if (r == null || r.elemTs == null || r.elemTs[c.ordinal] == null) {
                    return new Eval.TV(null, lt);
                }
                List<Object> out = new ArrayList<>();
                long[] src = ttl ? r.elemTtl[c.ordinal] : r.elemTs[c.ordinal];
                for (long x : src) {
                    out.add(ttl ? (x < 0 ? null : (Object) (int) x) : (Object) x);
                }
                return new Eval.TV(out, lt);
            }
            if (r == null || r.vals[c.ordinal] == null) {
                return new Eval.TV(null, ttl ? CqlType.INT : CqlType.BIGINT);
            }
            if (ttl) {
                return new Eval.TV(r.ttl[c.ordinal] < 0 ? null : (Object) (int) r.ttl[c.ordinal], CqlType.INT);
            }
            return new Eval.TV(r.ts[c.ordinal], CqlType.BIGINT);
        }
    }

    static String describe(Term t) {
        if (t instanceof ColRef c) {
            return c.name() + (c.index() != null ? "[" + describe(c.index()) + "]" : c.field() != null ? "." + c.field() : "");
        }
        if (t instanceof Lit l) {
            return l.value() == null ? "null" : l.isString() ? "'" + l.value() + "'" : l.value() instanceof byte[] b ? "0x" + CqlType.hex(b)
                    : l.value() instanceof long[][] d ? CqlType.formatDuration(d[0]) : String.valueOf(l.value());
        }
        if (t instanceof Bind) {
            return "?";
        }
        if (t instanceof Func f) {
            String n = f.name().toLowerCase(Locale.ROOT);
            StringBuilder b = new StringBuilder();
            if (!(n.equals("writetime") || n.equals("ttl") || n.equals("maxwritetime"))) {
                b.append(f.ks() != null ? f.ks() : "system").append('.');
            }
            b.append(n).append('(');
            if (f.star()) {
                b.append('*');
            }
            for (int i = 0; i < f.args().size(); i++) {
                b.append(i > 0 ? ", " : "").append(describe(f.args().get(i)));
            }
            return b.append(')').toString();
        }
        if (t instanceof Cast c) {
            return "cast(" + describe(c.t()) + " as " + c.type() + ")";
        }
        if (t instanceof Arith a) {
            return describe(a.l()) + " " + a.op() + " " + describe(a.r());
        }
        if (t instanceof TypeHint h) {
            return "(" + h.type() + ")" + describe(h.t());
        }
        if (t instanceof ListLit l) {
            return "[" + String.join(", ", l.items().stream().map(Select::describe).toList()) + "]";
        }
        if (t instanceof TupleLit l) {
            return "(" + String.join(", ", l.items().stream().map(Select::describe).toList()) + ")";
        }
        if (t instanceof BraceLit b) {
            if (!b.pairs().isEmpty()) {
                return "{" + String.join(", ", b.pairs().stream().map(p -> describe(p[0]) + ": " + describe(p[1])).toList()) + "}";
            }
            if (!b.fields().isEmpty()) {
                return "{" + String.join(", ", b.fields().entrySet().stream().map(e -> e.getKey() + ": " + describe(e.getValue())).toList()) + "}";
            }
            return "{" + String.join(", ", b.items().stream().map(Select::describe).toList()) + "}";
        }
        return "?";
    }

    private static final java.util.Set<String> AGGS = java.util.Set.of("count", "sum", "avg", "min", "max");

    private List<Item> selection(Ast.Select st, Schema.Table t, Eval.Binds b) {
        List<Item> items = new ArrayList<>();
        for (Selector s : st.selectors()) {
            if (s.star()) {
                for (Schema.Column c : t.selectOrder) {
                    Item it = new Item();
                    it.name = c.name;
                    it.type = c.type;
                    final int o = c.ordinal;
                    it.eval = r -> r.vals[o];
                    it.star = true;
                    items.add(it);
                }
                continue;
            }
            Term x = s.expr();
            if (x instanceof Lit) {
                throw CqlError.invalid("Cannot infer type for term " + describe(x) + " in selection clause (try using a cast to force a type)");
            }
            Item it = new Item();
            it.name = s.alias() != null ? s.alias() : describe(x);
            if (x instanceof Func f && AGGS.contains(f.name().toLowerCase(Locale.ROOT)) && f.ks() == null) {
                it.aggregate = true;
                it.aggFn = f.name().toLowerCase(Locale.ROOT);
                it.aggArg = f.star() ? null : f.args().get(0);
                if (s.alias() == null) {
                    it.name = it.aggFn.equals("count") && f.star() ? "count" : describe(x);
                }
                if (it.aggFn.equals("count")) {
                    it.type = CqlType.BIGINT;
                } else {
                    if (it.aggArg == null) {
                        throw CqlError.invalid("Invalid number of arguments for function system." + it.aggFn);
                    }
                    Eval.TV probe = Eval.evalTyped(it.aggArg, null, b, new RowView(t, null));
                    it.type = probe.t();
                    if ((it.aggFn.equals("sum") || it.aggFn.equals("avg")) && !it.type.isNumeric()) {
                        throw CqlError.invalid("Invalid call to function " + it.aggFn + ", none of its type signatures match");
                    }
                }
                items.add(it);
                continue;
            }
            if (x instanceof ColRef c && c.index() == null && c.field() == null) {
                Schema.Column col = t.col(c.name());
                if (col == null) {
                    throw CqlError.invalid(Dml.undefined(t, c.name()));
                }
                it.type = col.type;
                final int o = col.ordinal;
                it.eval = r -> r.vals[o];
                items.add(it);
                continue;
            }
            if (x instanceof Func f && f.name().equalsIgnoreCase("token") && f.ks() == null) {
                it.type = CqlType.BIGINT;
                it.name = s.alias() != null ? s.alias() : "system.token(" + String.join(", ", f.args().stream().map(Select::describe).toList()) + ")";
                it.eval = r -> r.token;
                items.add(it);
                continue;
            }
            Eval.TV probe = Eval.evalTyped(x, null, b, new RowView(t, null));
            it.type = probe.t() == null ? CqlType.TEXT : probe.t();
            final Term xx = x;
            it.eval = r -> Eval.evalTyped(xx, null, b, new RowView(t, r)).v();
            items.add(it);
        }
        return items;
    }

    // ------------------------------------------------------------------ predicates

    private interface Pred {
        boolean test(Rows.RowData r);
    }

    private static Object elementOf(Schema.Column c, Object v, Object key) {
        if (v == null) {
            return null;
        }
        if (c.type.k == CqlType.K.MAP) {
            return ((Map<?, ?>) v).get(key);
        }
        List<?> l = (List<?>) v;
        int i = (Integer) key;
        return i < 0 || i >= l.size() ? null : l.get(i);
    }

    /** The value of a token relation's right hand side: a bigint or token(v1, ..) computed with the partition key types. */
    private static Object tokenValue(Term rhs, Schema.Table t, Eval.Binds b) {
        if (rhs instanceof Func f && f.name().equalsIgnoreCase("token") && f.ks() == null) {
            if (f.args().size() != t.partition.size()) {
                throw CqlError.invalid("Invalid number of arguments in call to function system.token: " + t.partition.size() + " required but "
                        + f.args().size() + " provided");
            }
            Object[] pv = new Object[t.partition.size()];
            for (int i = 0; i < pv.length; i++) {
                pv[i] = Eval.coerce(f.args().get(i), t.partition.get(i).type, b, t.partition.get(i).name);
                if (pv[i] == null) {
                    throw CqlError.invalid("Invalid null value for partition key part " + t.partition.get(i).name);
                }
            }
            return Murmur3.token(t.partitionBytes(pv));
        }
        return Eval.coerce(rhs, CqlType.BIGINT, b, "token");
    }

    private static Term tupleRhs(Term rhs) {
        return rhs instanceof TupleLit || rhs instanceof Bind ? rhs : new TupleLit(List.of(rhs));
    }

    private Pred predicate(Schema.Table t, Relation r, Eval.Binds b) {
        if (!r.token()) {
            for (String n : r.cols()) {
                if (t.col(n) == null) {
                    throw CqlError.invalid(Dml.undefined(t, n));
                }
            }
        }
        if (r.token()) {
            Object v = r.rhs() == null ? null : tokenValue(r.rhs(), t, b);
            String op = r.op();
            return row -> Dml.cmpOp(op, Long.compare(row.token, (Long) v)) || op.equals("=") && row.token == (Long) v;
        }
        if (r.multi()) {
            List<Schema.Column> cols = new ArrayList<>();
            for (String n : r.cols()) {
                Schema.Column mc = t.col(n);
                if (mc.kind != Schema.Kind.CLUSTERING) {
                    throw CqlError.invalid("Multi-column relations can only be applied to clustering columns but was applied to: " + n);
                }
                cols.add(mc);
            }
            CqlType tt = CqlType.tuple(cols.stream().map(c -> c.type).toList(), false);
            if (r.op().equals("in")) {
                List<Object[]> alts = new ArrayList<>();
                if (r.inList() != null) {
                    for (Term x : r.inList()) {
                        alts.add((Object[]) Eval.coerce(tupleRhs(x), tt, b, "tuple"));
                    }
                } else {
                    for (Object o : (List<?>) Eval.coerce(r.rhs(), CqlType.list(tt, false), b, "tuple")) {
                        alts.add((Object[]) o);
                    }
                }
                return row -> {
                    Object[] have = new Object[cols.size()];
                    for (int i = 0; i < have.length; i++) {
                        have[i] = row.vals[cols.get(i).ordinal];
                    }
                    for (Object[] a : alts) {
                        if (tt.compare(have, a) == 0) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            Object[] want = (Object[]) Eval.coerce(tupleRhs(r.rhs()), tt, b, "tuple");
            String op = r.op();
            return row -> {
                Object[] have = new Object[cols.size()];
                for (int i = 0; i < have.length; i++) {
                    have[i] = row.vals[cols.get(i).ordinal];
                }
                int c = 0;
                for (int i = 0; i < have.length && c == 0; i++) {
                    if (want[i] == null) {
                        break;
                    }
                    c = cols.get(i).type.compare(have[i], want[i]);
                }
                return op.equals("=") ? c == 0 : Dml.cmpOp(op, c);
            };
        }
        Schema.Column col = t.col(r.cols().get(0));
        final int o = col.ordinal;
        String op = r.op();
        if (r.index() != null) {
            CqlType kt = col.type.k == CqlType.K.MAP ? col.type.args.get(0) : CqlType.INT;
            CqlType vt = col.type.k == CqlType.K.MAP ? col.type.args.get(1) : col.type.args.get(0);
            Object key = Eval.coerce(r.index(), kt, b, col.name);
            Object want = Eval.coerce(r.rhs(), vt, b, col.name);
            return row -> {
                Object have = elementOf(col, row.vals[o], key);
                return have != null && want != null && (op.equals("=") ? vt.compare(have, want) == 0 : Dml.cmpOp(op, vt.compare(have, want)));
            };
        }
        switch (op) {
            case "in" -> {
                List<Object> alts = new ArrayList<>();
                if (r.inList() != null) {
                    for (Term x : r.inList()) {
                        alts.add(Eval.coerce(x, col.type, b, col.name));
                    }
                } else {
                    alts.addAll((List<?>) Eval.coerce(r.rhs(), CqlType.list(col.type, false), b, col.name));
                }
                return row -> {
                    Object have = row.vals[o];
                    for (Object a : alts) {
                        if (have != null && a != null && col.type.compare(have, a) == 0) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            case "contains", "contains key" -> {
                CqlType et = col.type.k == CqlType.K.MAP && op.equals("contains key") ? col.type.args.get(0)
                        : col.type.k == CqlType.K.MAP ? col.type.args.get(1) : col.type.args.get(0);
                if (!col.type.isCollection()) {
                    throw CqlError.invalid("Cannot use " + op.toUpperCase(Locale.ROOT) + " on non-collection column " + col.name);
                }
                Object want = Eval.coerce(r.rhs(), et, b, col.name);
                boolean keys = op.equals("contains key");
                return row -> {
                    Object have = row.vals[o];
                    if (have == null || want == null) {
                        return false;
                    }
                    java.util.Collection<?> c = have instanceof Map<?, ?> m ? (keys ? m.keySet() : m.values()) : (java.util.Collection<?>) have;
                    for (Object x : c) {
                        if (et.compare(x, want) == 0) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            case "like" -> throw CqlError.invalid("LIKE restriction is only supported on properly indexed columns. " + col.name + " LIKE "
                    + describe(r.rhs()) + " is not valid.");
            case "!=" -> throw CqlError.invalid("Unsupported '!=' relation: " + col.name + " != ...");
            case "is not null" -> throw CqlError.invalid("Unsupported restriction: " + col.name + " IS NOT NULL");
            default -> {
                if (col.type.k == CqlType.K.DURATION && !op.equals("=")) {
                    throw CqlError.invalid("Slice restrictions are not supported on duration columns");
                }
                Object want = Eval.coerce(r.rhs(), col.type, b, col.name);
                if (want == null) {
                    throw CqlError.invalid(col.isKey() ? "Invalid null value in condition for column " + col.name
                            : "Unsupported null value for column " + col.name);
                }
                if (want == Eval.UNSET_VALUE) {
                    throw CqlError.invalid("Invalid unset value for column " + col.name);
                }
                return row -> {
                    Object have = row.vals[o];
                    if (have == null) {
                        return false;
                    }
                    int c = col.type.compare(have, want);
                    return op.equals("=") ? c == 0 : Dml.cmpOp(op, c);
                };
            }
        }
    }

    // ------------------------------------------------------------------ plan

    private static final class Unit {
        Object[] pkv;
        byte[] pk;
        long token;
        String host;
        byte[] ckLo, ckHi;
    }

    private static final class Plan {
        Schema.Table t;
        List<Item> items;
        List<Pred> preds = new ArrayList<>();
        List<Unit> units;
        boolean scanAll;
        Long tokLo, tokHi;
        boolean tokLoIncl = true, tokHiIncl = true;
        boolean reversed;
        boolean staticRows;
        long limit = Long.MAX_VALUE;
        long perPartition = Long.MAX_VALUE;
        boolean aggregate, distinct, json;
        boolean multiPartition;
        boolean sortByCk;
        List<Schema.Column> groupBy;
    }

    private static boolean isEqOrIn(Relation r) {
        return r.op().equals("=") || r.op().equals("in");
    }

    private Plan plan(Ast.Select st, Schema.Table t, Eval.Binds b, int pageSize) {
        Plan p = new Plan();
        p.t = t;
        p.items = selection(st, t, b);
        p.aggregate = p.items.stream().anyMatch(i -> i.aggregate);
        p.distinct = st.distinct();
        p.json = st.json();
        if (st.limit() != null) {
            Object v = Eval.coerce(st.limit(), CqlType.INT, b, "[limit]");
            if (v == null || v == Eval.UNSET_VALUE) {
                throw CqlError.invalid("Invalid null value of limit");
            }
            if ((Integer) v <= 0) {
                throw CqlError.invalid("LIMIT must be strictly positive");
            }
            p.limit = (Integer) v;
        }
        if (st.perPartitionLimit() != null) {
            Object v = Eval.coerce(st.perPartitionLimit(), CqlType.INT, b, "[per_partition_limit]");
            if ((Integer) v <= 0) {
                throw CqlError.invalid("LIMIT must be strictly positive");
            }
            p.perPartition = (Integer) v;
        }
        if (p.distinct) {
            for (Item it : p.items) {
                boolean ok = false;
                for (Selector s : st.selectors()) {
                    if (s.star()) {
                        throw CqlError.invalid("SELECT DISTINCT queries must only request partition key columns and/or static columns");
                    }
                }
                Schema.Column c = t.col(it.name);
                ok = c != null && (c.kind == Schema.Kind.PARTITION_KEY || c.kind == Schema.Kind.STATIC);
                if (!ok && !it.aggregate) {
                    throw CqlError.invalid("SELECT DISTINCT queries must only request partition key columns and/or static columns (not " + it.name + ")");
                }
            }
        }
        boolean filtering = st.allowFiltering();
        Map<String, List<Relation>> byCol = new LinkedHashMap<>();
        List<Relation> tokenRels = new ArrayList<>();
        for (Relation r : st.where()) {
            p.preds.add(predicate(t, r, b));
            if (r.token()) {
                tokenRels.add(r);
                List<String> pkNames = t.partition.stream().map(c -> c.name).toList();
                if (!r.cols().equals(pkNames)) {
                    if (new java.util.HashSet<>(r.cols()).containsAll(pkNames) && r.cols().size() == pkNames.size()) {
                        throw CqlError.invalid("The token function arguments must be in the partition key order: " + String.join(", ", pkNames));
                    }
                    throw CqlError.invalid("The token() function must be applied to all partition key components or none of them");
                }
                if (r.op().equals("in")) {
                    throw CqlError.invalid("IN restrictions are not supported on token relations");
                }
                continue;
            }
            for (String n : r.cols()) {
                Schema.Column c = t.col(n);
                if (c == null) {
                    throw CqlError.invalid(Dml.undefined(t, n));
                }
                byCol.computeIfAbsent(n, k -> new ArrayList<>()).add(r);
            }
        }
        for (var en : byCol.entrySet()) {
            Schema.Column kc = t.col(en.getKey());
            if (!kc.isKey()) {
                continue;
            }
            List<Relation> single = en.getValue().stream().filter(x -> !x.multi()).toList();
            long eqs = single.stream().filter(x -> x.op().equals("=")).count(), ins = single.stream().filter(x -> x.op().equals("in")).count();
            long lows = single.stream().filter(x -> x.op().equals(">") || x.op().equals(">=")).count();
            long highs = single.stream().filter(x -> x.op().equals("<") || x.op().equals("<=")).count();
            if (eqs > 0 && single.size() > 1) {
                throw CqlError.invalid(kc.name + " cannot be restricted by more than one relation if it includes an Equal");
            }
            if (ins > 0 && single.size() > 1) {
                throw CqlError.invalid(kc.name + " cannot be restricted by more than one relation if it includes a IN");
            }
            if (lows > 1) {
                throw CqlError.invalid("More than one restriction was found for the start bound on " + kc.name);
            }
            if (highs > 1) {
                throw CqlError.invalid("More than one restriction was found for the end bound on " + kc.name);
            }
        }
        // partition key
        int pkRestricted = 0;
        boolean pkNonEq = false, pkIn = false;
        for (Schema.Column c : t.partition) {
            List<Relation> rs = byCol.get(c.name);
            if (rs != null) {
                pkRestricted++;
                for (Relation r : rs) {
                    if (!isEqOrIn(r) || r.multi()) {
                        pkNonEq = true;
                    }
                    pkIn |= r.op().equals("in");
                }
            }
        }
        boolean pkFull = pkRestricted == t.partition.size() && !pkNonEq;
        boolean virtualTable = t.virtual != null;
        if (!filtering && !virtualTable) {
            if (!tokenRels.isEmpty() && pkRestricted > 0) {
                throw CqlError.invalid(FILTERING_ERR);
            }
            if ((pkNonEq || pkRestricted > 0 && !pkFull)) {
                throw CqlError.invalid(FILTERING_ERR);
            }
        }
        // clustering restrictions
        boolean sliceSeen = false;
        String gap = null, sliceOwner = null;
        List<Schema.Column> pushdownEq = new ArrayList<>();
        Schema.Column sliceCol = null;
        boolean ckFilter = false;
        for (Schema.Column c : t.clustering) {
            List<Relation> rs = byCol.get(c.name);
            if (rs == null) {
                if (gap == null) {
                    gap = c.name;
                }
                continue;
            }
            boolean allEq = rs.stream().allMatch(r -> isEqOrIn(r) && !r.multi() && r.index() == null);
            boolean multi = rs.stream().anyMatch(Relation::multi);
            boolean slice = rs.stream().allMatch(r -> List.of("<", "<=", ">", ">=").contains(r.op()) && !r.multi() && r.index() == null);
            boolean other = rs.stream().anyMatch(r -> r.op().equals("contains") || r.op().equals("contains key") || r.op().equals("like")
                    || r.index() != null || r.op().equals("!=") || r.op().equals("is not null"));
            boolean pushable = pkFull && gap == null && sliceOwner == null;
            if (multi && !other && pushable && multiOk(t, rs, c, pushdownEq)) {
                ckFilter = true;
                sliceOwner = c.name;
            } else if (multi && sliceOwner != null && multiContinues(rs, sliceOwner)) {
                ckFilter = true;
            } else if (!other && !multi && allEq && rs.size() == 1 && pushable) {
                pushdownEq.add(c);
            } else if (!other && !multi && slice && pushable && sliceCol == null) {
                sliceCol = c;
                sliceOwner = c.name;
            } else {
                if (!filtering && !virtualTable) {
                    if (!pkFull) {
                        throw CqlError.invalid(FILTERING_ERR);
                    }
                    if (gap != null) {
                        throw CqlError.invalid("PRIMARY KEY column \"" + c.name + "\" cannot be restricted as preceding column \"" + gap
                                + "\" is not restricted");
                    }
                    if (sliceOwner != null) {
                        throw CqlError.invalid("Clustering column \"" + c.name + "\" cannot be restricted (preceding column \"" + sliceOwner
                                + "\" is restricted by a non-EQ relation)");
                    }
                    throw CqlError.invalid(FILTERING_ERR);
                }
                ckFilter = true;
                if (gap == null) {
                    gap = c.name;
                }
            }
        }
        // regular / static column restrictions
        boolean regularFilter = false;
        boolean usesIndex = false;
        int regularCount = 0;
        for (var en : byCol.entrySet()) {
            Schema.Column c = t.col(en.getKey());
            if (c.isKey()) {
                continue;
            }
            regularCount += en.getValue().size();
            if (t.indexed(c.name)) {
                usesIndex = true;
            } else if (!filtering && !virtualTable) {
                throw CqlError.invalid(FILTERING_ERR);
            }
            regularFilter = true;
        }
        if (!filtering && !virtualTable && regularCount > 1) {
            throw CqlError.invalid(FILTERING_ERR);
        }
        if (!pkFull && tokenRels.isEmpty() && !byCol.isEmpty() && !usesIndex && !filtering && !virtualTable) {
            throw CqlError.invalid(FILTERING_ERR);
        }
        // ORDER BY
        if (!st.order().isEmpty()) {
            if (!pkFull && !virtualTable) {
                throw CqlError.invalid("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
            }
            if (pkIn && pageSize > 0) {
                throw CqlError.invalid("Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the "
                        + "ORDER BY or the IN and sort client side, or disable paging for this query");
            }
            List<Schema.Column> ordCols = new ArrayList<>();
            for (Order o : st.order()) {
                Schema.Column c = t.col(o.col());
                if (c == null) {
                    throw CqlError.invalid(Dml.undefined(t, o.col()));
                }
                if (c.kind != Schema.Kind.CLUSTERING) {
                    throw CqlError.invalid("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got " + o.col());
                }
                ordCols.add(c);
            }
            // the columns must follow the clustering order, skipping EQ restricted ones
            int ci = 0;
            List<Schema.Column> expect = new ArrayList<>();
            for (Schema.Column c : t.clustering) {
                if (!(pushdownEq.contains(c) && isSingleEq(byCol.get(c.name)))) {
                    expect.add(c);
                }
            }
            for (Schema.Column c : ordCols) {
                if (ci >= expect.size() || expect.get(ci) != c) {
                    throw CqlError.invalid("Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY");
                }
                ci++;
            }
            boolean rev = st.order().get(0).desc() != ordCols.get(0).desc;
            for (int i = 0; i < ordCols.size(); i++) {
                if ((st.order().get(i).desc() != ordCols.get(i).desc) != rev) {
                    throw CqlError.invalid("Unsupported order by relation");
                }
            }
            p.reversed = rev;
        }
        // GROUP BY
        if (!st.groupBy().isEmpty()) {
            List<Schema.Column> gcols = new ArrayList<>();
            for (String g : st.groupBy()) {
                Schema.Column c = t.col(g);
                if (c == null) {
                    throw CqlError.invalid(Dml.undefined(t, g));
                }
                if (!c.isKey()) {
                    throw CqlError.invalid("Group by is currently only supported on the columns of the PRIMARY KEY, got " + g);
                }
                gcols.add(c);
            }
            List<Schema.Column> order = new ArrayList<>(t.partition);
            order.addAll(t.clustering);
            for (int i = 0; i < gcols.size(); i++) {
                if (order.get(i) != gcols.get(i)) {
                    throw CqlError.invalid("Group by currently only support groups of columns following their declared order in the PRIMARY KEY");
                }
            }
            p.groupBy = gcols;
        }
        // build the units
        if (virtualTable) {
            p.scanAll = true;
        } else if (pkFull) {
            Dml.Keys k = e.dml.keys(t, st.where().stream().filter(r -> !r.token()
                    && (t.col(r.cols().get(0)).kind == Schema.Kind.PARTITION_KEY)).toList(), b, true, false);
            List<byte[][]> ranges = new ArrayList<>();
            List<Object[]> cks = new ArrayList<>();
            // clustering eq prefix combos and slice
            Dml.Keys ck = ckKeys(t, st.where(), b, pushdownEq, sliceCol);
            ranges = ckRanges(t, ck);
            p.units = new ArrayList<>();
            List<Unit> units = new ArrayList<>();
            for (Object[] pkv : k.pks) {
                byte[] pk = t.partitionBytes(pkv);
                if (t.partition.size() == 1 && pk.length == 0) {
                    throw CqlError.invalid("Key may not be empty");
                }
                long token = Murmur3.token(pk);
                String host = shards.owner(shards.hosts(), pk);
                for (byte[][] rg : ranges) {
                    Unit u = new Unit();
                    u.pkv = pkv;
                    u.pk = pk;
                    u.token = token;
                    u.host = host;
                    u.ckLo = rg[0];
                    u.ckHi = rg[1];
                    units.add(u);
                }
            }
            if (k.pks.size() > 1) {
                p.multiPartition = true;
            }
            p.units = units;
            if (p.reversed) {
                // reversed order applies within each partition; the range list is reversed too
                List<Unit> rev = new ArrayList<>();
                int per = ranges.size();
                for (int i = 0; i < units.size(); i += per) {
                    List<Unit> grp = new ArrayList<>(units.subList(i, i + per));
                    java.util.Collections.reverse(grp);
                    rev.addAll(grp);
                }
                p.units = rev;
            }
            p.sortByCk = p.multiPartition && !st.order().isEmpty();
        } else {
            p.scanAll = true;
            for (Relation r : tokenRels) {
                Object v = tokenValue(r.rhs(), t, b);
                if (v == null) {
                    throw CqlError.invalid("Invalid null token value");
                }
                long x = (Long) v;
                switch (r.op()) {
                    case ">" -> {
                        if (p.tokLo == null || x >= p.tokLo) {
                            p.tokLo = x;
                            p.tokLoIncl = false;
                        }
                    }
                    case ">=" -> {
                        if (p.tokLo == null || x > p.tokLo) {
                            p.tokLo = x;
                            p.tokLoIncl = true;
                        }
                    }
                    case "<" -> {
                        if (p.tokHi == null || x <= p.tokHi) {
                            p.tokHi = x;
                            p.tokHiIncl = false;
                        }
                    }
                    case "<=" -> {
                        if (p.tokHi == null || x < p.tokHi) {
                            p.tokHi = x;
                            p.tokHiIncl = true;
                        }
                    }
                    case "=" -> {
                        p.tokLo = x;
                        p.tokHi = x;
                    }
                    default -> throw CqlError.invalid("Unsupported operator " + r.op() + " on token");
                }
            }
        }
        p.staticRows = t.hasStatic && sliceCol == null && pushdownEq.isEmpty() && !ckFilter && !regularFilter;
        if (st.order().isEmpty() && p.reversed) {
            p.reversed = false;
        }
        return p;
    }

    private static boolean multiContinues(List<Relation> rs, String owner) {
        return rs.stream().allMatch(r -> r.cols().get(0).equals(owner));
    }

    /** A multi-column relation must start at the first unrestricted clustering column and cover consecutive columns. */
    private static boolean multiOk(Schema.Table t, List<Relation> rs, Schema.Column c, List<Schema.Column> eqCols) {
        for (Relation r : rs) {
            if (!r.multi() || !r.cols().get(0).equals(c.name) || r.index() != null) {
                return false;
            }
            int start = t.clustering.indexOf(c);
            if (start != eqCols.size()) {
                return false;
            }
            for (int i = 0; i < r.cols().size(); i++) {
                if (start + i >= t.clustering.size() || !t.clustering.get(start + i).name.equals(r.cols().get(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isSingleEq(List<Relation> rs) {
        return rs != null && rs.size() == 1 && rs.get(0).op().equals("=");
    }

    private Dml.Keys ckKeys(Schema.Table t, List<Relation> where, Eval.Binds b, List<Schema.Column> eqCols, Schema.Column sliceCol) {
        List<Relation> rels = new ArrayList<>();
        for (Relation r : where) {
            if (r.token() || r.multi()) {
                continue;
            }
            Schema.Column c = t.col(r.cols().get(0));
            if (eqCols.contains(c) || c == sliceCol) {
                rels.add(r);
            } else if (c.kind == Schema.Kind.PARTITION_KEY) {
                rels.add(r);
            }
        }
        return e.dml.keys(t, rels, b, true, false);
    }

    /** [lo, hi) byte ranges of the clustering key restrictions; a single unbounded range when there are none. */
    private List<byte[][]> ckRanges(Schema.Table t, Dml.Keys k) {
        List<byte[][]> out = new ArrayList<>();
        if (t.clustering.isEmpty()) {
            out.add(new byte[][] {null, null});
            return out;
        }
        List<Object[]> combos = k.cks.isEmpty() || k.prefixLen == 0 ? java.util.Collections.singletonList(new Object[0]) : k.cks;
        // sort the alternatives in clustering order
        List<byte[]> prefixes = new ArrayList<>();
        for (Object[] c : combos) {
            prefixes.add(t.clusteringBytes(c, c.length));
        }
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < prefixes.size(); i++) {
            order.add(i);
        }
        order.sort((x, y) -> Arrays.compareUnsigned(prefixes.get(x), prefixes.get(y)));
        for (int oi : order) {
            byte[] prefix = prefixes.get(oi);
            byte[] lo = prefix.length == 0 ? null : prefix;
            byte[] hi = prefix.length == 0 ? null : Rows.succ(prefix);
            if (k.slice != null) {
                Schema.Column sc = t.clustering.get(combos.get(oi).length);
                byte[] loKey = k.slice[0] == null ? null : concat(prefix, sc.type.keyEncode(k.slice[0], sc.desc));
                byte[] hiKey = k.slice[2] == null ? null : concat(prefix, sc.type.keyEncode(k.slice[2], sc.desc));
                boolean loIncl = (Boolean) k.slice[1], hiIncl = (Boolean) k.slice[3];
                byte[] base = prefix.length == 0 ? new byte[] {0} : prefix;
                byte[] from, to;
                if (!sc.desc) {
                    from = loKey == null ? base : loIncl ? loKey : Rows.succ(loKey);
                    to = hiKey == null ? hi : hiIncl ? Rows.succ(hiKey) : hiKey;
                } else {
                    from = hiKey == null ? base : hiIncl ? hiKey : Rows.succ(hiKey);
                    to = loKey == null ? hi : loIncl ? Rows.succ(loKey) : loKey;
                }
                lo = from;
                hi = to;
            }
            out.add(new byte[][] {lo, hi});
        }
        return out;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    // ------------------------------------------------------------------ paging state

    private static final class PageState {
        int unit;
        long remaining = -1;
        int perPartCount;
        boolean hasKey;
        long token;
        byte[] pk;
        byte[] ck;
        int offset;

        byte[] encode() {
            Wire.Out o = new Wire.Out().u8(1).i32(unit).i64(remaining).i32(perPartCount).u8(hasKey ? 1 : 0).i64(token);
            o.bytes(pk).bytes(ck).i32(offset);
            return o.toBytes();
        }

        static PageState decode(byte[] b) {
            try {
                Wire.In in = new Wire.In(b);
                if (in.u8() != 1) {
                    throw new IllegalArgumentException();
                }
                PageState s = new PageState();
                s.unit = in.i32();
                s.remaining = in.i64();
                s.perPartCount = in.i32();
                s.hasKey = in.u8() == 1;
                s.token = in.i64();
                s.pk = in.bytes();
                s.ck = in.bytes();
                s.offset = in.i32();
                return s;
            } catch (RuntimeException ex) {
                throw CqlError.invalid("Invalid value for the paging state");
            }
        }
    }

    // ------------------------------------------------------------------ execution

    List<Result.ColSpec> resultSpecs(Ast.Select st, ClientState cs, Eval.Binds b) {
        Schema.Table t = e.dml.table(st.ks(), st.table(), cs, false);
        List<Item> items = selection(st, t, b);
        if (st.json()) {
            return List.of(new Result.ColSpec(t.ks, t.name, "[json]", CqlType.TEXT));
        }
        List<Result.ColSpec> out = new ArrayList<>();
        for (Item it : items) {
            out.add(new Result.ColSpec(t.ks, t.name, it.name, it.type));
        }
        return out;
    }

    Result select(Ast.Select st, Eval.Binds b, ClientState cs, QueryOpts o) {
        Schema.Table t = e.dml.table(st.ks(), st.table(), cs, false);
        SysTables.LOCAL_ADDRESS.set(cs.localAddress);
        Plan p = plan(st, t, b, o.pageSize);
        List<Result.ColSpec> specs = new ArrayList<>();
        if (p.json) {
            specs.add(new Result.ColSpec(t.ks, t.name, "[json]", CqlType.TEXT));
        } else {
            for (Item it : p.items) {
                specs.add(new Result.ColSpec(t.ks, t.name, it.name, it.type));
            }
        }
        int pageSize = o.pageSize;
        boolean materialize = p.aggregate || p.sortByCk || p.distinct || p.groupBy != null;
        PageState ps;
        if (materialize) {
            // the whole result is computed, then served page by page by offset
            ps = new PageState();
            pageSize = -1;
        } else {
            ps = o.pagingState != null ? PageState.decode(o.pagingState) : new PageState();
            if (ps.remaining < 0 && p.limit != Long.MAX_VALUE) {
                ps.remaining = p.limit;
            }
        }
        List<Rows.RowData> rows = new ArrayList<>();
        boolean more;
        if (t.virtual != null) {
            more = fetchVirtual(p, t, ps, pageSize, rows);
        } else {
            more = fetchReal(p, t, ps, pageSize, rows);
        }
        if (p.sortByCk) {
            rows.sort((x, y) -> {
                int c = Arrays.compareUnsigned(x.ck, y.ck);
                return p.reversed ? -c : c;
            });
        }
        List<byte[][]> out = new ArrayList<>();
        if (p.groupBy != null) {
            List<Rows.RowData> cur = new ArrayList<>();
            String curKey = null;
            for (Rows.RowData r : rows) {
                StringBuilder k = new StringBuilder();
                for (Schema.Column g : p.groupBy) {
                    Object v = r.vals[g.ordinal];
                    k.append(v == null ? "n" : CqlType.hex(g.type.serialize(v))).append('|');
                }
                String key = k.toString();
                if (!cur.isEmpty() && !key.equals(curKey)) {
                    out.add(p.aggregate ? aggregate(p, cur) : project(p, cur.get(0)));
                    cur = new ArrayList<>();
                }
                cur.add(r);
                curKey = key;
            }
            if (!cur.isEmpty()) {
                out.add(p.aggregate ? aggregate(p, cur) : project(p, cur.get(0)));
            }
        } else if (p.aggregate) {
            out.add(aggregate(p, rows));
        } else {
            for (Rows.RowData r : rows) {
                out.add(project(p, r));
            }
        }
        if (p.distinct) {
            List<byte[][]> d = new ArrayList<>();
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (byte[][] r : out) {
                StringBuilder k = new StringBuilder();
                for (byte[] c : r) {
                    k.append(c == null ? "n" : CqlType.hex(c)).append('|');
                }
                if (seen.add(k.toString())) {
                    d.add(r);
                }
            }
            out = d;
        }
        if (materialize) {
            if (p.limit != Long.MAX_VALUE && out.size() > p.limit) {
                out = new ArrayList<>(out.subList(0, (int) p.limit));
            }
            if (o.pageSize > 0 && !p.aggregate || o.pageSize > 0 && p.groupBy != null) {
                PageState in = o.pagingState != null ? PageState.decode(o.pagingState) : new PageState();
                int from = Math.min(in.offset, out.size());
                int to = Math.min(out.size(), from + o.pageSize);
                boolean hasMore = to < out.size();
                PageState next = new PageState();
                next.offset = to;
                out = new ArrayList<>(out.subList(from, to));
                return Result.rows(specs, out, hasMore ? next.encode() : null);
            }
            return Result.rows(specs, out, null);
        }
        return Result.rows(specs, out, more ? ps.encode() : null);
    }

    private byte[][] project(Plan p, Rows.RowData r) {
        if (p.json) {
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < p.items.size(); i++) {
                Item it = p.items.get(i);
                if (i > 0) {
                    sb.append(", ");
                }
                String n = it.name;
                Eval.jsonString(sb, n.matches("[a-z][a-z0-9_]*") ? n : "\"" + n + "\"");
                sb.append(": ").append(Eval.toJson(it.eval.apply(r), it.type));
            }
            return new byte[][] {CqlType.TEXT.serialize(sb.append('}').toString())};
        }
        byte[][] cells = new byte[p.items.size()][];
        for (int i = 0; i < cells.length; i++) {
            Item it = p.items.get(i);
            Object v = it.eval.apply(r);
            cells[i] = v == null ? null : it.type.serialize(v);
        }
        return cells;
    }

    private boolean fetchVirtual(Plan p, Schema.Table t, PageState ps, int pageSize, List<Rows.RowData> out) {
        List<Rows.RowData> all = new ArrayList<>();
        for (Map<String, Object> m : t.virtual.get()) {
            Rows.RowData r = new Rows.RowData();
            r.vals = new Object[t.columns.size()];
            r.ts = new long[r.vals.length];
            r.ttl = new long[r.vals.length];
            Arrays.fill(r.ttl, -1);
            for (Schema.Column c : t.columns) {
                r.vals[c.ordinal] = m.get(c.name);
            }
            Object[] pv = new Object[t.partition.size()];
            for (int i = 0; i < pv.length; i++) {
                pv[i] = r.vals[t.partition.get(i).ordinal];
            }
            r.pk = t.partitionBytes(pv);
            r.token = Murmur3.token(r.pk);
            Object[] cv = new Object[t.clustering.size()];
            for (int i = 0; i < cv.length; i++) {
                cv[i] = r.vals[t.clustering.get(i).ordinal];
            }
            r.ck = t.clustering.isEmpty() ? Rows.SINGLE_CK : t.clusteringBytes(cv, cv.length);
            all.add(r);
        }
        all.sort(Comparator.<Rows.RowData>comparingLong(r -> r.token).thenComparing((a, c) -> Arrays.compareUnsigned(a.pk, c.pk))
                .thenComparing((a, c) -> Arrays.compareUnsigned(a.ck, c.ck)));
        if (p.reversed) {
            java.util.Collections.reverse(all);
        }
        int i = ps.offset;
        long remaining = ps.remaining;
        for (; i < all.size(); i++) {
            if (pageSize > 0 && out.size() >= pageSize || remaining == 0) {
                break;
            }
            Rows.RowData r = all.get(i);
            if (matches(p, r)) {
                out.add(r);
                if (remaining > 0) {
                    remaining--;
                }
            }
        }
        ps.offset = i;
        ps.remaining = remaining;
        return pageSize > 0 && i < all.size() && remaining != 0;
    }

    private boolean matches(Plan p, Rows.RowData r) {
        if (r.dead) {
            return false;
        }
        for (Pred pr : p.preds) {
            if (!pr.test(r)) {
                return false;
            }
        }
        return true;
    }

    /** Returns true when more rows may follow (a paging state is needed). */
    private boolean fetchReal(Plan p, Schema.Table t, PageState ps, int pageSize, List<Rows.RowData> out) {
        long remaining = ps.remaining;
        boolean unitsMode = !p.scanAll;
        int unitIdx = ps.unit;
        boolean hasKey = ps.hasKey;
        long lastToken = ps.token;
        byte[] lastPk = ps.pk, lastCk = ps.ck;
        int perPartCount = ps.perPartCount;
        boolean pageFull = false;
        while (!pageFull && remaining != 0) {
            List<Rows.RowData> raw;
            int want = pageSize > 0 ? Math.max(pageSize - out.size(), 1) : 500;
            want = Math.max(want, p.preds.isEmpty() ? want : 100);
            boolean exhausted;
            if (unitsMode) {
                if (unitIdx >= p.units.size()) {
                    break;
                }
                Unit u = p.units.get(unitIdx);
                raw = fetchUnit(p, t, u, hasKey ? lastCk : null, want);
                exhausted = raw.size() < want;
            } else {
                boolean[] ex = {true};
                raw = fetchScan(p, t, hasKey ? lastToken : null, hasKey ? lastPk : null, hasKey ? lastCk : null, want, ex);
                exhausted = ex[0];
            }
            for (Rows.RowData r : raw) {
                boolean samePart = lastPk != null && Arrays.equals(lastPk, r.pk) && lastToken == r.token;
                if (!samePart) {
                    perPartCount = 0;
                }
                hasKey = true;
                lastToken = r.token;
                lastPk = r.pk;
                lastCk = r.ck;
                if (matches(p, r)) {
                    if (perPartCount < p.perPartition) {
                        out.add(r);
                        perPartCount++;
                        if (remaining > 0) {
                            remaining--;
                        }
                    }
                }
                if (remaining == 0) {
                    break;
                }
                if (pageSize > 0 && out.size() >= pageSize) {
                    pageFull = true;
                    break;
                }
            }
            if (remaining == 0) {
                break;
            }
            if (!pageFull && exhausted) {
                if (unitsMode) {
                    unitIdx++;
                    hasKey = false;
                    lastPk = null;
                    lastCk = null;
                    perPartCount = 0;
                } else {
                    break;
                }
            }
            if (!pageFull && !exhausted && raw.isEmpty()) {
                break;
            }
        }
        ps.unit = unitIdx;
        ps.remaining = remaining;
        ps.perPartCount = perPartCount;
        ps.hasKey = hasKey;
        ps.token = lastToken;
        ps.pk = lastPk;
        ps.ck = lastCk;
        boolean more = pageFull && remaining != 0;
        if (more && unitsMode) {
            more = true;
        }
        return more;
    }

    private List<Rows.RowData> fetchUnit(Plan p, Schema.Table t, Unit u, byte[] resumeCk, int want) {
        CqlStore.Scan s = new CqlStore.Scan();
        s.token = u.token;
        s.pk = u.pk;
        s.ckLo = u.ckLo;
        s.ckHi = u.ckHi;
        s.reversed = p.reversed;
        s.limit = want;
        s.staticRows = p.staticRows;
        if (resumeCk != null) {
            s.resumePk = u.pk;
            s.resumeCk = resumeCk;
        }
        return e.timed(u.host, "read", () -> shards.conn(u.host, c -> readRows(c, t, s)));
    }

    private List<Rows.RowData> readRows(Connection c, Schema.Table t, CqlStore.Scan s) throws SQLException {
        List<CqlStore.Rec> recs = store.scan(c, t.id, s);
        List<CqlStore.Rec> statics = List.of();
        List<long[]> toks = new ArrayList<>();
        List<byte[]> pks = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (CqlStore.Rec r : recs) {
            if (seen.add(Rows.pkKey(r.token, r.pk))) {
                toks.add(new long[] {r.token});
                pks.add(r.pk);
            }
        }
        if (t.hasStatic) {
            List<long[]> st = new ArrayList<>();
            List<byte[]> sp = new ArrayList<>();
            for (int i = 0; i < pks.size(); i++) {
                boolean hasRegular = false;
                for (CqlStore.Rec r : recs) {
                    if (r.ck.length > 0 && r.token == toks.get(i)[0] && Arrays.equals(r.pk, pks.get(i))) {
                        hasRegular = true;
                        break;
                    }
                }
                if (hasRegular) {
                    st.add(toks.get(i));
                    sp.add(pks.get(i));
                }
            }
            statics = store.statics(c, t.id, st, sp);
        }
        return Rows.assemble(t, recs, statics, store.rangeTombstones(c, t.id, toks, pks));
    }

    private List<Rows.RowData> fetchScan(Plan p, Schema.Table t, Long token, byte[] pk, byte[] ck, int want, boolean[] exhausted) {
        List<String> hosts = shards.allHosts();
        List<Rows.RowData> merged = new ArrayList<>();
        exhausted[0] = true;
        for (String h : hosts) {
            CqlStore.Scan s = new CqlStore.Scan();
            s.tokLo = p.tokLo;
            s.tokHi = p.tokHi;
            s.tokLoIncl = p.tokLoIncl;
            s.tokHiIncl = p.tokHiIncl;
            s.limit = want;
            s.staticRows = p.staticRows;
            if (pk != null) {
                s.resumeToken = token;
                s.resumePk = pk;
                s.resumeCk = ck;
            }
            List<Rows.RowData> rows = e.timed(h, "scan", () -> shards.conn(h, c -> readRows(c, t, s)));
            if (rows.size() >= want) {
                exhausted[0] = false;
            }
            merged.addAll(rows);
        }
        if (hosts.size() > 1) {
            merged.sort(Comparator.<Rows.RowData>comparingLong(r -> r.token).thenComparing((a, c) -> Arrays.compareUnsigned(a.pk, c.pk))
                    .thenComparing((a, c) -> Arrays.compareUnsigned(a.ck, c.ck)));
        }
        if (merged.size() > want) {
            exhausted[0] = false;
            return new ArrayList<>(merged.subList(0, want));
        }
        return merged;
    }

    // ------------------------------------------------------------------ aggregates

    private byte[][] aggregate(Plan p, List<Rows.RowData> rows) {
        byte[][] out = new byte[p.items.size()][];
        for (int i = 0; i < out.length; i++) {
            Item it = p.items.get(i);
            if (!it.aggregate) {
                if (!rows.isEmpty()) {
                    Object v = it.eval.apply(rows.get(0));
                    out[i] = v == null ? null : it.type.serialize(v);
                }
                continue;
            }
            Object res = null;
            long count = 0;
            BigDecimal sum = BigDecimal.ZERO;
            Object best = null;
            for (Rows.RowData r : rows) {
                Object v;
                if (it.aggArg == null) {
                    count++;
                    continue;
                }
                v = Eval.evalTyped(it.aggArg, null, Eval.Binds.NONE, new RowView(p.t, r)).v();
                if (v == null) {
                    continue;
                }
                count++;
                switch (it.aggFn) {
                    case "sum", "avg" -> sum = sum.add(Eval.toBig(v));
                    case "min" -> best = best == null || it.type.compare(v, best) < 0 ? v : best;
                    case "max" -> best = best == null || it.type.compare(v, best) > 0 ? v : best;
                    default -> {
                    }
                }
            }
            switch (it.aggFn) {
                case "count" -> res = count;
                case "sum" -> res = count == 0 ? Eval.numeric(BigDecimal.ZERO, it.type) : wrap(sum, it.type);
                case "avg" -> res = count == 0 ? Eval.numeric(BigDecimal.ZERO, it.type) : avg(sum, count, it.type);
                default -> res = best;
            }
            out[i] = res == null ? null : it.type.serialize(res);
        }
        return out;
    }

    private static Object wrap(BigDecimal sum, CqlType t) {
        return switch (t.k) {
            case DOUBLE -> sum.doubleValue();
            case FLOAT -> sum.floatValue();
            case DECIMAL -> sum;
            case VARINT -> sum.toBigInteger();
            case BIGINT, COUNTER -> sum.longValue();
            case INT -> sum.intValue();
            case SMALLINT -> (short) sum.intValue();
            default -> (byte) sum.intValue();
        };
    }

    private static Object avg(BigDecimal sum, long n, CqlType t) {
        if (t.k == CqlType.K.DECIMAL) {
            return sum.divide(BigDecimal.valueOf(n), java.math.RoundingMode.HALF_EVEN);
        }
        if (t.k == CqlType.K.DOUBLE || t.k == CqlType.K.FLOAT) {
            return wrap(sum.divide(BigDecimal.valueOf(n), java.math.MathContext.DECIMAL64), t);
        }
        return wrap(new BigDecimal(sum.toBigInteger().divide(BigInteger.valueOf(n))), t);
    }

}
