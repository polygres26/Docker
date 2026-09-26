package com.sayonora.warp.cosmoswire;

import static com.sayonora.warp.cosmoswire.CosmosEval.isTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.cosmoswire.CosmosEval.Scope;
import com.sayonora.warp.cosmoswire.CosmosSql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A compiled Cosmos SQL query: parsing, static analysis (streamability, partition-key / id equality constraints for routing, the
 * facts the query plan endpoint reports) and execution over JSON documents. Execution is a pipeline over the scopes produced by
 * FROM/JOIN and WHERE: streamable queries project each document as it arrives; the others (ORDER BY, GROUP BY, aggregates, DISTINCT,
 * FROM (subquery)) accumulate and finish globally, which is what makes results identical however the documents were gathered from
 * the shards.
 */
final class CosmosQuery {

    interface SubRunner {
        List<JsonElement> run(Query q, Scope parent);
    }

    static final boolean EXCLUDE_UNDEFINED_ORDER = "true".equalsIgnoreCase(System.getenv("WARP_COSMOSWIRE_ORDERBY_EXCLUDE_UNDEFINED"));

    final Query ast;
    final Map<String, JsonElement> params;
    private final CosmosEval ev;
    final List<Call> aggNodes = new ArrayList<>();
    final boolean grouped;
    final boolean ordered;
    final boolean streamable;
    final String rootName;

    private CosmosQuery(Query ast, Map<String, JsonElement> params, long now) {
        this.ast = ast;
        this.params = params;
        this.ev = new CosmosEval(params, now, this::runSub);
        validate(ast);
        for (Item it : ast.items) {
            collectAggs(it.expr(), aggNodes);
        }
        for (Order o : ast.orderBy) {
            collectAggs(o.expr(), aggNodes);
        }
        List<Call> bad = new ArrayList<>();
        if (ast.where != null) {
            collectAggs(ast.where, bad);
        }
        for (Expr g : ast.groupBy) {
            collectAggs(g, bad);
        }
        if (!bad.isEmpty()) {
            throw CosmosException.badRequest("Aggregate functions are not allowed in WHERE or GROUP BY.");
        }
        this.grouped = !aggNodes.isEmpty() || !ast.groupBy.isEmpty();
        this.ordered = !ast.orderBy.isEmpty();
        this.streamable = !grouped && !ordered && !ast.distinct && (ast.from.isEmpty() || ast.from.get(0).sub() == null);
        this.rootName = ast.from.isEmpty() || ast.from.get(0).sub() != null ? null : rootIdent(ast.from.get(0).expr());
    }

    static CosmosQuery compile(String sql, JsonArray parameters) {
        return compile(sql, parameters, System.currentTimeMillis());
    }

    static CosmosQuery compile(String sql, JsonArray parameters, long nowMillis) {
        if (sql == null || sql.isBlank()) {
            throw CosmosException.badRequest("The query text is empty.");
        }
        Query ast = CosmosSql.parse(sql);
        Map<String, JsonElement> ps = new LinkedHashMap<>();
        if (parameters != null) {
            for (JsonElement p : parameters) {
                if (!p.isJsonObject() || !p.getAsJsonObject().has("name")) {
                    throw CosmosException.badRequest("Each query parameter needs a name.");
                }
                String n = p.getAsJsonObject().get("name").getAsString();
                if (!n.startsWith("@")) {
                    throw CosmosException.badRequest("Query parameter names must start with '@': " + n);
                }
                ps.put(n, p.getAsJsonObject().get("value"));
            }
        }
        return new CosmosQuery(ast, ps, nowMillis);
    }

    private static void validate(Query q) {
        if (q.star && q.from.size() > 1) {
            throw CosmosException.badRequest("'SELECT *' is only valid with a single input set.");
        }
        if (q.distinct && q.value && false) {
            throw CosmosException.badRequest("unreachable");
        }
    }

    static String rootIdent(Expr e) {
        while (true) {
            if (e instanceof Ident i) {
                return i.name();
            } else if (e instanceof Prop p) {
                e = p.base();
            } else if (e instanceof Index x) {
                e = x.base();
            } else {
                return null;
            }
        }
    }

    // ------------------------------------------------------------------------------------------ static analysis

    private static void collectAggs(Expr e, List<Call> out) {
        if (e == null) {
            return;
        }
        if (e instanceof Call c) {
            if (CosmosSql.AGGREGATES.contains(c.name().toUpperCase(java.util.Locale.ROOT))) {
                if (c.args().size() != 1) {
                    throw CosmosException.badRequest("The " + c.name().toLowerCase(java.util.Locale.ROOT) + " function requires 1 argument(s).");
                }
                out.add(c);
                return;
            }
            c.args().forEach(a -> collectAggs(a, out));
        } else if (e instanceof Prop p) {
            collectAggs(p.base(), out);
        } else if (e instanceof Index x) {
            collectAggs(x.base(), out);
            collectAggs(x.index(), out);
        } else if (e instanceof Unary u) {
            collectAggs(u.e(), out);
        } else if (e instanceof Binary b) {
            collectAggs(b.l(), out);
            collectAggs(b.r(), out);
        } else if (e instanceof Ternary t) {
            collectAggs(t.cond(), out);
            collectAggs(t.a(), out);
            collectAggs(t.b(), out);
        } else if (e instanceof InList in) {
            collectAggs(in.e(), out);
            in.list().forEach(a -> collectAggs(a, out));
        } else if (e instanceof Between b) {
            collectAggs(b.e(), out);
            collectAggs(b.lo(), out);
            collectAggs(b.hi(), out);
        } else if (e instanceof Like l) {
            collectAggs(l.e(), out);
            collectAggs(l.pattern(), out);
        } else if (e instanceof ArrayLit a) {
            a.items().forEach(x -> collectAggs(x, out));
        } else if (e instanceof ObjLit o) {
            o.fields().values().forEach(x -> collectAggs(x, out));
        }
        // Sub: its aggregates belong to the subquery
    }

    /**
     * Equality constraints {@code alias.path = literal} found in the top-level AND-conjuncts of WHERE (keyed by {@code /a/b}); used to
     * route to one partition / one id. Empty when the query does not read the container root directly.
     */
    Map<String, JsonElement> equalities() {
        Map<String, JsonElement> out = new LinkedHashMap<>();
        if (ast.from.isEmpty() || ast.from.get(0).sub() != null || ast.from.get(0).iterate() || ast.where == null) {
            return out;
        }
        Source s = ast.from.get(0);
        if (!(s.expr() instanceof Ident)) {
            return out;
        }
        conj(ast.where, s.alias(), out);
        return out;
    }

    private void conj(Expr e, String alias, Map<String, JsonElement> out) {
        if (e instanceof Binary b && b.op().equals("AND")) {
            conj(b.l(), alias, out);
            conj(b.r(), alias, out);
        } else if (e instanceof Binary b && b.op().equals("=")) {
            eq(b.l(), b.r(), alias, out);
            eq(b.r(), b.l(), alias, out);
        }
    }

    private void eq(Expr path, Expr val, String alias, Map<String, JsonElement> out) {
        StringBuilder p = new StringBuilder();
        Expr cur = path;
        List<String> segs = new ArrayList<>();
        while (cur instanceof Prop pr) {
            segs.add(0, pr.name());
            cur = pr.base();
        }
        if (!(cur instanceof Ident i) || !i.name().equals(alias) || segs.isEmpty()) {
            return;
        }
        JsonElement v;
        if (val instanceof Lit l) {
            v = l.value();
        } else if (val instanceof Param pm && params.containsKey(pm.name())) {
            v = params.get(pm.name());
        } else {
            return;
        }
        if (v == null || v.isJsonObject() || v.isJsonArray()) {
            return;
        }
        for (String sg : segs) {
            p.append('/').append(sg);
        }
        out.putIfAbsent(p.toString(), v);
    }

    /** True for {@code SELECT VALUE COUNT(1) FROM c} (no filter): answerable with a row count. */
    boolean isPlainCount() {
        if (!ast.value || ast.where != null || grouped && ast.groupBy.size() > 0 || ast.distinct || ast.top != null || ast.offset != null
                || ast.from.size() != 1 || ast.from.get(0).sub() != null || ast.from.get(0).iterate() || !(ast.from.get(0).expr() instanceof Ident)
                || aggNodes.size() != 1 || !ast.orderBy.isEmpty()) {
            return false;
        }
        Expr item = ast.items.get(0).expr();
        return item instanceof Call c && c.name().equalsIgnoreCase("COUNT") && c.args().get(0) instanceof Lit l && CosmosJson.isNum(l.value());
    }

    long topValue() {
        return ast.top == null ? -1 : constInt(ast.top, "TOP");
    }

    long offsetValue() {
        return ast.offset == null ? 0 : constInt(ast.offset, "OFFSET");
    }

    long limitValue() {
        return ast.limit == null ? -1 : constInt(ast.limit, "LIMIT");
    }

    private long constInt(Expr e, String what) {
        JsonElement v = ev.eval(e, new Scope(null, "$", null));
        if (!CosmosJson.isInteger(v) || CosmosJson.dbl(v) < 0) {
            throw CosmosException.badRequest(what + " must be a non-negative integer.");
        }
        return (long) CosmosJson.dbl(v);
    }

    // ------------------------------------------------------------------------------------------ execution

    /** Rows of one document (streamable queries): FROM/JOIN, WHERE, projection. Each row goes to {@code sink}. */
    void streamDoc(JsonElement doc, Consumer<JsonElement> sink) {
        scopes(ast, doc, null, null, s -> {
            JsonElement row = project(ast, s);
            if (row != null) {
                sink.accept(row);
            }
        });
    }

    /** Runs the query over every document (materializing). */
    List<JsonElement> runAll(Iterable<JsonElement> docs) {
        Pipeline p = pipeline();
        for (JsonElement d : docs) {
            p.add(d);
        }
        return p.finish();
    }

    Pipeline pipeline() {
        return new Pipeline(this, ast, null, true);
    }

    private List<JsonElement> runSub(Query q, Scope parent) {
        Pipeline p = new Pipeline(this, q, parent, false);
        p.addScopeless();
        return p.finish();
    }

    /** Expands FROM/JOIN for one document ({@code doc} non-null: top-level) or from a parent scope, applying WHERE. */
    private void scopes(Query q, JsonElement doc, Scope parent, Iterable<JsonElement> firstRows, Consumer<Scope> sink) {
        Scope base = parent == null ? new Scope(null, "$", null) : parent;
        if (q.from.isEmpty()) {
            if (q.where == null || isTrue(ev.eval(q.where, base))) {
                sink.accept(base);
            }
            return;
        }
        expand(q, 0, base, doc, firstRows, sink);
    }

    private void expand(Query q, int i, Scope cur, JsonElement doc, Iterable<JsonElement> firstRows, Consumer<Scope> sink) {
        if (i == q.from.size()) {
            if (q.where == null || isTrue(ev.eval(q.where, cur))) {
                sink.accept(cur);
            }
            return;
        }
        Source src = q.from.get(i);
        Iterable<JsonElement> values;
        if (src.sub() != null) {
            values = i == 0 && firstRows != null ? firstRows : runSub(src.sub(), cur);
        } else {
            Scope es = cur;
            if (i == 0 && doc != null) {
                String root = rootIdent(src.expr());
                es = root == null ? cur : cur.bind(root, doc);
            }
            JsonElement v = ev.eval(src.expr(), es);
            if (src.iterate()) {
                values = v != null && v.isJsonArray() ? v.getAsJsonArray() : List.of();
            } else {
                values = v == null ? List.of() : List.of(v);
            }
        }
        for (JsonElement v : values) {
            expand(q, i + 1, cur.bind(src.alias(), v), doc, firstRows, sink);
        }
    }

    /** SELECT projection of one scope; null when the row is undefined (SELECT VALUE of an undefined expression). */
    private JsonElement project(Query q, Scope s) {
        if (q.star) {
            return s.get(q.from.get(0).alias());
        }
        if (q.value) {
            return ev.eval(q.items.get(0).expr(), s);
        }
        JsonObject o = new JsonObject();
        int unnamed = 0;
        for (Item it : q.items) {
            String name = it.alias();
            if (name == null) {
                name = derivedName(it.expr());
                if (name == null) {
                    name = "$" + (++unnamed);
                }
            }
            JsonElement v = ev.eval(it.expr(), s);
            if (v != null) {
                o.add(name, v);
            }
        }
        return o;
    }

    private static String derivedName(Expr e) {
        if (e instanceof Prop p) {
            return p.name();
        }
        if (e instanceof Ident i) {
            return i.name();
        }
        return null;
    }

    // ------------------------------------------------------------------------------------------ pipeline

    private static final class Acc {
        final Call call;
        final String kind;
        long count;
        double sum;
        long numeric;
        boolean bad;
        JsonElement best;

        Acc(Call call) {
            this.call = call;
            this.kind = call.name().toUpperCase(java.util.Locale.ROOT);
        }

        void add(JsonElement v) {
            if (v == null) {
                return;
            }
            switch (kind) {
                case "COUNT" -> count++;
                case "SUM", "AVG" -> {
                    if (CosmosJson.isNum(v)) {
                        sum += CosmosJson.dbl(v);
                        numeric++;
                    } else {
                        bad = true;
                    }
                }
                case "MIN" -> {
                    if (best == null || CosmosJson.compare(v, best) < 0) {
                        best = v;
                    }
                }
                default -> {
                    if (best == null || CosmosJson.compare(v, best) > 0) {
                        best = v;
                    }
                }
            }
        }

        JsonElement result() {
            return switch (kind) {
                case "COUNT" -> CosmosJson.num(count);
                case "SUM" -> bad || numeric == 0 ? null : CosmosJson.num(sum);
                case "AVG" -> bad || numeric == 0 ? null : CosmosJson.num(sum / numeric);
                default -> best;
            };
        }
    }

    private static final class Group {
        final Scope rep;
        final Acc[] accs;

        Group(Scope rep, List<Call> nodes) {
            this.rep = rep;
            this.accs = new Acc[nodes.size()];
            for (int i = 0; i < accs.length; i++) {
                accs[i] = new Acc(nodes.get(i));
            }
        }
    }

    private record SortRow(JsonElement[] keys, JsonElement value) {
    }

    final class Pipeline {
        private final CosmosQuery owner;
        private final Query q;
        private final Scope parent;
        private final boolean top;
        private final List<Call> aggs = new ArrayList<>();
        private final boolean grp;
        private final boolean ord;
        private final Map<String, Group> groups = new LinkedHashMap<>();
        private final List<SortRow> rows = new ArrayList<>();
        private List<JsonElement> firstRows;
        private CosmosQuery inner;
        private final List<JsonElement> innerInput = new ArrayList<>();

        Pipeline(CosmosQuery owner, Query q, Scope parent, boolean top) {
            this.owner = owner;
            this.q = q;
            this.parent = parent;
            this.top = top;
            for (Item it : q.items) {
                collectAggs(it.expr(), aggs);
            }
            for (Order o : q.orderBy) {
                collectAggs(o.expr(), aggs);
            }
            this.grp = !aggs.isEmpty() || !q.groupBy.isEmpty();
            this.ord = !q.orderBy.isEmpty();
            if (top && !q.from.isEmpty() && q.from.get(0).sub() != null) {
                this.inner = new CosmosQuery(q.from.get(0).sub(), params, ev.nowMillis);
            }
        }

        /** Feeds one container document (top-level pipelines). */
        void add(JsonElement doc) {
            if (inner != null) {
                innerInput.add(doc);
                return;
            }
            scopes(q, doc, null, null, this::accept);
        }

        /** Runs a subquery pipeline (scopes come from the parent scope). */
        void addScopeless() {
            scopes(q, null, parent, null, this::accept);
        }

        private void accept(Scope s) {
            if (grp) {
                StringBuilder key = new StringBuilder();
                for (Expr g : q.groupBy) {
                    key.append(CosmosJson.canon(ev.eval(g, s))).append('\u0001');
                }
                Group g = groups.computeIfAbsent(key.toString(), k -> new Group(s, aggs));
                for (Acc a : g.accs) {
                    Expr arg = a.call.args().get(0);
                    a.add(ev.eval(arg, s));
                }
                return;
            }
            JsonElement v = project(q, s);
            if (v == null) {
                return;
            }
            rows.add(new SortRow(ord ? orderKeys(s) : null, v));
        }

        private JsonElement[] orderKeys(Scope s) {
            JsonElement[] k = new JsonElement[q.orderBy.size()];
            for (int i = 0; i < k.length; i++) {
                k[i] = ev.eval(q.orderBy.get(i).expr(), s);
            }
            return k;
        }

        List<JsonElement> finish() {
            if (inner != null) {
                List<JsonElement> innerRows = inner.runAll(innerInput);
                scopes(q, null, null, innerRows, this::accept);
            }
            if (grp) {
                if (groups.isEmpty() && q.groupBy.isEmpty()) {
                    groups.put("", new Group(parent == null ? new Scope(null, "$", null) : parent, aggs));
                }
                for (Group g : groups.values()) {
                    IdentityHashMap<Call, JsonElement> m = new IdentityHashMap<>();
                    for (Acc a : g.accs) {
                        m.put(a.call, a.result());
                    }
                    Scope gs = g.rep.withAggs(m);
                    JsonElement v = project(q, gs);
                    if (v != null) {
                        rows.add(new SortRow(ord ? orderKeys(gs) : null, v));
                    }
                }
            }
            List<SortRow> rs = rows;
            if (ord) {
                if (EXCLUDE_UNDEFINED_ORDER) {
                    rs = new ArrayList<>();
                    for (SortRow r : rows) {
                        boolean ok = true;
                        for (JsonElement k : r.keys()) {
                            ok &= k != null;
                        }
                        if (ok) {
                            rs.add(r);
                        }
                    }
                }
                Comparator<SortRow> cmp = (a, b) -> {
                    for (int i = 0; i < a.keys().length; i++) {
                        int c = CosmosJson.compare(a.keys()[i], b.keys()[i]);
                        if (c != 0) {
                            return q.orderBy.get(i).desc() ? -c : c;
                        }
                    }
                    return 0;
                };
                rs.sort(cmp);
            }
            List<JsonElement> out = new ArrayList<>(rs.size());
            Set<String> seen = q.distinct ? new HashSet<>() : null;
            long topN = q.top == null ? -1 : constInt(q.top, "TOP");
            long off = q.offset == null ? 0 : constInt(q.offset, "OFFSET");
            long lim = q.limit == null ? -1 : constInt(q.limit, "LIMIT");
            long skipped = 0;
            for (SortRow r : rs) {
                if (seen != null && !seen.add(CosmosJson.canon(r.value()))) {
                    continue;
                }
                if (skipped < off) {
                    skipped++;
                    continue;
                }
                if (lim >= 0 && out.size() >= lim || topN >= 0 && out.size() >= topN) {
                    break;
                }
                out.add(r.value());
            }
            return out;
        }
    }
}
