package com.sayonora.warp.boltwire;

import com.sayonora.warp.boltwire.Cy.*;
import com.sayonora.warp.boltwire.Values.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Runs an analysed Cypher {@link Query} clause by clause over materialised row sets. */
final class Executor {

    /** The rows of a finished query. */
    record Result(List<String> columns, List<List<Object>> rows) {
    }

    final Exec x;
    final Eval ev;
    private final Matcher matcher;
    private final Schema schema;
    private final Analyzer.Info info;

    Executor(Exec x, Analyzer.Info info) {
        this.x = x;
        this.info = info;
        this.ev = new Eval(x, this);
        this.matcher = new Matcher(ev);
        this.schema = new Schema(x);
    }

    // ------------------------------------------------------------------------------------------ top level

    Result run(Query q) {
        Result r = runQuery(q, List.of(new HashMap<>()));
        x.finish();
        return r;
    }

    Result runQuery(Query q, List<Map<String, Object>> input) {
        if (q.parts().size() == 1) {
            return runPart(q.parts().get(0), input);
        }
        List<String> cols = null;
        List<List<Object>> rows = new ArrayList<>();
        boolean allUnionAll = true;
        for (boolean a : q.all()) {
            allUnionAll &= a;
        }
        for (List<Clause> part : q.parts()) {
            Result r = runPart(part, input);
            if (cols == null) {
                cols = r.columns();
            }
            rows.addAll(r.rows());
        }
        if (!allUnionAll) {
            // UNION (distinct) anywhere: a mixed chain is rejected by the analyzer, so all are DISTINCT
            rows = distinctRows(rows);
        }
        return new Result(cols, rows);
    }

    private static List<List<Object>> distinctRows(List<List<Object>> rows) {
        Set<Object> seen = new java.util.HashSet<>();
        List<List<Object>> out = new ArrayList<>();
        for (List<Object> r : rows) {
            List<Object> key = new ArrayList<>(r.size());
            for (Object o : r) {
                key.add(Values.groupKey(o));
            }
            if (seen.add(key)) {
                out.add(r);
            }
        }
        return out;
    }

    Result runPart(List<Clause> clauses, List<Map<String, Object>> input) {
        List<Map<String, Object>> rows = input;
        for (Clause c : clauses) {
            x.checkDeadline();
            switch (c) {
                case Match m -> rows = match(m, rows);
                case Unwind u -> rows = unwind(u, rows);
                case With w -> rows = with(w, rows);
                case Return r -> {
                    ProjResult pr = project(r.proj(), rows, true, false);
                    List<List<Object>> out = new ArrayList<>(pr.rows.size());
                    for (Map<String, Object> row : pr.rows) {
                        List<Object> vals = new ArrayList<>(pr.columns.size());
                        for (String col : pr.columns) {
                            vals.add(row.get(col));
                        }
                        out.add(vals);
                    }
                    return new Result(pr.columns, out);
                }
                case Create cr -> rows = create(cr, rows);
                case Merge mg -> rows = merge(mg, rows);
                case SetClause s -> rows = set(s.items(), rows);
                case Remove rm -> rows = remove(rm, rows);
                case Delete d -> rows = delete(d, rows);
                case CallClause cc -> rows = call(cc, rows);
                case Foreach f -> rows = foreach(f, rows);
                case CallSubquery cs -> rows = callSubquery(cs, rows);
                case SchemaCmd sc -> {
                    return schema.run(sc, this);
                }
            }
        }
        return new Result(List.of(), List.of());
    }

    // ------------------------------------------------------------------------------------------ MATCH / UNWIND

    private List<Map<String, Object>> match(Match m, List<Map<String, Object>> in) {
        List<Map<String, Object>> out = new ArrayList<>();
        Set<String> newVars = m.optional() ? patternVars(m.parts()) : Set.of();
        for (Map<String, Object> row : in) {
            List<Map<String, Object>> found = matcher.matchAll(m.parts(), row, m.where());
            if (found.isEmpty() && m.optional()) {
                Map<String, Object> r = new HashMap<>(row);
                for (String v : newVars) {
                    r.putIfAbsent(v, null);
                }
                out.add(r);
            } else {
                out.addAll(found);
            }
        }
        return out;
    }

    static Set<String> patternVars(List<PatternPart> parts) {
        Set<String> vars = new LinkedHashSet<>();
        for (PatternPart p : parts) {
            if (p.pathVar() != null) {
                vars.add(p.pathVar());
            }
            for (int i = 0; i <= p.relCount(); i++) {
                if (p.node(i).var() != null) {
                    vars.add(p.node(i).var());
                }
                if (i < p.relCount() && p.rel(i).var() != null) {
                    vars.add(p.rel(i).var());
                }
            }
        }
        return vars;
    }

    private List<Map<String, Object>> unwind(Unwind u, List<Map<String, Object>> in) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> row : in) {
            Object v = ev.eval(u.expr(), row);
            if (v == null) {
                continue;
            }
            List<?> l = v instanceof List<?> ll ? ll : List.of(v);
            for (Object o : l) {
                Map<String, Object> r = new HashMap<>(row);
                r.put(u.var(), o);
                out.add(r);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------------------------------ projection

    private static final class ProjResult {
        List<String> columns;
        List<Map<String, Object>> rows;
        /** Per output row: the input row overlaid with the projection (only for WITH ... WHERE, non-aggregating). */
        List<Map<String, Object>> envs;
    }

    private List<Map<String, Object>> with(With w, List<Map<String, Object>> in) {
        ProjResult pr = project(w.proj(), in, false, w.where() != null);
        List<Map<String, Object>> rows = pr.rows;
        if (w.where() != null) {
            List<Map<String, Object>> out = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> env = pr.envs != null ? pr.envs.get(i) : rows.get(i);
                if (ev.isTrue(w.where(), env)) {
                    out.add(rows.get(i));
                }
            }
            rows = out;
        }
        return rows;
    }

    private static String columnName(ProjItem it) {
        return it.alias() != null ? it.alias() : (it.expr() instanceof Var v ? v.name() : it.text());
    }

    private ProjResult project(Projection p, List<Map<String, Object>> in, boolean isReturn, boolean needEnv) {
        // columns
        List<String> columns = new ArrayList<>();
        List<Expr> exprs = new ArrayList<>();
        if (p.star()) {
            List<String> vars = info.star.get(p);
            if (vars == null) {
                vars = new ArrayList<>(new java.util.TreeSet<>(in.isEmpty() ? java.util.Set.<String>of() : in.get(0).keySet()));
            }
            for (String v : vars) {
                columns.add(v);
                exprs.add(new Var(v, 0));
            }
        }
        for (ProjItem it : p.items()) {
            columns.add(columnName(it));
            exprs.add(it.expr());
        }
        boolean aggregating = false;
        List<Boolean> isAgg = new ArrayList<>();
        for (Expr e : exprs) {
            boolean a = containsAggregate(e);
            isAgg.add(a);
            aggregating |= a;
        }
        List<Map<String, Object>> outRows = new ArrayList<>();
        List<Map<String, Object>> sortEnv = new ArrayList<>(); // per output row: env for ORDER BY (input row overlaid)
        List<java.util.HashMap<String, Object>> sortAgg = new ArrayList<>();

        List<Expr> orderExprs = new ArrayList<>();
        for (SortItem s : p.orderBy()) {
            orderExprs.add(s.expr());
        }

        if (!aggregating) {
            for (Map<String, Object> row : in) {
                x.checkDeadline();
                Map<String, Object> out = new LinkedHashMap<>();
                for (int i = 0; i < exprs.size(); i++) {
                    out.put(columns.get(i), ev.eval(exprs.get(i), row));
                }
                outRows.add(out);
                if (!orderExprs.isEmpty() || needEnv) {
                    Map<String, Object> env = new HashMap<>(row);
                    env.putAll(out);
                    sortEnv.add(env);
                }
            }
        } else {
            // collect aggregate calls from items and order-by
            List<Expr> aggCalls = new ArrayList<>();
            for (Expr e : exprs) {
                collectAggregates(e, aggCalls);
            }
            for (Expr e : orderExprs) {
                collectAggregates(e, aggCalls);
            }
            List<Integer> keyIdx = new ArrayList<>();
            for (int i = 0; i < exprs.size(); i++) {
                if (!isAgg.get(i)) {
                    keyIdx.add(i);
                }
            }
            record Group(List<Object> keyVals, Map<String, Object> firstRow, List<Funcs.Agg> aggs) {
            }
            Map<Object, Group> groups = new LinkedHashMap<>();
            for (Map<String, Object> row : in) {
                x.checkDeadline();
                List<Object> keyVals = new ArrayList<>(keyIdx.size());
                List<Object> gk = new ArrayList<>(keyIdx.size());
                for (int i : keyIdx) {
                    Object v = ev.eval(exprs.get(i), row);
                    keyVals.add(v);
                    gk.add(Values.groupKey(v));
                }
                Group g = groups.get(gk);
                if (g == null) {
                    List<Funcs.Agg> aggs = new ArrayList<>();
                    for (Expr c : aggCalls) {
                        aggs.add(newAgg(c));
                    }
                    g = new Group(keyVals, row, aggs);
                    groups.put(gk, g);
                }
                for (int k = 0; k < aggCalls.size(); k++) {
                    Expr c = aggCalls.get(k);
                    if (c instanceof CountStar) {
                        g.aggs.get(k).count++;
                    } else {
                        Call call = (Call) c;
                        Object extra = call.args().size() > 1 ? ev.eval(call.args().get(1), row) : null;
                        g.aggs.get(k).add(ev.eval(call.args().get(0), row), extra);
                    }
                }
            }
            if (groups.isEmpty() && keyIdx.isEmpty()) {
                List<Funcs.Agg> aggs = new ArrayList<>();
                for (Expr c : aggCalls) {
                    aggs.add(newAgg(c));
                }
                groups.put(List.of(), new Group(List.of(), new HashMap<>(), aggs));
            }
            for (Group g : groups.values()) {
                java.util.HashMap<String, Object> res = new java.util.HashMap<>();
                for (int k = 0; k < aggCalls.size(); k++) {
                    res.put(Analyzer.norm(aggCalls.get(k)), g.aggs.get(k).result());
                }
                Map<String, Object> keyRow = new HashMap<>(g.firstRow);
                Map<String, Object> out = new LinkedHashMap<>();
                int ki = 0;
                java.util.HashMap<String, Object> prev = ev.aggResults;
                ev.aggResults = res;
                try {
                    for (int i = 0; i < exprs.size(); i++) {
                        Object v = isAgg.get(i) ? ev.eval(exprs.get(i), keyRow) : g.keyVals.get(ki++);
                        out.put(columns.get(i), v);
                    }
                } finally {
                    ev.aggResults = prev;
                }
                outRows.add(out);
                if (!orderExprs.isEmpty()) {
                    Map<String, Object> env = new HashMap<>(g.firstRow);
                    env.putAll(out);
                    sortEnv.add(env);
                    sortAgg.add(res);
                }
            }
        }

        // DISTINCT
        if (p.distinct()) {
            Set<Object> seen = new java.util.HashSet<>();
            List<Map<String, Object>> d = new ArrayList<>();
            List<Map<String, Object>> denv = new ArrayList<>();
            List<java.util.HashMap<String, Object>> dagg = new ArrayList<>();
            for (int i = 0; i < outRows.size(); i++) {
                List<Object> key = new ArrayList<>();
                for (String col : columns) {
                    key.add(Values.groupKey(outRows.get(i).get(col)));
                }
                if (seen.add(key)) {
                    d.add(outRows.get(i));
                    if (!sortEnv.isEmpty()) {
                        denv.add(sortEnv.get(i));
                    }
                    if (!sortAgg.isEmpty()) {
                        dagg.add(sortAgg.get(i));
                    }
                }
            }
            outRows = d;
            sortEnv = denv;
            sortAgg = dagg;
        }

        // ORDER BY
        if (!orderExprs.isEmpty()) {
            List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < outRows.size(); i++) {
                idx.add(i);
            }
            List<List<Object>> keys = new ArrayList<>();
            for (int i = 0; i < outRows.size(); i++) {
                List<Object> k = new ArrayList<>();
                java.util.HashMap<String, Object> prev = ev.aggResults;
                if (!sortAgg.isEmpty()) {
                    ev.aggResults = sortAgg.get(i);
                }
                try {
                    Map<String, Object> env = sortEnv.get(i);
                    for (Expr oe : orderExprs) {
                        k.add(ev.eval(oe, env));
                    }
                } finally {
                    ev.aggResults = prev;
                }
                keys.add(k);
            }
            idx.sort((a, b) -> {
                for (int j = 0; j < orderExprs.size(); j++) {
                    int c = Values.order(keys.get(a).get(j), keys.get(b).get(j));
                    if (p.orderBy().get(j).desc()) {
                        c = -c;
                    }
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            });
            List<Map<String, Object>> sorted = new ArrayList<>(outRows.size());
            List<Map<String, Object>> sortedEnv = new ArrayList<>(outRows.size());
            for (int i : idx) {
                sorted.add(outRows.get(i));
                sortedEnv.add(sortEnv.get(i));
            }
            outRows = sorted;
            sortEnv = sortedEnv;
        }

        // SKIP / LIMIT
        if (p.skip() != null || p.limit() != null) {
            long skip = p.skip() == null ? 0 : nonNegative(ev.eval(p.skip(), Map.of()), "SKIP");
            long limit = p.limit() == null ? Long.MAX_VALUE : nonNegative(ev.eval(p.limit(), Map.of()), "LIMIT");
            int from = (int) Math.min(skip, outRows.size());
            int to = limit >= outRows.size() - from ? outRows.size() : (int) (from + limit);
            outRows = new ArrayList<>(outRows.subList(from, to));
            if (!sortEnv.isEmpty()) {
                sortEnv = new ArrayList<>(sortEnv.subList(from, to));
            }
        }
        ProjResult pr = new ProjResult();
        pr.columns = columns;
        pr.rows = outRows;
        if (needEnv && !aggregating && sortEnv.size() == outRows.size()) {
            pr.envs = sortEnv;
        }
        return pr;
    }

    private long nonNegative(Object v, String what) {
        if (v == null) {
            throw CypherException.syntax("Invalid input for " + what + ": expected a non-negative integer");
        }
        if (v instanceof Double d) {
            throw CypherException.argument("It is not allowed to use floating point numbers for " + what + " (got " + d + ")");
        }
        if (!(v instanceof Long l)) {
            throw CypherException.syntax("Type mismatch: expected Integer but was " + Values.typeName(v));
        }
        if (l < 0) {
            throw new CypherException(CypherException.ARGUMENT,
                    "Invalid input. '" + l + "' is not a valid value. Must be a non-negative integer.");
        }
        return l;
    }

    static boolean containsAggregate(Expr e) {
        return Analyzer.walkAny(e, n -> n instanceof CountStar || (n instanceof Call c && Funcs.isAggregate(c.name())));
    }

    private static void collectAggregates(Expr e, List<Expr> out) {
        Analyzer.walk(e, n -> {
            if (n instanceof CountStar || (n instanceof Call c && Funcs.isAggregate(c.name()))) {
                String key = Analyzer.norm(n);
                boolean present = false;
                for (Expr o : out) {
                    if (Analyzer.norm(o).equals(key)) {
                        present = true;
                        break;
                    }
                }
                if (!present) {
                    out.add(n);
                }
                return false;
            }
            return true;
        });
    }

    private Funcs.Agg newAgg(Expr c) {
        if (c instanceof CountStar) {
            return new Funcs.Agg("count", false);
        }
        Call call = (Call) c;
        return new Funcs.Agg(call.name(), call.distinct());
    }

    // ------------------------------------------------------------------------------------------ CREATE / MERGE

    private List<Map<String, Object>> create(Create c, List<Map<String, Object>> in) {
        List<Map<String, Object>> out = new ArrayList<>(in.size());
        for (Map<String, Object> row : in) {
            Map<String, Object> r = new HashMap<>(row);
            for (PatternPart part : c.parts()) {
                createPart(part, r);
            }
            out.add(r);
        }
        return out;
    }

    private Map<String, Object> propsOf(Expr props, Map<String, Object> row) {
        if (props == null) {
            return new LinkedHashMap<>();
        }
        Object v = ev.eval(props, row);
        if (v == null) {
            return new LinkedHashMap<>();
        }
        if (v instanceof NodeV n) {
            return new LinkedHashMap<>(n.props);
        }
        if (v instanceof RelV r) {
            return new LinkedHashMap<>(r.props);
        }
        if (!(v instanceof Map<?, ?> m)) {
            throw Eval.typeMismatch("Map", v);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : m.entrySet()) {
            out.put((String) e.getKey(), e.getValue());
        }
        return out;
    }

    private NodeV nodeFor(NodePat np, Map<String, Object> row) {
        if (np.var() != null && row.containsKey(np.var())) {
            Object v = row.get(np.var());
            if (v == null) {
                return null;
            }
            if (!(v instanceof NodeV n)) {
                throw Eval.typeMismatch("Node", v);
            }
            return n;
        }
        NodeV n = x.createNode(np.labels(), propsOf(np.props(), row));
        if (np.var() != null) {
            row.put(np.var(), n);
        }
        return n;
    }

    private void createPart(PatternPart part, Map<String, Object> row) {
        List<NodeV> nodes = new ArrayList<>();
        List<RelV> rels = new ArrayList<>();
        NodeV prev = nodeFor(part.node(0), row);
        nodes.add(prev);
        for (int i = 0; i < part.relCount(); i++) {
            RelPat rp = part.rel(i);
            NodeV next = nodeFor(part.node(i + 1), row);
            if (prev == null || next == null) {
                String missing = prev == null ? part.node(i).var() : part.node(i + 1).var();
                throw new CypherException(CypherException.SEMANTIC,
                        "Failed to create relationship" + (rp.var() != null ? " `" + rp.var() + "`" : "") + ", node `"
                                + missing + "` is missing. If you prefer to simply ignore rows where a relationship node is "
                                + "missing, set 'cypher.lenient_create_relationship = true' in neo4j.conf");
            }
            String type = rp.types().get(0);
            NodeV from = rp.dir() == Dir.IN ? next : prev, to = rp.dir() == Dir.IN ? prev : next;
            RelV r = x.createRel(type, from, to, propsOf(rp.props(), row));
            if (rp.var() != null) {
                row.put(rp.var(), r);
            }
            rels.add(r);
            nodes.add(next);
            prev = next;
        }
        if (part.pathVar() != null) {
            row.put(part.pathVar(), new PathV(nodes, rels));
        }
    }

    private List<Map<String, Object>> merge(Merge m, List<Map<String, Object>> in) {
        List<Map<String, Object>> out = new ArrayList<>();
        PatternPart part = m.part();
        for (Map<String, Object> row : in) {
            // null property values in a MERGE pattern are an error
            for (int i = 0; i <= part.relCount(); i++) {
                checkNoNullProps(part.node(i).props(), row, "node", part.node(i).var());
                if (i < part.relCount()) {
                    checkNoNullProps(part.rel(i).props(), row, "relationship", part.rel(i).var());
                }
            }
            List<Map<String, Object>> found = matcher.matchAll(List.of(part), row, null);
            if (!found.isEmpty()) {
                for (Map<String, Object> f : found) {
                    if (!m.onMatch().isEmpty()) {
                        applySet(m.onMatch(), f);
                    }
                    out.add(f);
                }
            } else {
                Map<String, Object> r = new HashMap<>(row);
                createPart(part, r);
                if (!m.onCreate().isEmpty()) {
                    applySet(m.onCreate(), r);
                }
                out.add(r);
            }
        }
        return out;
    }

    private void checkNoNullProps(Expr props, Map<String, Object> row, String what, String var) {
        if (props == null) {
            return;
        }
        Object v = ev.eval(props, row);
        if (v instanceof Map<?, ?> mm) {
            for (Map.Entry<?, ?> e : mm.entrySet()) {
                if (e.getValue() == null) {
                    throw new CypherException(CypherException.SEMANTIC,
                            "Cannot merge the following " + what + " because of null property value for '" + e.getKey()
                                    + "': (" + (var == null ? "" : var) + ")");
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------ SET / REMOVE / DELETE

    private List<Map<String, Object>> set(List<SetItem> items, List<Map<String, Object>> in) {
        for (Map<String, Object> row : in) {
            applySet(items, row);
        }
        return in;
    }

    private void applySet(List<SetItem> items, Map<String, Object> row) {
        // every right-hand side is evaluated before any item is applied
        Object[] targets = new Object[items.size()], values = new Object[items.size()];
        for (int i = 0; i < items.size(); i++) {
            SetItem it = items.get(i);
            switch (it.kind()) {
                case "PROP" -> {
                    targets[i] = ev.eval(((Prop) it.target()).target(), row);
                    values[i] = ev.eval(it.value(), row);
                }
                case "REPLACE", "MERGE" -> {
                    targets[i] = ev.eval(it.target(), row);
                    Object v = ev.eval(it.value(), row);
                    if (v instanceof NodeV n) {
                        v = new LinkedHashMap<>(n.props);
                    } else if (v instanceof RelV r) {
                        v = new LinkedHashMap<>(r.props);
                    }
                    values[i] = v;
                }
                default -> targets[i] = ev.eval(it.target(), row);
            }
        }
        for (int i = 0; i < items.size(); i++) {
            SetItem it = items.get(i);
            Object target = targets[i];
            if (target == null) {
                continue;
            }
            switch (it.kind()) {
                case "PROP" -> {
                    checkEntity(target);
                    x.setProp(target, ((Prop) it.target()).key(), values[i]);
                }
                case "REPLACE", "MERGE" -> {
                    checkEntity(target);
                    Object value = values[i];
                    Map<String, Object> m;
                    if (value instanceof Map<?, ?> mm) {
                        m = new LinkedHashMap<>();
                        for (Map.Entry<?, ?> e : mm.entrySet()) {
                            m.put((String) e.getKey(), e.getValue());
                        }
                    } else if (value == null && it.kind().equals("MERGE")) {
                        continue;
                    } else {
                        throw new CypherException(CypherException.TYPE, "Type mismatch: expected Map, Node or Relationship but was "
                                + Values.typeName(value));
                    }
                    if (it.kind().equals("REPLACE")) {
                        x.replaceProps(target, m);
                    } else {
                        x.mergeProps(target, m);
                    }
                }
                default -> {
                    if (!(target instanceof NodeV n)) {
                        throw Eval.typeMismatch("Node", target);
                    }
                    x.addLabels(n, it.labels());
                }
            }
        }
    }

    private static void checkEntity(Object t) {
        if (!(t instanceof NodeV) && !(t instanceof RelV)) {
            throw new CypherException(CypherException.TYPE, "Type mismatch: expected Node or Relationship but was "
                    + Values.typeName(t));
        }
    }

    private List<Map<String, Object>> remove(Remove rm, List<Map<String, Object>> in) {
        for (Map<String, Object> row : in) {
            for (RemoveItem it : rm.items()) {
                if (it.kind().equals("PROP")) {
                    Prop p = (Prop) it.target();
                    Object target = ev.eval(p.target(), row);
                    if (target == null) {
                        continue;
                    }
                    checkEntity(target);
                    x.setProp(target, p.key(), null);
                } else {
                    Object target = ev.eval(it.target(), row);
                    if (target == null) {
                        continue;
                    }
                    if (!(target instanceof NodeV n)) {
                        throw Eval.typeMismatch("Node", target);
                    }
                    x.removeLabels(n, it.labels());
                }
            }
        }
        return in;
    }

    private List<Map<String, Object>> delete(Delete d, List<Map<String, Object>> in) {
        for (Map<String, Object> row : in) {
            for (Expr e : d.targets()) {
                deleteValue(ev.eval(e, row), d.detach());
            }
        }
        return in;
    }

    private void deleteValue(Object v, boolean detach) {
        if (v == null) {
            return;
        }
        if (v instanceof NodeV n) {
            x.deleteNode(n, detach);
        } else if (v instanceof RelV r) {
            x.deleteRel(r);
        } else if (v instanceof PathV p) {
            for (RelV r : p.rels()) {
                x.deleteRel(r);
            }
            for (NodeV n : p.nodes()) {
                x.deleteNode(n, detach);
            }
        } else if (v instanceof List<?> l) {
            for (Object o : l) {
                deleteValue(o, detach);
            }
        } else {
            throw new CypherException(CypherException.TYPE, "Failed to delete `" + Funcs.describe(v)
                    + "`: expected a node, relationship or path");
        }
    }

    // ------------------------------------------------------------------------------------------ CALL / FOREACH

    private List<Map<String, Object>> call(CallClause cc, List<Map<String, Object>> in) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> row : in) {
            List<Object> args = new ArrayList<>();
            if (cc.args() != null) {
                for (Expr a : cc.args()) {
                    args.add(ev.eval(a, row));
                }
            }
            Procedures.Out res = Procedures.call(this, cc.name(), args, cc.args() == null);
            for (List<Object> rr : res.rows()) {
                Map<String, Object> r = new HashMap<>(row);
                if (cc.yields() == null || cc.yieldStar()) {
                    for (int i = 0; i < res.columns().size(); i++) {
                        r.put(res.columns().get(i), rr.get(i));
                    }
                } else {
                    for (YieldItem y : cc.yields()) {
                        int ci = res.columns().indexOf(y.name());
                        if (ci < 0) {
                            throw CypherException.syntax("Variable `" + y.name() + "` not defined: procedure "
                                    + cc.name() + " does not output it");
                        }
                        r.put(y.alias() != null ? y.alias() : y.name(), rr.get(ci));
                    }
                }
                if (cc.where() == null || ev.isTrue(cc.where(), r)) {
                    out.add(r);
                }
            }
        }
        return out;
    }

    private List<Map<String, Object>> foreach(Foreach f, List<Map<String, Object>> in) {
        for (Map<String, Object> row : in) {
            Object l = ev.eval(f.list(), row);
            if (l == null) {
                continue;
            }
            if (!(l instanceof List<?> list)) {
                throw Eval.typeMismatch("List", l);
            }
            for (Object o : list) {
                Map<String, Object> r = new HashMap<>(row);
                r.put(f.var(), o);
                List<Map<String, Object>> rows = new ArrayList<>();
                rows.add(r);
                for (Clause c : f.body()) {
                    rows = switch (c) {
                        case Create cr -> create(cr, rows);
                        case Merge mg -> merge(mg, rows);
                        case SetClause s -> set(s.items(), rows);
                        case Remove rm -> remove(rm, rows);
                        case Delete d -> delete(d, rows);
                        case Foreach ff -> foreach(ff, rows);
                        default -> throw CypherException.syntax("Invalid use of "
                                + c.getClass().getSimpleName() + " inside FOREACH");
                    };
                }
            }
        }
        return in;
    }

    private List<Map<String, Object>> callSubquery(CallSubquery cs, List<Map<String, Object>> in) {
        List<Map<String, Object>> out = new ArrayList<>();
        boolean returns = false;
        List<Clause> last = cs.query().parts().get(cs.query().parts().size() - 1);
        returns = last.get(last.size() - 1) instanceof Return;
        for (Map<String, Object> row : in) {
            Result r = runQuery(cs.query(), List.of(new HashMap<>(row)));
            if (!returns) {
                out.add(row);
                continue;
            }
            for (List<Object> vals : r.rows()) {
                Map<String, Object> nr = new HashMap<>(row);
                for (int i = 0; i < r.columns().size(); i++) {
                    nr.put(r.columns().get(i), vals.get(i));
                }
                out.add(nr);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------------------------------ expression subqueries

    Object patternExists(Eval e, PatternPart pattern, Map<String, Object> row) {
        return matcher.matchAny(List.of(pattern), row, null);
    }

    Object patternComprehension(Eval e, PatternComp pc, Map<String, Object> row) {
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> m : matcher.matchAll(List.of(pc.pattern()), row, pc.where())) {
            out.add(e.eval(pc.map(), m));
        }
        return out;
    }

    Object existsSub(Eval e, List<PatternPart> patterns, Expr where, Query q, Map<String, Object> row) {
        if (q == null) {
            return matcher.matchAny(patterns, row, where);
        }
        return !runQuery(withReturn(q), List.of(new HashMap<>(row))).rows().isEmpty();
    }

    Object countSub(Eval e, List<PatternPart> patterns, Expr where, Query q, Map<String, Object> row) {
        if (q == null) {
            return (long) matcher.matchAll(patterns, row, where).size();
        }
        return (long) runQuery(withReturn(q), List.of(new HashMap<>(row))).rows().size();
    }

    /** A subquery without a final RETURN (EXISTS / COUNT bodies) gets a synthetic one so its rows can be counted. */
    private static Query withReturn(Query q) {
        List<List<Clause>> parts = new ArrayList<>();
        for (List<Clause> part : q.parts()) {
            if (part.get(part.size() - 1) instanceof Return) {
                parts.add(part);
            } else {
                List<Clause> p2 = new ArrayList<>(part);
                p2.add(new Return(new Projection(false, false, List.of(new ProjItem(new Lit(1L), "x", "x")), List.of(), null, null)));
                parts.add(p2);
            }
        }
        return new Query(parts, q.all(), q.text());
    }

    Object collectSub(Eval e, Query q, Map<String, Object> row) {
        Result r = runQuery(q, List.of(new HashMap<>(row)));
        List<Object> out = new ArrayList<>();
        for (List<Object> vals : r.rows()) {
            out.add(vals.get(0));
        }
        return out;
    }

    Object shortestPathExpr(Eval e, PatternPart p, Map<String, Object> row) {
        List<Map<String, Object>> ms = matcher.matchAll(List.of(new PatternPart("\u0000p", p.elements(), p.shortest())), row, null);
        if (ms.isEmpty()) {
            return null;
        }
        return ms.get(0).get("\u0000p");
    }
}
