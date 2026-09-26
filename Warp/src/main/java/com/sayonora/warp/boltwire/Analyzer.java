package com.sayonora.warp.boltwire;

import com.sayonora.warp.boltwire.Cy.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Semantic analysis done before a statement runs (Neo4j reports these at compile time, as SyntaxError): variable scoping,
 * variable type conflicts, clause composition, aggregation rules, function existence / arity, and a light static type check
 * of operators and function arguments over literal-typed operands.
 */
final class Analyzer {

    enum K { ANY, NULL, BOOL, INT, FLOAT, NUM, STR, LIST, MAP, NODE, REL, PATH, POINT, DATE, TIME, LTIME, LDT, DT, DUR }

    /** A static type; elem is the list element type (or null when unknown). */
    record Ty(K k, Ty elem) {
        static final Ty ANY = new Ty(K.ANY, null);
        static final Ty NULL = new Ty(K.NULL, null);
        static final Ty BOOL = new Ty(K.BOOL, null);
        static final Ty INT = new Ty(K.INT, null);
        static final Ty FLOAT = new Ty(K.FLOAT, null);
        static final Ty NUM = new Ty(K.NUM, null);
        static final Ty STR = new Ty(K.STR, null);
        static final Ty MAP = new Ty(K.MAP, null);
        static final Ty NODE = new Ty(K.NODE, null);
        static final Ty REL = new Ty(K.REL, null);
        static final Ty PATH = new Ty(K.PATH, null);
        static final Ty POINT = new Ty(K.POINT, null);
        static final Ty DUR = new Ty(K.DUR, null);

        static Ty list(Ty e) {
            return new Ty(K.LIST, e);
        }

        boolean any() {
            return k == K.ANY || k == K.NULL;
        }

        boolean number() {
            return k == K.INT || k == K.FLOAT || k == K.NUM;
        }

        boolean temporal() {
            return k == K.DATE || k == K.TIME || k == K.LTIME || k == K.LDT || k == K.DT;
        }

        String name() {
            return switch (k) {
                case ANY -> "Any";
                case NULL -> "Null";
                case BOOL -> "Boolean";
                case INT -> "Integer";
                case FLOAT -> "Float";
                case NUM -> "Number";
                case STR -> "String";
                case LIST -> "List<" + (elem == null ? "Any" : elem.name()) + ">";
                case MAP -> "Map";
                case NODE -> "Node";
                case REL -> "Relationship";
                case PATH -> "Path";
                case POINT -> "Point";
                case DATE -> "Date";
                case TIME -> "Time";
                case LTIME -> "LocalTime";
                case LDT -> "LocalDateTime";
                case DT -> "DateTime";
                case DUR -> "Duration";
            };
        }
    }

    /** What the executor needs to know about an analysed query. */
    static final class Info {
        boolean updates;
        boolean schema;
        boolean readOnlyShow;
        boolean usesParams;
        List<String> columns = List.of();
        /** RETURN * / WITH * expansions (sorted names in scope at that point). */
        final IdentityHashMap<Projection, List<String>> star = new IdentityHashMap<>();
    }

    private final Info info = new Info();
    private boolean lastRestricted;
    private final Map<String, Object> knownParams;

    private Analyzer(Map<String, Object> params) {
        this.knownParams = params;
    }

    static Info analyze(Query q) {
        return analyze(q, null);
    }

    static Info analyze(Query q, Map<String, Object> params) {
        Analyzer a = new Analyzer(params);
        a.query(q, new Scope(), false);
        return a.info;
    }

    static final class Scope {
        final LinkedHashMap<String, Ty> vars = new LinkedHashMap<>();

        Scope copy() {
            Scope s = new Scope();
            s.vars.putAll(vars);
            return s;
        }
    }

    // ------------------------------------------------------------------------------------------ query / clauses

    private List<String> query(Query q, Scope outer, boolean subquery) {
        List<List<String>> colsPerPart = new ArrayList<>();
        for (int i = 0; i < q.parts().size(); i++) {
            colsPerPart.add(part(q.parts().get(i), outer.copy(), subquery));
        }
        if (q.parts().size() > 1) {
            Boolean first = q.all().get(0);
            for (Boolean b : q.all()) {
                if (!b.equals(first)) {
                    throw CypherException.syntax("Invalid combination of UNION and UNION ALL");
                }
            }
            for (int i = 1; i < colsPerPart.size(); i++) {
                List<String> a = colsPerPart.get(0), b = colsPerPart.get(i);
                if (a == null || b == null || !new HashSet<>(a).equals(new HashSet<>(b)) || a.size() != b.size()) {
                    throw CypherException.syntax("All sub queries in an UNION must have the same return column names");
                }
            }
        }
        List<String> cols = colsPerPart.get(0);
        if (!subquery) {
            info.columns = cols == null ? List.of() : cols;
        }
        return cols;
    }

    private List<String> part(List<Clause> clauses, Scope scope, boolean subquery) {
        boolean updated = false;
        String lastUpdate = null;
        List<String> columns = null;
        boolean endsWithReturn = false;
        for (int i = 0; i < clauses.size(); i++) {
            Clause c = clauses.get(i);
            boolean last = i == clauses.size() - 1;
            if (endsWithReturn) {
                throw CypherException.syntax("Invalid input: RETURN must be the last clause of a query");
            }
            switch (c) {
                case SchemaCmd sc -> {
                    if (clauses.size() != 1) {
                        throw CypherException.syntax("Schema commands cannot be combined with other clauses");
                    }
                    info.schema = true;
                    info.updates |= !sc.kind().equals("SHOW");
                    columns = schemaColumns(sc);
                    endsWithReturn = true;
                }
                case Match m -> {
                    requireWith(updated, lastUpdate, "MATCH");
                    match(m, scope);
                }
                case Unwind u -> {
                    requireWith(updated, lastUpdate, "UNWIND");
                    Ty t = expr(u.expr(), scope);
                    if (scope.vars.containsKey(u.var())) {
                        throw CypherException.syntax("Variable `" + u.var() + "` already declared");
                    }
                    scope.vars.put(u.var(), t.k() == K.LIST && t.elem() != null ? t.elem() : Ty.ANY);
                }
                case With w -> {
                    columns = null;
                    Scope next = projection(w.proj(), scope, false);
                    if (w.where() != null) {
                        noAggregate(w.where(), "WHERE");
                        Scope whereScope = next;
                        if (!lastRestricted) {
                            whereScope = scope.copy();
                            whereScope.vars.putAll(next.vars);
                        }
                        Ty wt = expr(w.where(), whereScope);
                        needBool(wt);
                    }
                    scope.vars.clear();
                    scope.vars.putAll(next.vars);
                    updated = false;
                    lastUpdate = null;
                }
                case Return r -> {
                    Scope next = projection(r.proj(), scope, true);
                    columns = new ArrayList<>(next.vars.keySet());
                    endsWithReturn = true;
                    if (!last) {
                        throw CypherException.syntax("Invalid input: RETURN must be the last clause of a query");
                    }
                }
                case Create cr -> {
                    updated = true;
                    lastUpdate = "CREATE";
                    info.updates = true;
                    for (PatternPart p : cr.parts()) {
                        createPattern(p, scope, false);
                    }
                }
                case Merge mg -> {
                    updated = true;
                    lastUpdate = "MERGE";
                    info.updates = true;
                    createPattern(mg.part(), scope, true);
                    for (SetItem s : mg.onCreate()) {
                        setItem(s, scope);
                    }
                    for (SetItem s : mg.onMatch()) {
                        setItem(s, scope);
                    }
                }
                case SetClause s -> {
                    updated = true;
                    lastUpdate = "SET";
                    info.updates = true;
                    for (SetItem it : s.items()) {
                        setItem(it, scope);
                    }
                }
                case Remove rm -> {
                    updated = true;
                    lastUpdate = "REMOVE";
                    info.updates = true;
                    for (RemoveItem it : rm.items()) {
                        Ty t = expr(it.kind().equals("PROP") ? ((Prop) it.target()).target() : it.target(), scope);
                        if (it.kind().equals("LABELS") && !t.any() && t.k() != K.NODE) {
                            throw typeMismatchErr("Node", t);
                        }
                    }
                }
                case Delete d -> {
                    updated = true;
                    lastUpdate = d.detach() ? "DETACH DELETE" : "DELETE";
                    info.updates = true;
                    for (Expr e : d.targets()) {
                        if (e instanceof HasLabels) {
                            throw CypherException.syntax("DELETE doesn't support removing labels from a node. Try REMOVE.");
                        }
                        Ty t = expr(e, scope);
                        if (!t.any() && t.k() != K.NODE && t.k() != K.REL && t.k() != K.PATH && t.k() != K.LIST
                                && t.k() != K.MAP) {
                            throw CypherException.syntax("Type mismatch: expected Node, Path or Relationship but was " + t.name());
                        }
                    }
                }
                case CallClause cc -> {
                    if (cc.args() == null && !(clauses.size() == 1)) {
                        throw CypherException.syntax("Procedure call inside a query does not support naming results implicitly "
                                + "(name explicitly using `YIELD` instead)");
                    }
                    if (!cc.standalone() && cc.args() == null && clauses.size() != 1) {
                        throw CypherException.syntax("Procedure call inside a query does not support passing arguments implicitly");
                    }
                    if (cc.yields() == null && clauses.size() > 1) {
                        throw CypherException.syntax("Procedure call inside a query does not support naming results implicitly "
                                + "(name explicitly using `YIELD` instead)");
                    }
                    List<String> outs = Procedures.outputs(cc.name());
                    if (outs == null) {
                        throw new CypherException("Neo.ClientError.Procedure.ProcedureNotFound",
                                "There is no procedure with the name `" + cc.name() + "` registered for this database instance. "
                                        + "Please ensure you've spelled the procedure name correctly and that the procedure is "
                                        + "properly deployed.");
                    }
                    if (cc.args() != null) {
                        for (Expr a : cc.args()) {
                            expr(a, scope);
                        }
                    }
                    List<String> produced = new ArrayList<>();
                    if (cc.yields() == null || cc.yieldStar()) {
                        produced.addAll(outs);
                    } else {
                        for (YieldItem y : cc.yields()) {
                            if (!outs.contains(y.name())) {
                                throw CypherException.syntax("Unknown procedure output: `" + y.name() + "`");
                            }
                            produced.add(y.alias() != null ? y.alias() : y.name());
                        }
                    }
                    if (clauses.size() == 1 && cc.yields() == null) {
                        columns = new ArrayList<>(outs);
                        endsWithReturn = true;
                    }
                    for (String p : produced) {
                        if (scope.vars.containsKey(p)) {
                            throw CypherException.syntax("Variable `" + p + "` already declared");
                        }
                        scope.vars.put(p, Ty.ANY);
                    }
                    if (cc.where() != null) {
                        needBool(expr(cc.where(), scope));
                    }
                    if (last && clauses.size() > 1 && cc.yields() != null) {
                        // a trailing in-query CALL without RETURN is only valid standalone
                        throw CypherException.syntax("Query cannot conclude with CALL (must be RETURN or an update clause)");
                    }
                    if (clauses.size() == 1 && cc.yields() != null) {
                        columns = produced;
                        endsWithReturn = true;
                    }
                }
                case Foreach f -> {
                    updated = true;
                    lastUpdate = "FOREACH";
                    info.updates = true;
                    Ty lt = expr(f.list(), scope);
                    if (!lt.any() && lt.k() != K.LIST) {
                        throw typeMismatchErr("List", lt);
                    }
                    Scope inner = scope.copy();
                    if (inner.vars.containsKey(f.var())) {
                        throw CypherException.syntax("Variable `" + f.var() + "` already declared");
                    }
                    inner.vars.put(f.var(), lt.k() == K.LIST && lt.elem() != null ? lt.elem() : Ty.ANY);
                    for (Clause bc : f.body()) {
                        switch (bc) {
                            case Create cr -> {
                                for (PatternPart p : cr.parts()) {
                                    createPattern(p, inner, false);
                                }
                            }
                            case Merge mg -> {
                                createPattern(mg.part(), inner, true);
                                for (SetItem s : mg.onCreate()) {
                                    setItem(s, inner);
                                }
                                for (SetItem s : mg.onMatch()) {
                                    setItem(s, inner);
                                }
                            }
                            case SetClause s -> {
                                for (SetItem it : s.items()) {
                                    setItem(it, inner);
                                }
                            }
                            case Remove rm -> {
                                for (RemoveItem it : rm.items()) {
                                    expr(it.kind().equals("PROP") ? ((Prop) it.target()).target() : it.target(), inner);
                                }
                            }
                            case Delete d -> {
                                for (Expr e : d.targets()) {
                                    expr(e, inner);
                                }
                            }
                            case Foreach ff -> {
                                Ty t2 = expr(ff.list(), inner);
                                Scope in2 = inner.copy();
                                in2.vars.put(ff.var(), t2.k() == K.LIST && t2.elem() != null ? t2.elem() : Ty.ANY);
                                // nested FOREACH bodies are checked shallowly
                            }
                            default -> throw CypherException.syntax("Invalid use of "
                                    + bc.getClass().getSimpleName().toUpperCase(Locale.ROOT) + " inside FOREACH");
                        }
                    }
                }
                case CallSubquery cs -> {
                    requireWith(updated, lastUpdate, "CALL");
                    List<String> cols = query(cs.query(), scope, true);
                    detectUpdates(cs.query());
                    if (cols != null) {
                        for (String col : cols) {
                            if (scope.vars.containsKey(col)) {
                                throw CypherException.syntax("Variable `" + col + "` already declared");
                            }
                            scope.vars.put(col, Ty.ANY);
                        }
                    }
                }
            }
        }
        Clause lastClause = clauses.get(clauses.size() - 1);
        if (!endsWithReturn && !subquery) {
            boolean ok = lastClause instanceof Create || lastClause instanceof Merge || lastClause instanceof SetClause
                    || lastClause instanceof Remove || lastClause instanceof Delete || lastClause instanceof Foreach
                    || lastClause instanceof SchemaCmd || lastClause instanceof CallSubquery
                    || (lastClause instanceof CallClause cc && clauses.size() == 1);
            if (!ok) {
                throw CypherException.syntax("Query cannot conclude with " + clauseName(lastClause)
                        + " (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no "
                        + "YIELD).");
            }
        }
        return endsWithReturn ? columns : (subquery ? null : List.of());
    }

    private void detectUpdates(Query q) {
        for (List<Clause> part : q.parts()) {
            for (Clause c : part) {
                if (c instanceof Create || c instanceof Merge || c instanceof SetClause || c instanceof Remove
                        || c instanceof Delete || c instanceof Foreach) {
                    info.updates = true;
                }
            }
        }
    }

    private static String clauseName(Clause c) {
        return switch (c) {
            case Match m -> m.optional() ? "OPTIONAL MATCH" : "MATCH";
            case Unwind u -> "UNWIND";
            case With w -> "WITH";
            case CallClause cc -> "CALL";
            default -> c.getClass().getSimpleName().toUpperCase(Locale.ROOT);
        };
    }

    private static void requireWith(boolean updated, String lastUpdate, String clause) {
        if (updated) {
            throw CypherException.syntax("WITH is required between " + lastUpdate + " and " + clause);
        }
    }

    private List<String> schemaColumns(SchemaCmd sc) {
        return List.of();
    }

    // ------------------------------------------------------------------------------------------ MATCH / CREATE patterns

    private void match(Match m, Scope scope) {
        Set<String> relVarsInClause = new HashSet<>();
        for (PatternPart p : m.parts()) {
            declarePattern(p, scope, relVarsInClause, "MATCH");
        }
        if (m.where() != null) {
            noAggregate(m.where(), "WHERE");
            needBool(expr(m.where(), scope));
        }
    }

    /** Declares the variables of a MATCH pattern, checking variable-type conflicts; property maps are checked as expressions. */
    private void declarePattern(PatternPart p, Scope scope, Set<String> relVars, String ctx) {
        if (p.pathVar() != null) {
            if (scope.vars.containsKey(p.pathVar())) {
                throw CypherException.syntax("Variable `" + p.pathVar() + "` already declared");
            }
            for (int i = 0; i <= p.relCount(); i++) {
                if (p.pathVar().equals(p.node(i).var()) || (i < p.relCount() && p.pathVar().equals(p.rel(i).var()))) {
                    throw CypherException.syntax("Variable `" + p.pathVar() + "` already declared");
                }
            }
        }
        for (int i = 0; i <= p.relCount(); i++) {
            NodePat n = p.node(i);
            if (n.props() instanceof Param) {
                throw CypherException.syntax("Parameter maps cannot be used in `" + ctx + "` patterns (use a literal map instead, "
                        + "e.g. `{id: $p.id}`)");
            }
            if (n.var() != null) {
                Ty cur = scope.vars.get(n.var());
                if (cur != null && !cur.any() && cur.k() != K.NODE) {
                    throw CypherException.syntax("Type mismatch: " + n.var() + " defined with conflicting type " + cur.name()
                            + " (expected Node)");
                }
                scope.vars.put(n.var(), Ty.NODE);
            }
            if (n.props() != null) {
                propMap(n.props(), scope);
            }
            if (i < p.relCount()) {
                RelPat r = p.rel(i);
                if (r.props() instanceof Param) {
                    throw CypherException.syntax("Parameter maps cannot be used in `" + ctx + "` patterns (use a literal map "
                            + "instead, e.g. `{id: $p.id}`)");
                }
                if (r.var() != null) {
                    Ty cur = scope.vars.get(r.var());
                    Ty want = r.varLen() ? Ty.list(Ty.REL) : Ty.REL;
                    if (cur != null && !cur.any() && cur.k() != want.k()) {
                        throw CypherException.syntax("Type mismatch: " + r.var() + " defined with conflicting type " + cur.name()
                                + " (expected " + want.name() + ")");
                    }
                    if (!relVars.add(r.var()) && ctx.equals("MATCH") && false) {
                        throw CypherException.syntax("Cannot use the same relationship variable '" + r.var()
                                + "' for multiple relationships");
                    }
                    scope.vars.put(r.var(), want);
                }
                if (r.props() != null) {
                    propMap(r.props(), scope);
                }
                if (r.varLen() && r.maxHops() >= 0 && r.minHops() > r.maxHops() && false) {
                    throw CypherException.syntax("Invalid variable-length range");
                }
            }
        }
        if (p.pathVar() != null) {
            scope.vars.put(p.pathVar(), Ty.PATH);
        }
    }

    private void propMap(Expr props, Scope scope) {
        Ty t = expr(props, scope);
        if (!t.any() && t.k() != K.MAP) {
            throw typeMismatchErr("Map", t);
        }
    }

    private void createPattern(PatternPart p, Scope scope, boolean merge) {
        String ctx = merge ? "MERGE" : "CREATE";
        if (p.pathVar() != null && scope.vars.containsKey(p.pathVar())) {
            throw CypherException.syntax("Variable `" + p.pathVar() + "` already declared");
        }
        if (p.pathVar() != null) {
            for (int i = 0; i <= p.relCount(); i++) {
                if (p.pathVar().equals(p.node(i).var()) || (i < p.relCount() && p.pathVar().equals(p.rel(i).var()))) {
                    throw CypherException.syntax("Variable `" + p.pathVar() + "` already declared");
                }
            }
        }
        if (p.relCount() == 0 && p.node(0).var() != null && scope.vars.containsKey(p.node(0).var())) {
            throw CypherException.syntax("Variable `" + p.node(0).var() + "` already declared");
        }
        Set<String> seenRel = new HashSet<>();
        for (int i = 0; i <= p.relCount(); i++) {
            NodePat n = p.node(i);
            if (n.props() instanceof Param) {
                if (merge) {
                    throw CypherException.syntax("Parameter maps cannot be used in `MERGE` patterns (use a literal map instead, "
                            + "e.g. `{id: $p.id}`)");
                }
            }
            if (n.var() != null) {
                Ty cur = scope.vars.get(n.var());
                if (cur != null) {
                    if (!cur.any() && cur.k() != K.NODE) {
                        throw CypherException.syntax("Type mismatch: " + n.var() + " defined with conflicting type " + cur.name()
                                + " (expected Node)");
                    }
                    if (!n.labels().isEmpty() || n.props() != null) {
                        throw CypherException.syntax("Variable `" + n.var() + "` already declared");
                    }
                } else {
                    scope.vars.put(n.var(), Ty.NODE);
                }
            }
            if (n.props() != null) {
                propMap(n.props(), scope);
            }
            if (i < p.relCount()) {
                RelPat r = p.rel(i);
                if (r.varLen()) {
                    throw CypherException.syntax("Variable length relationships cannot be used in " + ctx);
                }
                if (r.types().size() != 1) {
                    throw CypherException.syntax("A single relationship type must be specified for " + ctx);
                }
                if (r.dir() == Dir.BOTH && !merge) {
                    throw CypherException.syntax("Only directed relationships are supported in " + ctx);
                }
                if (r.var() != null) {
                    if (scope.vars.containsKey(r.var()) || !seenRel.add(r.var())) {
                        throw CypherException.syntax("Variable `" + r.var() + "` already declared");
                    }
                    scope.vars.put(r.var(), Ty.REL);
                }
                if (r.props() instanceof Param && merge) {
                    throw CypherException.syntax("Parameter maps cannot be used in `MERGE` patterns (use a literal map instead, "
                            + "e.g. `{id: $p.id}`)");
                }
                if (r.props() != null) {
                    propMap(r.props(), scope);
                }
            }
        }
        if (p.pathVar() != null) {
            scope.vars.put(p.pathVar(), Ty.PATH);
        }
    }

    private void setItem(SetItem it, Scope scope) {
        switch (it.kind()) {
            case "PROP" -> {
                Prop p = (Prop) it.target();
                Ty t = expr(p.target(), scope);
                if (!t.any() && t.k() != K.NODE && t.k() != K.REL) {
                    throw typeMismatchErr("Node or Relationship", t);
                }
                noAggregate(it.value(), "SET");
                expr(it.value(), scope);
            }
            case "REPLACE", "MERGE" -> {
                Ty t = expr(it.target(), scope);
                if (!t.any() && t.k() != K.NODE && t.k() != K.REL) {
                    throw typeMismatchErr("Node or Relationship", t);
                }
                noAggregate(it.value(), "SET");
                Ty vt = expr(it.value(), scope);
                if (!vt.any() && vt.k() != K.MAP && vt.k() != K.NODE && vt.k() != K.REL) {
                    throw typeMismatchErr("Map", vt);
                }
            }
            default -> {
                Ty t = expr(it.target(), scope);
                if (!t.any() && t.k() != K.NODE) {
                    throw typeMismatchErr("Node", t);
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------ projections

    private Scope projection(Projection p, Scope scope, boolean isReturn) {
        Scope out = new Scope();
        List<Expr> exprs = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Set<String> nameSet = new HashSet<>();
        if (p.star()) {
            if (scope.vars.isEmpty() && isReturn) {
                throw CypherException.syntax("RETURN * is not allowed when there are no variables in scope");
            }
            List<String> vars = new ArrayList<>(new java.util.TreeSet<>(scope.vars.keySet()));
            info.star.put(p, vars);
            for (String v : vars) {
                out.vars.put(v, scope.vars.get(v));
                nameSet.add(v);
            }
        }
        boolean anyAgg = false;
        List<Ty> types = new ArrayList<>();
        for (ProjItem it : p.items()) {
            if (!isReturn && it.alias() == null && !(it.expr() instanceof Var)) {
                throw CypherException.syntax("Expression in WITH must be aliased (use AS)");
            }
            String name = it.alias() != null ? it.alias() : it.text();
            if (it.alias() == null && it.expr() instanceof Var v) {
                name = v.name();
            }
            if (!nameSet.add(name)) {
                throw CypherException.syntax("Multiple result columns with the same name are not supported");
            }
            if (it.expr() instanceof PatternPred) {
                throw CypherException.syntax("Invalid input: a pattern expression cannot be used as a projection");
            }
            checkAggregates(it.expr());
            anyAgg |= Executor.containsAggregate(it.expr());
            Ty t = expr(it.expr(), scope, true);
            types.add(t);
            names.add(name);
            exprs.add(it.expr());
        }
        // ambiguous aggregation
        if (anyAgg) {
            Set<String> keys = new HashSet<>();
            for (Expr e : exprs) {
                if (!Executor.containsAggregate(e)) {
                    keys.add(norm(e));
                }
            }
            if (p.star()) {
                for (String v : scope.vars.keySet()) {
                    keys.add(v);
                }
            }
            for (Expr e : exprs) {
                if (Executor.containsAggregate(e)) {
                    checkAmbiguous(e, keys);
                }
            }
        }
        for (int i = 0; i < names.size(); i++) {
            out.vars.put(names.get(i), types.get(i));
        }
        // ORDER BY: sees projected names; without aggregation/DISTINCT also the previous scope
        boolean restricted = anyAgg || p.distinct();
        lastRestricted = anyAgg;
        for (SortItem s : p.orderBy()) {
            boolean aggInOrder = Executor.containsAggregate(s.expr());
            if (aggInOrder && !anyAgg) {
                throw CypherException.syntax("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the "
                        + "preceding " + (isReturn ? "RETURN" : "WITH"));
            }
            checkAggregates(s.expr());
            Scope full = scope.copy();
            full.vars.putAll(out.vars);
            if (!restricted) {
                expr(s.expr(), full, true);
            } else {
                Set<String> projected = new HashSet<>();
                for (ProjItem pi : p.items()) {
                    projected.add(norm(pi.expr()));
                }
                checkOrderRestricted(s.expr(), projected, out, scope);
                expr(s.expr(), full, true);
            }
        }
        for (Expr lim : new Expr[] {p.skip(), p.limit()}) {
            if (lim != null) {
                checkLimit(lim, lim == p.skip() ? "SKIP" : "LIMIT");
            }
        }
        return out;
    }

    private static String varName(String message) {
        int a = message.indexOf('`'), b = message.indexOf('`', a + 1);
        return a >= 0 && b > a ? message.substring(a + 1, b) : "";
    }

    private void checkLimit(Expr e, String what) {
        if (Analyzer.walkAny(e, n -> n instanceof Var || n instanceof Prop && false)) {
            throw CypherException.syntax("It is not allowed to refer to variables in " + what
                    + ", so that the value for " + what + " can be statically calculated.");
        }
        if (Analyzer.walkAny(e, n -> n instanceof CountStar || (n instanceof Call c && Funcs.isAggregate(c.name())))) {
            throw CypherException.syntax("Invalid use of aggregating function in " + what);
        }
        if (e instanceof Lit l) {
            if (l.value() instanceof Long v && v < 0) {
                throw CypherException.syntax("Invalid input. '" + v + "' is not a valid value. Must be a non-negative integer.");
            }
            if (l.value() instanceof Double d) {
                throw CypherException.syntax("It is not allowed to use floating point numbers for " + what);
            }
            if (l.value() instanceof String || l.value() instanceof Boolean) {
                throw CypherException.syntax("Type mismatch: expected Integer but was " + Values.typeName(l.value()));
            }
        }
        if (e instanceof Unary u && u.op() == '-' && u.operand() instanceof Lit l && l.value() instanceof Long) {
            throw CypherException.syntax("Invalid input: a negative value is not a valid " + what);
        }
        if (e instanceof Lit l && l.value() instanceof Long v && v < 0) {
            throw CypherException.syntax("Invalid input");
        }
    }

    private void checkAggregates(Expr e) {
        walk(e, n -> {
            if (n instanceof CountStar || (n instanceof Call c && Funcs.isAggregate(c.name()))) {
                if (n instanceof Call c) {
                    for (Expr a : c.args()) {
                        if (Executor.containsAggregate(a)) {
                            throw CypherException.syntax("Can't use aggregate functions inside of aggregate functions.");
                        }
                    }
                }
                return false;
            }
            return true;
        });
    }

    private void checkAmbiguous(Expr e, Set<String> keys) {
        if (e instanceof CountStar || (e instanceof Call c && Funcs.isAggregate(c.name()))) {
            return;
        }
        if (e instanceof Var v) {
            if (!keys.contains(v.name())) {
                throw ambiguous(e);
            }
            return;
        }
        if (e instanceof Prop) {
            Expr root = e;
            while (root instanceof Prop pr) {
                root = pr.target();
            }
            if (root instanceof Var rv) {
                if (!keys.contains(norm(e)) && !keys.contains(rv.name())) {
                    throw ambiguous(e);
                }
                return;
            }
        }
        for (Expr child : bindersFiltered(e)) {
            checkAmbiguous(child, keys);
        }
    }

    private static CypherException ambiguous(Expr e) {
        return CypherException.syntax("Aggregation column contains implicit grouping expressions. Implicit grouping expressions "
                + "are not allowed: `" + norm(e) + "` is not a grouping key");
    }

    /** Children of an expression, with comprehension-bound variables shielded (their bodies are skipped: a bound variable is
     * never an implicit grouping reference, and the parts that are evaluated outside are visited separately). */
    private static List<Expr> bindersFiltered(Expr e) {
        List<Expr> out = new ArrayList<>();
        switch (e) {
            case ListComp lc -> out.add(lc.list());
            case Quantifier q -> out.add(q.list());
            case Reduce r -> {
                out.add(r.init());
                out.add(r.list());
            }
            case PatternComp pc -> {
            }
            default -> out.addAll(children(e));
        }
        return out;
    }

    /** ORDER BY after an aggregating / DISTINCT projection: sub-expressions equal to a projected expression are fine, other
     * variables must be projected names, and aggregates must be projected ones or use only projected names. */
    private void checkOrderRestricted(Expr e, Set<String> projected, Scope out, Scope old) {
        if (projected.contains(norm(e))) {
            return;
        }
        if (e instanceof CountStar) {
            return;
        }
        if (e instanceof Call c && Funcs.isAggregate(c.name())) {
            for (Expr a : c.args()) {
                Set<String> vars = new LinkedHashSet<>();
                freeVarsOutsideAggregates(a, new HashSet<>(), vars);
                for (String v : vars) {
                    if (!out.vars.containsKey(v)) {
                        throw CypherException.syntax("Variable `" + v + "` not defined");
                    }
                }
            }
            return;
        }
        if (e instanceof Var v) {
            if (!out.vars.containsKey(v.name())) {
                if (old.vars.containsKey(v.name())) {
                    throw CypherException.syntax("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access "
                            + "variables declared before the WITH/RETURN: " + v.name());
                }
                throw CypherException.syntax("Variable `" + v.name() + "` not defined");
            }
            return;
        }
        switch (e) {
            case ListComp lc -> {
                checkOrderRestricted(lc.list(), projected, out, old);
            }
            case Quantifier q -> checkOrderRestricted(q.list(), projected, out, old);
            case Reduce r -> {
                checkOrderRestricted(r.init(), projected, out, old);
                checkOrderRestricted(r.list(), projected, out, old);
            }
            default -> {
                for (Expr c : children(e)) {
                    checkOrderRestricted(c, projected, out, old);
                }
            }
        }
    }

    private static void freeVarsOutsideAggregates(Expr e, Set<String> bound, Set<String> out) {
        if (e instanceof CountStar || (e instanceof Call c && Funcs.isAggregate(c.name()))) {
            return;
        }
        switch (e) {
            case Var v -> {
                if (!bound.contains(v.name())) {
                    out.add(v.name());
                }
            }
            case ListComp lc -> {
                freeVarsOutsideAggregates(lc.list(), bound, out);
                Set<String> b2 = new HashSet<>(bound);
                b2.add(lc.var());
                if (lc.where() != null) {
                    freeVarsOutsideAggregates(lc.where(), b2, out);
                }
                if (lc.map() != null) {
                    freeVarsOutsideAggregates(lc.map(), b2, out);
                }
            }
            case Quantifier q -> {
                freeVarsOutsideAggregates(q.list(), bound, out);
                Set<String> b2 = new HashSet<>(bound);
                b2.add(q.var());
                if (q.where() != null) {
                    freeVarsOutsideAggregates(q.where(), b2, out);
                }
            }
            case Reduce r -> {
                freeVarsOutsideAggregates(r.init(), bound, out);
                freeVarsOutsideAggregates(r.list(), bound, out);
                Set<String> b2 = new HashSet<>(bound);
                b2.add(r.acc());
                b2.add(r.var());
                freeVarsOutsideAggregates(r.body(), b2, out);
            }
            default -> {
                for (Expr c : children(e)) {
                    freeVarsOutsideAggregates(c, bound, out);
                }
            }
        }
    }

    private void noAggregate(Expr e, String where) {
        if (Executor.containsAggregate(e)) {
            throw CypherException.syntax("Invalid use of aggregating function in " + where);
        }
    }

    private void needBool(Ty t) {
        if (!t.any() && t.k() != K.BOOL) {
            throw typeMismatchErr("Boolean", t);
        }
    }

    private static CypherException typeMismatchErr(String expected, Ty was) {
        return CypherException.syntax("Type mismatch: expected " + expected + " but was " + was.name());
    }

    // ------------------------------------------------------------------------------------------ expressions

    private Ty expr(Expr e, Scope s) {
        return expr(e, s, false);
    }

    private Ty expr(Expr e, Scope s, boolean allowAgg) {
        return switch (e) {
            case Lit l -> literalType(l.value());
            case Param p -> paramType(p);
            case Var v -> {
                Ty t = s.vars.get(v.name());
                if (t == null) {
                    throw CypherException.syntax("Variable `" + v.name() + "` not defined");
                }
                yield t;
            }
            case Prop p -> {
                Ty t = expr(p.target(), s, allowAgg);
                if (!t.any() && t.k() != K.MAP && t.k() != K.NODE && t.k() != K.REL && t.k() != K.POINT && !t.temporal()
                        && t.k() != K.DUR) {
                    throw typeMismatchErr("Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or "
                            + "DateTime", t);
                }
                yield Ty.ANY;
            }
            case Index i -> {
                Ty t = expr(i.target(), s, allowAgg);
                Ty it = expr(i.index(), s, allowAgg);
                if (t.k() == K.LIST) {
                    if (!it.any() && it.k() != K.INT) {
                        throw CypherException.syntax("Type mismatch: list index must be given as Integer, but was " + it.name());
                    }
                    yield t.elem() == null ? Ty.ANY : t.elem();
                }
                if (!t.any() && t.k() != K.MAP && t.k() != K.NODE && t.k() != K.REL) {
                    throw typeMismatchErr("Map, Node, Relationship or List", t);
                }
                if ((t.k() == K.MAP || t.k() == K.NODE || t.k() == K.REL) && !it.any() && it.k() != K.STR) {
                    throw typeMismatchErr("String", it);
                }
                yield Ty.ANY;
            }
            case Slice sl -> {
                Ty t = expr(sl.target(), s, allowAgg);
                if (!t.any() && t.k() != K.LIST) {
                    throw typeMismatchErr("List", t);
                }
                for (Expr b : new Expr[] {sl.from(), sl.to()}) {
                    if (b != null) {
                        Ty bt = expr(b, s, allowAgg);
                        if (!bt.any() && bt.k() != K.INT) {
                            throw CypherException.syntax("Type mismatch: expected Integer but was " + bt.name());
                        }
                    }
                }
                yield t.k() == K.LIST ? t : Ty.list(null);
            }
            case ListLit l -> {
                Ty elem = null;
                boolean first = true;
                for (Expr it : l.items()) {
                    Ty t = expr(it, s, allowAgg);
                    if (first) {
                        elem = t;
                        first = false;
                    } else if (elem != null && !elem.equals(t)) {
                        elem = elem.number() && t.number() ? Ty.NUM : (elem.k() == K.NULL ? t : (t.k() == K.NULL ? elem : Ty.ANY));
                    }
                }
                yield Ty.list(elem == null ? null : elem);
            }
            case MapLit m -> {
                Set<String> seen = new HashSet<>();
                for (int i = 0; i < m.keys().size(); i++) {
                    expr(m.values().get(i), s, allowAgg);
                }
                yield Ty.MAP;
            }
            case Unary u -> {
                Ty t = expr(u.operand(), s, allowAgg);
                if (!t.any() && !t.number() && t.k() != K.DUR) {
                    throw CypherException.syntax("Type mismatch: expected Float, Integer or Duration but was " + t.name());
                }
                yield t;
            }
            case Binary b -> binaryType(b, s, allowAgg);
            case Not n -> {
                Ty t = expr(n.operand(), s, allowAgg);
                needBool(t);
                yield Ty.BOOL;
            }
            case And a -> {
                needBool(expr(a.left(), s, allowAgg));
                needBool(expr(a.right(), s, allowAgg));
                yield Ty.BOOL;
            }
            case Or o -> {
                needBool(expr(o.left(), s, allowAgg));
                needBool(expr(o.right(), s, allowAgg));
                yield Ty.BOOL;
            }
            case Xor x -> {
                needBool(expr(x.left(), s, allowAgg));
                needBool(expr(x.right(), s, allowAgg));
                yield Ty.BOOL;
            }
            case Cmp c -> {
                expr(c.left(), s, allowAgg);
                expr(c.right(), s, allowAgg);
                yield Ty.BOOL;
            }
            case IsNull n -> {
                expr(n.operand(), s, allowAgg);
                yield Ty.BOOL;
            }
            case HasLabels h -> {
                Ty t = expr(h.target(), s, allowAgg);
                if (!t.any() && t.k() != K.NODE && t.k() != K.REL) {
                    throw typeMismatchErr("Node or Relationship", t);
                }
                yield Ty.BOOL;
            }
            case InList i -> {
                expr(i.element(), s, allowAgg);
                Ty t = expr(i.list(), s, allowAgg);
                if (!t.any() && t.k() != K.LIST) {
                    throw CypherException.syntax("Type mismatch: expected List<T> but was " + t.name());
                }
                yield Ty.BOOL;
            }
            case StrOp so -> {
                expr(so.left(), s, allowAgg);
                expr(so.right(), s, allowAgg);
                yield Ty.BOOL;
            }
            case RegexMatch r -> {
                Ty a = expr(r.left(), s, allowAgg), b = expr(r.right(), s, allowAgg);
                for (Ty t : new Ty[] {a, b}) {
                    if (!t.any() && t.k() != K.STR) {
                        throw typeMismatchErr("String", t);
                    }
                }
                yield Ty.BOOL;
            }
            case CountStar cs -> {
                if (!allowAgg) {
                    throw CypherException.syntax("Invalid use of aggregating function count(...) in this context");
                }
                yield Ty.INT;
            }
            case Call c -> callType(c, s, allowAgg);
            case CaseExpr c -> {
                if (c.subject() != null) {
                    expr(c.subject(), s, allowAgg);
                }
                for (Expr w : c.whens()) {
                    Ty t = expr(w, s, allowAgg);
                    if (c.subject() == null) {
                        needBool(t);
                    }
                }
                Ty res = null;
                for (Expr t : c.thens()) {
                    Ty tt = expr(t, s, allowAgg);
                    res = res == null ? tt : (res.equals(tt) ? res : Ty.ANY);
                }
                if (c.otherwise() != null) {
                    Ty tt = expr(c.otherwise(), s, allowAgg);
                    res = res == null ? tt : (res.equals(tt) ? res : Ty.ANY);
                }
                yield res == null ? Ty.ANY : (c.otherwise() == null ? Ty.ANY : res);
            }
            case ListComp lc -> {
                Ty lt = expr(lc.list(), s, allowAgg);
                if (!lt.any() && lt.k() != K.LIST) {
                    throw typeMismatchErr("List", lt);
                }
                Scope inner = s.copy();
                inner.vars.put(lc.var(), lt.k() == K.LIST && lt.elem() != null ? lt.elem() : Ty.ANY);
                if (lc.where() != null) {
                    needBool(expr(lc.where(), inner, false));
                }
                Ty mt = lc.map() != null ? expr(lc.map(), inner, false) : (lt.k() == K.LIST ? lt.elem() : null);
                yield Ty.list(mt);
            }
            case PatternComp pc -> {
                Scope inner = s.copy();
                declarePattern(pc.pattern(), inner, new HashSet<>(), "pattern comprehension");
                if (pc.where() != null) {
                    needBool(expr(pc.where(), inner, allowAgg));
                }
                yield Ty.list(expr(pc.map(), inner, allowAgg));
            }
            case Quantifier q -> {
                Ty lt = expr(q.list(), s, allowAgg);
                if (!lt.any() && lt.k() != K.LIST) {
                    throw typeMismatchErr("List", lt);
                }
                Scope inner = s.copy();
                inner.vars.put(q.var(), lt.k() == K.LIST && lt.elem() != null ? lt.elem() : Ty.ANY);
                if (q.where() != null) {
                    needBool(expr(q.where(), inner, false));
                }
                yield Ty.BOOL;
            }
            case Reduce r -> {
                Ty init = expr(r.init(), s, allowAgg);
                Ty lt = expr(r.list(), s, allowAgg);
                if (!lt.any() && lt.k() != K.LIST) {
                    throw typeMismatchErr("List", lt);
                }
                Scope inner = s.copy();
                inner.vars.put(r.acc(), init);
                inner.vars.put(r.var(), lt.k() == K.LIST && lt.elem() != null ? lt.elem() : Ty.ANY);
                Ty body = expr(r.body(), inner, false);
                yield init.equals(body) ? init : Ty.ANY;
            }
            case MapProj mp -> {
                Ty t = expr(mp.target(), s, allowAgg);
                for (Expr v : mp.values()) {
                    if (v != null) {
                        expr(v, s, allowAgg);
                    }
                }
                yield Ty.MAP;
            }
            case PatternPred pp -> {
                patternExpr(pp.pattern(), s);
                yield Ty.BOOL;
            }
            case ExistsSub es -> {
                subquery(es.patterns(), es.where(), es.query(), s);
                yield Ty.BOOL;
            }
            case CountSub cs -> {
                subquery(cs.patterns(), cs.where(), cs.query(), s);
                yield Ty.INT;
            }
            case CollectSub cs -> {
                query(cs.query(), s, true);
                yield Ty.list(null);
            }
            case PathFn pf -> {
                Scope inner = s.copy();
                declarePattern(pf.pattern(), inner, new HashSet<>(), "MATCH");
                yield Ty.PATH;
            }
        };
    }

    private void patternExpr(PatternPart p, Scope s) {
        for (int i = 0; i <= p.relCount(); i++) {
            NodePat n = p.node(i);
            if (n.props() != null) {
                expr(n.props(), s);
            }
            if (i < p.relCount() && p.rel(i).props() != null) {
                expr(p.rel(i).props(), s);
            }
            if (n.var() != null && !s.vars.containsKey(n.var())) {
                throw CypherException.syntax("Variable `" + n.var() + "` not defined");
            }
            if (i < p.relCount() && p.rel(i).var() != null && !s.vars.containsKey(p.rel(i).var())) {
                throw CypherException.syntax("Variable `" + p.rel(i).var() + "` not defined");
            }
            if (n.var() != null && s.vars.containsKey(n.var()) && s.vars.get(n.var()).k() != K.NODE
                    && !s.vars.get(n.var()).any()) {
                throw CypherException.syntax("Type mismatch: " + n.var() + " defined with conflicting type "
                        + s.vars.get(n.var()).name() + " (expected Node)");
            }
        }
    }

    private void subquery(List<PatternPart> patterns, Expr where, Query q, Scope s) {
        if (q != null) {
            for (List<Clause> part : q.parts()) {
                for (Clause c : part) {
                    if (c instanceof Create || c instanceof Merge || c instanceof SetClause || c instanceof Remove
                            || c instanceof Delete || c instanceof Foreach) {
                        throw CypherException.syntax("Invalid use of updating clauses inside an expression subquery");
                    }
                }
            }
            query(q, s, true);
            return;
        }
        Scope inner = s.copy();
        for (PatternPart p : patterns) {
            declarePattern(p, inner, new HashSet<>(), "MATCH");
        }
        if (where != null) {
            needBool(expr(where, inner));
        }
    }

    private Ty paramType(Param p) {
        info.usesParams = true;
        if (knownParams == null || !knownParams.containsKey(p.name())) {
            return Ty.ANY;
        }
        return typeOfValue(knownParams.get(p.name()));
    }

    static Ty typeOfValue(Object v) {
        if (v == null) {
            return Ty.NULL;
        }
        if (v instanceof Boolean) {
            return Ty.BOOL;
        }
        if (v instanceof Long) {
            return Ty.INT;
        }
        if (v instanceof Double) {
            return Ty.ANY; // Neo4j reports float parameter misuse at runtime
        }
        if (v instanceof String) {
            return Ty.STR;
        }
        if (v instanceof List<?> l) {
            Ty elem = null;
            boolean first = true;
            for (Object o : l) {
                Ty t = typeOfValue(o);
                if (first) {
                    elem = t;
                    first = false;
                } else if (elem != null && !elem.equals(t)) {
                    elem = null;
                }
            }
            return Ty.list(elem);
        }
        if (v instanceof Map) {
            return Ty.MAP;
        }
        if (v instanceof java.time.LocalDate) {
            return new Ty(K.DATE, null);
        }
        if (v instanceof java.time.OffsetTime) {
            return new Ty(K.TIME, null);
        }
        if (v instanceof java.time.LocalTime) {
            return new Ty(K.LTIME, null);
        }
        if (v instanceof java.time.LocalDateTime) {
            return new Ty(K.LDT, null);
        }
        if (v instanceof java.time.ZonedDateTime) {
            return new Ty(K.DT, null);
        }
        if (v instanceof Values.DurationV) {
            return Ty.DUR;
        }
        if (v instanceof Values.PointV) {
            return Ty.POINT;
        }
        return Ty.ANY;
    }

    private static Ty literalType(Object v) {
        if (v == null) {
            return Ty.NULL;
        }
        if (v instanceof Boolean) {
            return Ty.BOOL;
        }
        if (v instanceof Long) {
            return Ty.INT;
        }
        if (v instanceof Double) {
            return Ty.FLOAT;
        }
        return Ty.STR;
    }

    private Ty binaryType(Binary b, Scope s, boolean allowAgg) {
        Ty l = expr(b.left(), s, allowAgg), r = expr(b.right(), s, allowAgg);
        char op = b.op();
        boolean known = !l.any() && !r.any();
        switch (op) {
            case '+' -> {
                if (l.k() == K.LIST || r.k() == K.LIST) {
                    return Ty.list(null);
                }
                for (Ty t : new Ty[] {l, r}) {
                    if (!t.any() && !(t.number() || t.k() == K.STR || t.k() == K.LIST || t.k() == K.DUR || t.temporal())) {
                        throw CypherException.syntax("Type mismatch: expected Float, Integer, String, List<T>, Duration, Date, "
                                + "Time, LocalTime, LocalDateTime or DateTime but was " + t.name());
                    }
                }
                if (known) {
                    if (l.k() == K.STR && (r.number() || r.k() == K.STR)) {
                        return Ty.STR;
                    }
                    if (r.k() == K.STR && l.number()) {
                        return Ty.STR;
                    }
                    if (l.number() && r.number()) {
                        return l.k() == K.INT && r.k() == K.INT ? Ty.INT : (l.k() == K.FLOAT || r.k() == K.FLOAT ? Ty.FLOAT : Ty.NUM);
                    }
                    if (l.k() == K.LIST || r.k() == K.LIST) {
                        return Ty.list(null);
                    }
                    if ((l.k() == K.STR && !(r.number() || r.k() == K.STR)) || (r.k() == K.STR && !(l.number() || l.k() == K.STR))) {
                        throw CypherException.syntax("Type mismatch: expected Float, Integer, String or List<T> but was "
                                + (l.k() == K.STR ? r : l).name());
                    }
                    if ((l.number() && (r.temporal() || r.k() == K.DUR)) || (r.number() && (l.temporal() || l.k() == K.DUR))) {
                        throw CypherException.syntax("Type mismatch: expected Float, Integer, String or List<T> but was "
                                + (l.number() ? r : l).name());
                    }
                }
                return Ty.ANY;
            }
            case '-', '*', '/', '%', '^' -> {
                for (Ty t : new Ty[] {l, r}) {
                    boolean ok = t.any() || t.number() || ((op == '-' || op == '*' || op == '/') && (t.k() == K.DUR || t.temporal()));
                    if (!ok) {
                        throw CypherException.syntax("Type mismatch: expected Float, Integer" + (op == '%' || op == '^' ? ""
                                : " or Duration") + " but was " + t.name());
                    }
                }
                if (l.number() && r.number()) {
                    if (op == '^') {
                        return Ty.FLOAT;
                    }
                    return l.k() == K.INT && r.k() == K.INT ? Ty.INT : (l.k() == K.FLOAT || r.k() == K.FLOAT ? Ty.FLOAT : Ty.NUM);
                }
                return Ty.ANY;
            }
            default -> {
                return Ty.ANY;
            }
        }
    }

    /** Allowed static kinds per argument position of common functions (null entry = anything). */
    private static final Map<String, List<Set<K>>> ARGS = new LinkedHashMap<>();

    private static void sig(String names, Set<K>... args) {
        for (String n : names.split(",")) {
            ARGS.put(n, java.util.Arrays.asList(args));
        }
    }

    @SafeVarargs
    private static Set<K> ks(K... k) {
        return Set.of(k);
    }

    static {
        Set<K> num = ks(K.INT, K.FLOAT, K.NUM);
        Set<K> str = ks(K.STR);
        Set<K> intg = ks(K.INT);
        sig("abs,ceil,floor,sign,sqrt,exp,log,log10,sin,cos,tan,cot,asin,acos,atan,degrees,radians,haversin,isnan", num);
        sig("atan2", num, num);
        sig("round", num, intg, str);
        sig("tolower,toupper,lower,upper,trim,ltrim,rtrim,char_length,character_length,normalize", str);
        sig("left,right", str, intg);
        sig("replace", str, str, str);
        sig("split", str, str);
        sig("substring", str, intg, intg);
        sig("range", intg, intg, intg);
        sig("size", ks(K.STR, K.LIST));
        sig("head,last,tail", ks(K.LIST));
        sig("reverse", ks(K.STR, K.LIST));
        sig("keys,properties", ks(K.MAP, K.NODE, K.REL));
        sig("labels", ks(K.NODE));
        sig("type", ks(K.REL));
        sig("nodes,relationships,rels,length", ks(K.PATH));
        sig("startnode,endnode", ks(K.REL));
        sig("id,elementid", ks(K.NODE, K.REL));
        sig("isempty", ks(K.STR, K.LIST, K.MAP));
        sig("percentilecont,percentiledisc", num, num);
        sig("stdev,stdevp,sum,avg", ks(K.INT, K.FLOAT, K.NUM, K.DUR));
        sig("point", ks(K.MAP));
    }

    private Ty callType(Call c, Scope s, boolean allowAgg) {
        String n = c.name().toLowerCase(Locale.ROOT);
        int[] ar = Funcs.arity(c.name());
        if (ar == null) {
            throw CypherException.syntax("Unknown function '" + c.name() + "'");
        }
        int argc = c.args().size();
        if (argc < ar[0]) {
            throw CypherException.syntax("Insufficient parameters for function '" + c.name() + "'");
        }
        if (ar[1] >= 0 && argc > ar[1]) {
            throw CypherException.syntax("Too many parameters for function '" + c.name() + "'");
        }
        boolean agg = Funcs.isAggregate(n);
        if (agg && !allowAgg) {
            throw CypherException.syntax("Invalid use of aggregating function " + c.name() + "(...) in this context");
        }
        if (c.distinct() && !agg) {
            throw CypherException.syntax("Invalid use of DISTINCT with function '" + c.name() + "'");
        }
        List<Ty> ts = new ArrayList<>();
        for (Expr a : c.args()) {
            ts.add(expr(a, s, allowAgg && !agg ? true : allowAgg));
        }
        List<Set<K>> spec = ARGS.get(n);
        if (spec != null) {
            for (int i = 0; i < ts.size() && i < spec.size(); i++) {
                Ty t = ts.get(i);
                Set<K> allowed = spec.get(i);
                if (allowed != null && !t.any() && !allowed.contains(t.k())) {
                    if (n.equals("length") && (t.k() == K.STR || t.k() == K.LIST)) {
                        throw CypherException.syntax("Type mismatch: expected Path but was " + t.name());
                    }
                    throw CypherException.syntax("Type mismatch: expected " + describeSet(allowed) + " but was " + t.name());
                }
            }
        }
        if (agg) {
            for (Expr a : c.args()) {
                if (walkAny(a, x -> x instanceof Call fc && (fc.name().equalsIgnoreCase("rand")
                        || fc.name().equalsIgnoreCase("randomUUID")))) {
                    throw CypherException.syntax("Non-constant expression in aggregation: rand() cannot be used inside an "
                            + "aggregation");
                }
            }
        }
        return switch (n) {
            case "count", "size", "length", "char_length", "character_length", "id", "sign", "timestamp", "tointeger",
                    "tointegerornull" -> Ty.INT;
            case "sum", "avg", "min", "max", "abs" -> !ts.isEmpty() && (n.equals("abs") || n.equals("sum") || n.equals("min")
                    || n.equals("max")) ? (ts.get(0).number() || n.equals("min") || n.equals("max") ? ts.get(0) : Ty.ANY) : Ty.ANY;
            case "tostring", "tolower", "toupper", "lower", "upper", "trim", "ltrim", "rtrim", "left", "right", "replace",
                    "substring", "elementid", "type", "randomuuid", "tostringornull" -> Ty.STR;
            case "tofloat", "tofloatornull", "sqrt", "exp", "log", "log10", "sin", "cos", "tan", "cot", "asin", "acos", "atan",
                    "atan2", "pi", "e", "rand", "degrees", "radians", "haversin", "ceil", "floor", "round", "stdev", "stdevp" -> Ty.FLOAT;
            case "toboolean", "tobooleanornull", "isnan", "isempty", "exists" -> Ty.BOOL;
            case "collect", "range", "keys", "labels", "nodes", "relationships", "rels", "tail", "split", "reverse" ->
                    n.equals("reverse") && !ts.isEmpty() && ts.get(0).k() == K.STR ? Ty.STR : Ty.list(null);
            case "startnode", "endnode" -> Ty.NODE;
            case "properties" -> Ty.MAP;
            case "point" -> Ty.POINT;
            default -> Ty.ANY;
        };
    }

    private static String describeSet(Set<K> ks) {
        List<String> names = new ArrayList<>();
        for (K k : List.of(K.BOOL, K.FLOAT, K.INT, K.NUM, K.STR, K.MAP, K.NODE, K.REL, K.PATH, K.LIST)) {
            if (ks.contains(k)) {
                names.add(new Ty(k, null).name());
            }
        }
        if (ks.contains(K.NUM) && ks.contains(K.INT)) {
            return "Float or Integer";
        }
        return String.join(", ", names);
    }

    // ------------------------------------------------------------------------------------------ tree walking

    /** Canonical text of an expression (structural identity ignoring source positions). */
    static String norm(Expr e) {
        return switch (e) {
            case Lit l -> l.value() instanceof String s ? "'" + s + "'" : String.valueOf(l.value());
            case Param p -> "$" + p.name();
            case Var v -> v.name();
            case Prop p -> norm(p.target()) + "." + p.key();
            case Index i -> norm(i.target()) + "[" + norm(i.index()) + "]";
            case Slice s -> norm(s.target()) + "[" + (s.from() == null ? "" : norm(s.from())) + ".."
                    + (s.to() == null ? "" : norm(s.to())) + "]";
            case ListLit l -> "[" + String.join(",", l.items().stream().map(Analyzer::norm).toList()) + "]";
            case MapLit m -> "{" + String.join(",", java.util.stream.IntStream.range(0, m.keys().size())
                    .mapToObj(i -> m.keys().get(i) + ":" + norm(m.values().get(i))).toList()) + "}";
            case Unary u -> "(" + u.op() + norm(u.operand()) + ")";
            case Binary b -> "(" + norm(b.left()) + b.op() + norm(b.right()) + ")";
            case Not n -> "NOT " + norm(n.operand());
            case And a -> "(" + norm(a.left()) + " AND " + norm(a.right()) + ")";
            case Or o -> "(" + norm(o.left()) + " OR " + norm(o.right()) + ")";
            case Xor x -> "(" + norm(x.left()) + " XOR " + norm(x.right()) + ")";
            case Cmp c -> "(" + norm(c.left()) + c.op() + norm(c.right()) + ")";
            case IsNull n -> "(" + norm(n.operand()) + " IS" + (n.negated() ? " NOT" : "") + " NULL)";
            case HasLabels h -> norm(h.target()) + ":" + String.join(":", h.labels());
            case InList i -> "(" + norm(i.element()) + " IN " + norm(i.list()) + ")";
            case StrOp so -> "(" + norm(so.left()) + " " + so.op() + " " + norm(so.right()) + ")";
            case RegexMatch r -> "(" + norm(r.left()) + "=~" + norm(r.right()) + ")";
            case Call c -> c.name().toLowerCase(Locale.ROOT) + "(" + (c.distinct() ? "DISTINCT " : "")
                    + String.join(",", c.args().stream().map(Analyzer::norm).toList()) + ")";
            case CountStar cs -> "count(*)";
            default -> e.toString();
        };
    }

    static List<Expr> children(Expr e) {
        List<Expr> out = new ArrayList<>();
        switch (e) {
            case Prop p -> out.add(p.target());
            case Index i -> {
                out.add(i.target());
                out.add(i.index());
            }
            case Slice s -> {
                out.add(s.target());
                if (s.from() != null) {
                    out.add(s.from());
                }
                if (s.to() != null) {
                    out.add(s.to());
                }
            }
            case ListLit l -> out.addAll(l.items());
            case MapLit m -> out.addAll(m.values());
            case Unary u -> out.add(u.operand());
            case Binary b -> {
                out.add(b.left());
                out.add(b.right());
            }
            case Not n -> out.add(n.operand());
            case And a -> {
                out.add(a.left());
                out.add(a.right());
            }
            case Or o -> {
                out.add(o.left());
                out.add(o.right());
            }
            case Xor x -> {
                out.add(x.left());
                out.add(x.right());
            }
            case Cmp c -> {
                out.add(c.left());
                out.add(c.right());
            }
            case IsNull n -> out.add(n.operand());
            case HasLabels h -> out.add(h.target());
            case InList i -> {
                out.add(i.element());
                out.add(i.list());
            }
            case StrOp s -> {
                out.add(s.left());
                out.add(s.right());
            }
            case RegexMatch r -> {
                out.add(r.left());
                out.add(r.right());
            }
            case Call c -> out.addAll(c.args());
            case CaseExpr c -> {
                if (c.subject() != null) {
                    out.add(c.subject());
                }
                out.addAll(c.whens());
                out.addAll(c.thens());
                if (c.otherwise() != null) {
                    out.add(c.otherwise());
                }
            }
            case ListComp lc -> {
                out.add(lc.list());
                if (lc.where() != null) {
                    out.add(lc.where());
                }
                if (lc.map() != null) {
                    out.add(lc.map());
                }
            }
            case PatternComp pc -> {
                if (pc.where() != null) {
                    out.add(pc.where());
                }
                out.add(pc.map());
            }
            case Quantifier q -> {
                out.add(q.list());
                if (q.where() != null) {
                    out.add(q.where());
                }
            }
            case Reduce r -> {
                out.add(r.init());
                out.add(r.list());
                out.add(r.body());
            }
            case MapProj mp -> {
                out.add(mp.target());
                for (Expr v : mp.values()) {
                    if (v != null) {
                        out.add(v);
                    }
                }
            }
            default -> {
            }
        }
        return out;
    }

    /** Pre-order walk; the visitor returns false to skip a node's children. */
    static void walk(Expr e, Predicate<Expr> visitor) {
        if (!visitor.test(e)) {
            return;
        }
        for (Expr c : children(e)) {
            walk(c, visitor);
        }
    }

    static boolean walkAny(Expr e, Predicate<Expr> match) {
        boolean[] found = {false};
        walk(e, n -> {
            if (match.test(n)) {
                found[0] = true;
            }
            return !found[0];
        });
        return found[0];
    }
}
