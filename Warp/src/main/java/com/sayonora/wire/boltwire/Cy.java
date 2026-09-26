package com.sayonora.wire.boltwire;

import java.util.List;

/** The Cypher syntax tree ({@link CypherParser} builds it, {@link Analyzer} checks it, {@link Executor} runs it). */
final class Cy {

    private Cy() {
    }

    // ------------------------------------------------------------------------------------ expressions

    sealed interface Expr permits Lit, Param, Var, Prop, Index, Slice, ListLit, MapLit, Unary, Binary, Not, And, Or, Xor,
            Cmp, IsNull, HasLabels, Call, CountStar, CaseExpr, ListComp, PatternComp, Quantifier, Reduce, MapProj,
            PatternPred, ExistsSub, CountSub, CollectSub, InList, StrOp, RegexMatch, PathFn {
    }

    /** A literal value: null, Boolean, Long, Double, String. */
    record Lit(Object value) implements Expr {
    }

    record Param(String name, int pos) implements Expr {
    }

    record Var(String name, int pos) implements Expr {
    }

    record Prop(Expr target, String key, int pos) implements Expr {
    }

    record Index(Expr target, Expr index) implements Expr {
    }

    record Slice(Expr target, Expr from, Expr to) implements Expr {
    }

    record ListLit(List<Expr> items) implements Expr {
    }

    record MapLit(List<String> keys, List<Expr> values) implements Expr {
    }

    /** Unary minus / plus. */
    record Unary(char op, Expr operand) implements Expr {
    }

    /** + - * / % ^ . */
    record Binary(char op, Expr left, Expr right) implements Expr {
    }

    record Not(Expr operand) implements Expr {
    }

    record And(Expr left, Expr right) implements Expr {
    }

    record Or(Expr left, Expr right) implements Expr {
    }

    record Xor(Expr left, Expr right) implements Expr {
    }

    /** One comparison; a chain {@code a < b < c} is parsed as And of Cmps. op: = &lt;&gt; &lt; &gt; &lt;= &gt;= */
    record Cmp(String op, Expr left, Expr right) implements Expr {
    }

    record IsNull(Expr operand, boolean negated) implements Expr {
    }

    record HasLabels(Expr target, List<String> labels) implements Expr {
    }

    record InList(Expr element, Expr list) implements Expr {
    }

    /** STARTS WITH / ENDS WITH / CONTAINS. */
    record StrOp(String op, Expr left, Expr right) implements Expr {
    }

    record RegexMatch(Expr left, Expr right) implements Expr {
    }

    /** name may be namespaced (a.b.c). */
    record Call(String name, boolean distinct, List<Expr> args, int pos) implements Expr {
    }

    record CountStar() implements Expr {
    }

    record CaseExpr(Expr subject, List<Expr> whens, List<Expr> thens, Expr otherwise) implements Expr {
    }

    /** [x IN list WHERE pred | map] */
    record ListComp(String var, Expr list, Expr where, Expr map) implements Expr {
    }

    record PatternComp(PatternPart pattern, Expr where, Expr map) implements Expr {
    }

    /** ALL/ANY/NONE/SINGLE (x IN list WHERE pred). */
    record Quantifier(String kind, String var, Expr list, Expr where) implements Expr {
    }

    record Reduce(String acc, Expr init, String var, Expr list, Expr body) implements Expr {
    }

    /** n{.a, .*, b: expr, c} */
    record MapProj(Expr target, List<String> kinds, List<String> keys, List<Expr> values) implements Expr {
    }

    /** A bare pattern used as a predicate: {@code (a)-[:T]->(b)}. */
    record PatternPred(PatternPart pattern) implements Expr {
    }

    record ExistsSub(List<PatternPart> patterns, Expr where, Query query) implements Expr {
    }

    record CountSub(List<PatternPart> patterns, Expr where, Query query) implements Expr {
    }

    record CollectSub(Query query) implements Expr {
    }

    /** shortestPath(p) / allShortestPaths(p) used as an expression. */
    record PathFn(PatternPart pattern) implements Expr {
    }

    // ------------------------------------------------------------------------------------ patterns

    enum Dir { OUT, IN, BOTH }

    record NodePat(String var, List<String> labels, Expr props, int pos) {
    }

    /** minHops/maxHops only when {@code varLen}; maxHops = -1 means unbounded. */
    record RelPat(String var, List<String> types, Expr props, Dir dir, boolean varLen, long minHops, long maxHops, int pos) {
    }

    /** elements alternate NodePat, RelPat, NodePat ... shortest: 0 none, 1 shortestPath, 2 allShortestPaths. */
    record PatternPart(String pathVar, List<Object> elements, int shortest) {
        NodePat node(int i) {
            return (NodePat) elements.get(2 * i);
        }

        RelPat rel(int i) {
            return (RelPat) elements.get(2 * i + 1);
        }

        int relCount() {
            return elements.size() / 2;
        }
    }

    // ------------------------------------------------------------------------------------ clauses

    sealed interface Clause permits Match, Unwind, With, Return, Create, Merge, SetClause, Remove, Delete, CallClause,
            Foreach, CallSubquery, SchemaCmd {
    }

    record Match(boolean optional, List<PatternPart> parts, Expr where, int pos) implements Clause {
    }

    record Unwind(Expr expr, String var) implements Clause {
    }

    record ProjItem(Expr expr, String alias, String text) {
    }

    record SortItem(Expr expr, boolean desc) {
    }

    /** star = leading '*'. */
    record Projection(boolean distinct, boolean star, List<ProjItem> items, List<SortItem> orderBy, Expr skip, Expr limit) {
    }

    record With(Projection proj, Expr where) implements Clause {
    }

    record Return(Projection proj) implements Clause {
    }

    record Create(List<PatternPart> parts) implements Clause {
    }

    record Merge(PatternPart part, List<SetItem> onCreate, List<SetItem> onMatch) implements Clause {
    }

    /** kind: PROP (target is a Prop), REPLACE (n = map), MERGE (n += map), LABELS (n:L1:L2). */
    record SetItem(String kind, Expr target, Expr value, List<String> labels) {
    }

    record SetClause(List<SetItem> items) implements Clause {
    }

    /** kind: PROP or LABELS. */
    record RemoveItem(String kind, Expr target, List<String> labels) {
    }

    record Remove(List<RemoveItem> items) implements Clause {
    }

    record Delete(boolean detach, List<Expr> targets) implements Clause {
    }

    record YieldItem(String name, String alias) {
    }

    /** args == null: implicit arguments (standalone CALL only). yields == null: no YIELD; star: YIELD *. */
    record CallClause(String name, List<Expr> args, List<YieldItem> yields, boolean yieldStar, Expr where,
            boolean standalone) implements Clause {
    }

    record Foreach(String var, Expr list, List<Clause> body) implements Clause {
    }

    record CallSubquery(Query query) implements Clause {
    }

    /** CREATE/DROP CONSTRAINT|INDEX, SHOW ..., handled outside the row pipeline. */
    record SchemaCmd(String kind, String name, boolean ifNotExists, boolean ifExists, String entity, String label,
            List<String> properties, String constraintType, String indexType, String showWhat, List<String> options,
            String text) implements Clause {
    }

    // ------------------------------------------------------------------------------------ query

    /** A UNION chain (parts.size() == 1 for a plain query). all[i] tells whether part i+1 was joined with UNION ALL. */
    record Query(List<List<Clause>> parts, List<Boolean> all, String text) {
    }
}
