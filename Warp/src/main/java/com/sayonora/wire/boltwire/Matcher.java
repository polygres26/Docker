package com.sayonora.wire.boltwire;

import com.sayonora.wire.boltwire.Cy.*;
import com.sayonora.wire.boltwire.Values.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/** Pattern matching (MATCH / pattern predicates / pattern comprehensions / MERGE lookup) by backtracking over the graph. */
final class Matcher {

    private final Eval ev;
    private final Exec x;

    Matcher(Eval ev) {
        this.ev = ev;
        this.x = ev.x;
    }

    /** All bindings extending {@code row} that satisfy every part (relationships unique across parts) and {@code where}. */
    List<Map<String, Object>> matchAll(List<PatternPart> parts, Map<String, Object> row, Expr where) {
        List<Map<String, Object>> out = new ArrayList<>();
        Map<String, Object> b = new HashMap<>(row);
        Map<String, Object> pushed = where == null ? Map.of() : pushdownEqualities(where, parts, row);
        matchParts(parts, 0, b, new HashSet<>(), pushed, r -> {
            if (where == null || ev.isTrue(where, r)) {
                out.add(new HashMap<>(r));
            }
        });
        return out;
    }

    boolean matchAny(List<PatternPart> parts, Map<String, Object> row, Expr where) {
        boolean[] found = {false};
        Map<String, Object> b = new HashMap<>(row);
        try {
            matchParts(parts, 0, b, new HashSet<>(), Map.of(), r -> {
                if (where == null || ev.isTrue(where, r)) {
                    found[0] = true;
                    throw STOP;
                }
            });
        } catch (Stop s) {
            // first solution found
        }
        return found[0];
    }

    private static final class Stop extends RuntimeException {
        Stop() {
            super(null, null, false, false);
        }
    }

    private static final Stop STOP = new Stop();

    private void matchParts(List<PatternPart> parts, int i, Map<String, Object> b, Set<Long> used,
            Map<String, Object> pushed, Consumer<Map<String, Object>> done) {
        if (i == parts.size()) {
            done.accept(b);
            return;
        }
        PatternPart part = parts.get(i);
        Consumer<Map<String, Object>> next = r -> matchParts(parts, i + 1, r, used, pushed, done);
        if (part.shortest() != 0) {
            matchShortest(part, b, pushed, next);
        } else {
            matchPart(part, b, used, pushed, next);
        }
    }

    /** Equality conjuncts {@code var.prop = <expr not depending on unbound variables>} of a WHERE that can be pushed to scans. */
    private Map<String, Object> pushdownEqualities(Expr where, List<PatternPart> parts, Map<String, Object> row) {
        Map<String, Object> out = new HashMap<>();
        Set<String> patternVars = new HashSet<>();
        for (PatternPart p : parts) {
            for (int i = 0; i <= p.relCount(); i++) {
                if (p.node(i).var() != null) {
                    patternVars.add(p.node(i).var());
                }
            }
        }
        collect(where, patternVars, row, out);
        return out;
    }

    private void collect(Expr e, Set<String> patternVars, Map<String, Object> row, Map<String, Object> out) {
        if (e instanceof And a) {
            collect(a.left(), patternVars, row, out);
            collect(a.right(), patternVars, row, out);
        } else if (e instanceof Cmp c && c.op().equals("=")) {
            tryPush(c.left(), c.right(), patternVars, row, out);
            tryPush(c.right(), c.left(), patternVars, row, out);
        }
    }

    private void tryPush(Expr prop, Expr value, Set<String> patternVars, Map<String, Object> row, Map<String, Object> out) {
        if (prop instanceof Prop p && p.target() instanceof Var v && patternVars.contains(v.name())
                && !row.containsKey(v.name()) && (value instanceof Lit || value instanceof Param)) {
            try {
                Object val = ev.eval(value, row);
                if (val != null) {
                    out.put(v.name() + "\u0000" + p.key(), val);
                }
            } catch (CypherException ex) {
                // evaluated later with the proper error
            }
        }
    }

    // ------------------------------------------------------------------------------------------ one path pattern

    private record Step(int rel, int from, int to, boolean backward) {
    }

    private void matchPart(PatternPart part, Map<String, Object> b, Set<Long> used, Map<String, Object> pushed,
            Consumer<Map<String, Object>> done) {
        int n = part.relCount();
        int s = chooseStart(part, b);
        List<Step> steps = new ArrayList<>();
        for (int k = s; k < n; k++) {
            steps.add(new Step(k, k, k + 1, false));
        }
        for (int k = s - 1; k >= 0; k--) {
            steps.add(new Step(k, k + 1, k, true));
        }
        NodeV[] nodeVals = new NodeV[n + 1];
        List<List<RelV>> segRels = new ArrayList<>();
        List<List<NodeV>> segMid = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            segRels.add(null);
            segMid.add(null);
        }
        NodePat sp = part.node(s);
        for (NodeV cand : candidates(sp, b, pushed)) {
            List<String> undo = new ArrayList<>();
            if (!bindNode(sp, cand, b, undo)) {
                unbind(b, undo);
                continue;
            }
            nodeVals[s] = cand;
            runSteps(part, steps, 0, nodeVals, segRels, segMid, b, used, pushed, done);
            unbind(b, undo);
        }
    }

    private int chooseStart(PatternPart part, Map<String, Object> b) {
        int n = part.relCount();
        for (int i = 0; i <= n; i++) {
            String v = part.node(i).var();
            if (v != null && b.containsKey(v)) {
                return i;
            }
        }
        // prefer a node with labels / properties
        int best = 0, bestScore = -1;
        for (int i = 0; i <= n; i++) {
            NodePat np = part.node(i);
            int score = np.labels().size() * 2 + (np.props() != null ? 3 : 0);
            if (score > bestScore) {
                bestScore = score;
                best = i;
            }
        }
        return best;
    }

    private List<NodeV> candidates(NodePat np, Map<String, Object> b, Map<String, Object> pushed) {
        if (np.var() != null && b.containsKey(np.var())) {
            Object v = b.get(np.var());
            if (v instanceof NodeV nv && !nv.deleted && nodeOk(np, nv, b)) {
                return List.of(nv);
            }
            if (v != null && !(v instanceof NodeV)) {
                throw CypherException.type("Type mismatch: expected Node but was " + Values.typeName(v));
            }
            return List.of();
        }
        Map<String, Object> eq = new LinkedHashMap<>();
        if (np.props() != null) {
            Object pv = ev.eval(np.props(), b);
            if (pv instanceof Map<?, ?> m) {
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (e.getValue() == null) {
                        return List.of();
                    }
                    eq.put((String) e.getKey(), e.getValue());
                }
            } else if (pv != null) {
                throw Eval.typeMismatch("Map", pv);
            } else {
                return List.of();
            }
        }
        if (np.var() != null) {
            String prefix = np.var() + "\u0000";
            for (Map.Entry<String, Object> e : pushed.entrySet()) {
                if (e.getKey().startsWith(prefix)) {
                    eq.putIfAbsent(e.getKey().substring(prefix.length()), e.getValue());
                }
            }
        }
        List<NodeV> raw = x.scan(np.labels(), eq);
        List<NodeV> out = new ArrayList<>(raw.size());
        for (NodeV n : raw) {
            if (nodeOk(np, n, b)) {
                out.add(n);
            }
        }
        return out;
    }

    private boolean nodeOk(NodePat np, NodeV n, Map<String, Object> b) {
        if (n.deleted) {
            return false;
        }
        if (!n.labels.containsAll(np.labels())) {
            return false;
        }
        if (np.props() != null) {
            Object pv = ev.eval(np.props(), b);
            if (!(pv instanceof Map<?, ?> m)) {
                return false;
            }
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!Boolean.TRUE.equals(Values.equal(n.props.get((String) e.getKey()), e.getValue()))) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean bindNode(NodePat np, NodeV n, Map<String, Object> b, List<String> undo) {
        if (np.var() == null) {
            return true;
        }
        if (b.containsKey(np.var())) {
            Object cur = b.get(np.var());
            return cur instanceof NodeV c && c.id == n.id;
        }
        b.put(np.var(), n);
        undo.add(np.var());
        return true;
    }

    private static void unbind(Map<String, Object> b, List<String> undo) {
        for (String v : undo) {
            b.remove(v);
        }
    }

    private void runSteps(PatternPart part, List<Step> steps, int si, NodeV[] nodeVals, List<List<RelV>> segRels,
            List<List<NodeV>> segMid, Map<String, Object> b, Set<Long> used, Map<String, Object> pushed,
            Consumer<Map<String, Object>> done) {
        x.checkDeadline();
        if (si == steps.size()) {
            String pv = part.pathVar();
            if (pv != null) {
                List<NodeV> pn = new ArrayList<>();
                List<RelV> pr = new ArrayList<>();
                pn.add(nodeVals[0]);
                for (int k = 0; k < part.relCount(); k++) {
                    List<NodeV> mids = segMid.get(k);
                    pn.addAll(mids);
                    pr.addAll(segRels.get(k));
                    if (!segRels.get(k).isEmpty()) {
                        pn.add(nodeVals[k + 1]);
                    }
                }
                b.put(pv, new PathV(pn, pr));
                done.accept(b);
                b.remove(pv);
            } else {
                done.accept(b);
            }
            return;
        }
        Step st = steps.get(si);
        RelPat rp = part.rel(st.rel());
        NodePat target = part.node(st.to());
        NodeV from = nodeVals[st.from()];
        Dir dir = rp.dir();
        if (st.backward()) {
            dir = dir == Dir.OUT ? Dir.IN : dir == Dir.IN ? Dir.OUT : Dir.BOTH;
        }
        int dirInt = dir == Dir.OUT ? 0 : dir == Dir.IN ? 1 : 2;
        Set<String> types = rp.types().isEmpty() ? null : new LinkedHashSet<>(rp.types());
        Object relProps = rp.props() == null ? null : ev.eval(rp.props(), b);
        if (rp.props() != null && !(relProps instanceof Map)) {
            if (relProps == null) {
                return;
            }
            throw Eval.typeMismatch("Map", relProps);
        }

        if (!rp.varLen()) {
            Object preRel = rp.var() != null ? b.get(rp.var()) : null;
            boolean relBound = rp.var() != null && b.containsKey(rp.var());
            if (relBound && !(preRel instanceof RelV)) {
                if (preRel == null) {
                    return;
                }
                throw Eval.typeMismatch("Relationship", preRel);
            }
            for (PgGraphStore.Adj adj : x.expand(from, dirInt, types)) {
                RelV r = adj.rel();
                if (used.contains(r.id)) {
                    continue;
                }
                if (relBound && ((RelV) preRel).id != r.id) {
                    continue;
                }
                if (relProps != null && !propsMatch(r.props, (Map<?, ?>) relProps)) {
                    continue;
                }
                NodeV other = adj.other();
                // for BOTH with a self loop `other` is the same node: fine
                if (!nodeOk(target, other, b)) {
                    continue;
                }
                List<String> undo = new ArrayList<>();
                if (!bindNode(target, other, b, undo)) {
                    unbind(b, undo);
                    continue;
                }
                if (rp.var() != null && !relBound) {
                    b.put(rp.var(), r);
                    undo.add(rp.var());
                }
                used.add(r.id);
                nodeVals[st.to()] = other;
                segRels.set(st.rel(), List.of(r));
                segMid.set(st.rel(), List.of());
                if (st.backward()) {
                    // orientation is irrelevant for a single hop
                }
                runSteps(part, steps, si + 1, nodeVals, segRels, segMid, b, used, pushed, done);
                used.remove(r.id);
                unbind(b, undo);
            }
            return;
        }

        // variable length: enumerate trails
        long min = rp.minHops(), max = rp.maxHops();
        ArrayDeque<RelV> trail = new ArrayDeque<>();
        ArrayDeque<NodeV> mids = new ArrayDeque<>();
        varLen(part, steps, si, rp, target, from, from, dirInt, types, (Map<?, ?>) relProps, min, max, trail, mids, nodeVals,
                segRels, segMid, b, used, pushed, done, 0);
    }

    private void varLen(PatternPart part, List<Step> steps, int si, RelPat rp, NodePat target, NodeV start, NodeV cur,
            int dirInt, Set<String> types, Map<?, ?> relProps, long min, long max, ArrayDeque<RelV> trail,
            ArrayDeque<NodeV> mids, NodeV[] nodeVals, List<List<RelV>> segRels, List<List<NodeV>> segMid,
            Map<String, Object> b, Set<Long> used, Map<String, Object> pushed, Consumer<Map<String, Object>> done,
            int depth) {
        x.checkDeadline();
        Step st = steps.get(si);
        if (depth >= min) {
            // cur is a candidate end node
            List<String> undo = new ArrayList<>();
            if (nodeOk(target, cur, b) && bindNode(target, cur, b, undo)) {
                boolean relOk = true;
                List<RelV> rels = new ArrayList<>(trail);
                List<NodeV> midList = new ArrayList<>(mids);
                if (!midList.isEmpty()) {
                    midList.remove(midList.size() - 1); // last entry of mids is `cur` itself
                }
                if (st.backward()) {
                    Collections.reverse(rels);
                    Collections.reverse(midList);
                }
                if (rp.var() != null) {
                    if (b.containsKey(rp.var())) {
                        Object pre = b.get(rp.var());
                        relOk = pre instanceof List<?> l && sameRels(l, rels);
                    } else {
                        b.put(rp.var(), new ArrayList<Object>(rels));
                        undo.add(rp.var());
                    }
                }
                if (relOk) {
                    nodeVals[st.to()] = cur;
                    segRels.set(st.rel(), rels);
                    segMid.set(st.rel(), midList);
                    runSteps(part, steps, si + 1, nodeVals, segRels, segMid, b, used, pushed, done);
                }
            }
            unbind(b, undo);
        }
        if (max >= 0 && depth >= max) {
            return;
        }
        for (PgGraphStore.Adj adj : x.expand(cur, dirInt, types)) {
            RelV r = adj.rel();
            if (used.contains(r.id) || trailContains(trail, r)) {
                continue;
            }
            if (relProps != null && !propsMatch(r.props, relProps)) {
                continue;
            }
            trail.addLast(r);
            used.add(r.id);
            mids.addLast(adj.other());
            varLen(part, steps, si, rp, target, start, adj.other(), dirInt, types, relProps, min, max, trail, mids, nodeVals,
                    segRels, segMid, b, used, pushed, done, depth + 1);
            mids.removeLast();
            used.remove(r.id);
            trail.removeLast();
        }
    }

    private static boolean trailContains(ArrayDeque<RelV> trail, RelV r) {
        for (RelV t : trail) {
            if (t.id == r.id) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameRels(List<?> l, List<RelV> rels) {
        if (l.size() != rels.size()) {
            return false;
        }
        for (int i = 0; i < l.size(); i++) {
            if (!(l.get(i) instanceof RelV r) || r.id != rels.get(i).id) {
                return false;
            }
        }
        return true;
    }

    private boolean propsMatch(Map<String, Object> have, Map<?, ?> want) {
        for (Map.Entry<?, ?> e : want.entrySet()) {
            if (!Boolean.TRUE.equals(Values.equal(have.get((String) e.getKey()), e.getValue()))) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------------------------------ shortest paths

    private void matchShortest(PatternPart part, Map<String, Object> b, Map<String, Object> pushed,
            Consumer<Map<String, Object>> done) {
        if (part.relCount() != 1) {
            throw CypherException.syntax("shortestPath requires a path pattern with a single relationship");
        }
        NodePat a = part.node(0), z = part.node(1);
        RelPat rp = part.rel(0);
        Set<String> types = rp.types().isEmpty() ? null : new LinkedHashSet<>(rp.types());
        int dirInt = rp.dir() == Dir.OUT ? 0 : rp.dir() == Dir.IN ? 1 : 2;
        long min = rp.varLen() ? rp.minHops() : 1, max = rp.varLen() ? rp.maxHops() : 1;
        Object relProps = rp.props() == null ? null : ev.eval(rp.props(), b);
        for (NodeV from : candidates(a, b, pushed)) {
            List<String> undoA = new ArrayList<>();
            if (!bindNode(a, from, b, undoA)) {
                unbind(b, undoA);
                continue;
            }
            for (NodeV to : candidates(z, b, pushed)) {
                List<String> undoZ = new ArrayList<>();
                if (!bindNode(z, to, b, undoZ)) {
                    unbind(b, undoZ);
                    continue;
                }
                List<List<PgGraphStore.Adj>> paths = shortest(from, to, dirInt, types, (Map<?, ?>) relProps, min, max,
                        part.shortest() == 2);
                for (List<PgGraphStore.Adj> p : paths) {
                    List<String> undo = new ArrayList<>();
                    List<NodeV> pn = new ArrayList<>();
                    List<RelV> pr = new ArrayList<>();
                    pn.add(from);
                    for (PgGraphStore.Adj adj : p) {
                        pr.add(adj.rel());
                        pn.add(adj.other());
                    }
                    if (rp.var() != null && !b.containsKey(rp.var())) {
                        b.put(rp.var(), rp.varLen() ? new ArrayList<Object>(pr) : (pr.isEmpty() ? null : pr.get(0)));
                        undo.add(rp.var());
                    }
                    if (part.pathVar() != null) {
                        b.put(part.pathVar(), new PathV(pn, pr));
                        undo.add(part.pathVar());
                    }
                    done.accept(b);
                    unbind(b, undo);
                }
                unbind(b, undoZ);
            }
            unbind(b, undoA);
        }
    }

    /** Breadth-first search; returns one (or all) shortest path(s) as lists of hops. */
    private List<List<PgGraphStore.Adj>> shortest(NodeV from, NodeV to, int dirInt, Set<String> types, Map<?, ?> relProps,
            long min, long max, boolean all) {
        List<List<PgGraphStore.Adj>> results = new ArrayList<>();
        if (from.id == to.id && min <= 0) {
            results.add(new ArrayList<>());
            return results;
        }
        // frontier of paths (each a list of hops), extended level by level; node distance recorded to prune
        Map<Long, Integer> dist = new HashMap<>();
        dist.put(from.id, 0);
        List<List<PgGraphStore.Adj>> frontier = new ArrayList<>();
        frontier.add(new ArrayList<>());
        List<NodeV> frontierNodes = new ArrayList<>();
        frontierNodes.add(from);
        for (int depth = 1; max < 0 || depth <= max; depth++) {
            x.checkDeadline();
            List<List<PgGraphStore.Adj>> nextFrontier = new ArrayList<>();
            List<NodeV> nextNodes = new ArrayList<>();
            for (int i = 0; i < frontier.size(); i++) {
                List<PgGraphStore.Adj> path = frontier.get(i);
                NodeV cur = frontierNodes.get(i);
                for (PgGraphStore.Adj adj : x.expand(cur, dirInt, types)) {
                    if (relProps != null && !propsMatch(adj.rel().props, relProps)) {
                        continue;
                    }
                    if (usesRel(path, adj.rel())) {
                        continue;
                    }
                    Integer d = dist.get(adj.other().id);
                    if (d != null && d < depth && adj.other().id != to.id) {
                        continue;
                    }
                    if (d != null && d < depth) {
                        continue;
                    }
                    if (d == null) {
                        dist.put(adj.other().id, depth);
                    }
                    List<PgGraphStore.Adj> np = new ArrayList<>(path);
                    np.add(adj);
                    if (adj.other().id == to.id) {
                        if (depth >= min) {
                            results.add(np);
                            if (!all) {
                                return results;
                            }
                        }
                        if (from.id != to.id) {
                            continue; // do not extend beyond the target
                        }
                    }
                    nextFrontier.add(np);
                    nextNodes.add(adj.other());
                }
            }
            if (!results.isEmpty()) {
                return results;
            }
            if (nextFrontier.isEmpty()) {
                return results;
            }
            frontier = nextFrontier;
            frontierNodes = nextNodes;
        }
        return results;
    }

    private static boolean usesRel(List<PgGraphStore.Adj> path, RelV r) {
        for (PgGraphStore.Adj a : path) {
            if (a.rel().id == r.id) {
                return true;
            }
        }
        return false;
    }
}
