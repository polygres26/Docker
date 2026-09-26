package com.sayonora.wire.gremlinwire;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The Gremlin traversal interpreter: executes {@link G.Bytecode} (the form both a script's {@code g.V()...} chain and a driver's
 * bytecode request reduce to) lazily over a {@link GraphStore}, one iterator per step.
 */
final class Engine {

    private Engine() {
    }

    // ------------------------------------------------------------------ traverser + path

    static final class Node {
        final Node parent;
        final Object obj;
        final Set<String> labels;

        Node(Node parent, Object obj, Set<String> labels) {
            this.parent = parent;
            this.obj = obj;
            this.labels = labels;
        }
    }

    static final class Loop {
        final String name;
        final int n;
        final Loop next;

        Loop(String name, int n, Loop next) {
            this.name = name;
            this.n = n;
            this.next = next;
        }
    }

    static final class Tr {
        final Object v;
        final Node path;
        final Object sack;
        final Loop loops;

        Tr(Object v, Node path, Object sack, Loop loops) {
            this.v = v;
            this.path = path;
            this.sack = sack;
            this.loops = loops;
        }

        /** A traverser split from this one with a new current object appended to the path. */
        Tr child(Object nv) {
            return new Tr(nv, new Node(path, nv, Set.of()), sack, loops);
        }

        Tr withSack(Object s) {
            return new Tr(v, path, s, loops);
        }

        Tr withLoops(Loop l) {
            return new Tr(v, path, sack, l);
        }

        Tr labeled(Collection<String> ls) {
            if (path == null) {
                return this;
            }
            Set<String> s = new LinkedHashSet<>(path.labels);
            s.addAll(ls);
            return new Tr(v, new Node(path.parent, path.obj, s), sack, loops);
        }

        /** A fresh traverser (reducing barrier output): new path holding only the value. */
        Tr fresh(Object nv) {
            return new Tr(nv, new Node(null, nv, Set.of()), sack, null);
        }
    }

    static G.Path pathOf(Node n) {
        List<Object> objs = new ArrayList<>();
        List<Set<String>> labs = new ArrayList<>();
        for (Node x = n; x != null; x = x.parent) {
            objs.add(x.obj);
            labs.add(x.labels);
        }
        Collections.reverse(objs);
        Collections.reverse(labs);
        return new G.Path(objs, labs);
    }

    /** Marks "no value" from a modulator (missing property, empty traversal). */
    static final Object NONE = new Object() {
        @Override
        public String toString() {
            return "<none>";
        }
    };

    // ------------------------------------------------------------------ context

    static final class Ctx {
        final GraphStore store;
        final Map<String, Object> side = new LinkedHashMap<>();
        boolean readOnly;
        Random rnd = new Random();
        long deadlineNanos;
        Object sackInit;
        boolean hasSack;
        G.Operator sackMerge;
        BiFunction<G.Closure, Object[], Object> closureCaller;
        /** The traversal contains a repeat() somewhere: its traversers then support loops() (value 0 outside a loop). */
        boolean hasRepeat;
        int repeatLimit = 100_000;

        Ctx(GraphStore store) {
            this.store = store;
        }

        void tick() {
            if (deadlineNanos != 0 && System.nanoTime() > deadlineNanos) {
                throw new G.GremlinError(598, "Evaluation exceeded the configured 'evaluationTimeout' threshold");
            }
        }

        Object call(G.Closure c, Object... args) {
            if (closureCaller == null) {
                throw G.GremlinError.script("closures are not available in this context");
            }
            return closureCaller.apply(c, args);
        }

        void needWrite(String what) {
            if (readOnly) {
                throw G.GremlinError.script("Read-only mode: " + what + " is not allowed");
            }
        }
    }

    // ------------------------------------------------------------------ normalized steps

    static final class Step {
        final String name;
        final Object[] args;
        final List<Object[]> by = new ArrayList<>();
        final List<Object[]> from = new ArrayList<>();
        final List<Object[]> to = new ArrayList<>();
        final List<Object[]> option = new ArrayList<>();
        final List<Object[]> with = new ArrayList<>();
        /** addV / addE property() calls folded into the creation. */
        final List<Object[]> props = new ArrayList<>();
        /** repeat modulators before / after the repeat() call. */
        final List<Object[]> pre = new ArrayList<>();
        final List<Object[]> post = new ArrayList<>();

        Step(String name, Object[] args) {
            this.name = name;
            this.args = args;
        }
    }

    @SuppressWarnings("unchecked")
    static List<Step> steps(G.Bytecode bc) {
        Object n = bc.norm;
        if (n != null) {
            return (List<Step>) n;
        }
        List<Step> out = new ArrayList<>();
        List<Object[]> pending = new ArrayList<>();
        Step last = null;
        boolean afterRepeat = false;
        boolean propsOpen = false;
        for (Object[] ins : rewriteConnectives(bc)) {
            String name = (String) ins[0];
            Object[] args = java.util.Arrays.copyOfRange(ins, 1, ins.length);
            switch (name) {
                case "by", "from", "to", "option", "with" -> {
                    if (last == null) {
                        throw G.GremlinError.script("modulator " + name + "() has no step to modulate");
                    }
                    (switch (name) {
                        case "by" -> last.by;
                        case "from" -> last.from;
                        case "to" -> last.to;
                        case "option" -> last.option;
                        default -> last.with;
                    }).add(args);
                }
                case "until", "emit", "times" -> {
                    Object[] m = new Object[args.length + 1];
                    m[0] = name;
                    System.arraycopy(args, 0, m, 1, args.length);
                    if (afterRepeat && last != null) {
                        last.post.add(m);
                    } else {
                        pending.add(m);
                    }
                }
                case "property" -> {
                    if (propsOpen && last != null) {
                        last.props.add(args);
                    } else {
                        last = new Step(name, args);
                        out.add(last);
                        afterRepeat = false;
                        propsOpen = false;
                    }
                }
                default -> {
                    Step s = new Step(name, args);
                    if (name.equals("repeat")) {
                        s.pre.addAll(pending);
                        pending.clear();
                        afterRepeat = true;
                    } else {
                        afterRepeat = false;
                        if (!pending.isEmpty()) {
                            throw G.GremlinError.script("until()/emit()/times() must be used with repeat()");
                        }
                    }
                    propsOpen = name.equals("addV") || name.equals("addE");
                    out.add(s);
                    last = s;
                }
            }
        }
        if (!pending.isEmpty()) {
            throw G.GremlinError.script("until()/emit()/times() must be used with repeat()");
        }
        out = foldGraphStepIds(out);
        bc.norm = out;
        return out;
    }

    /** Infix connectives of an anonymous traversal: {@code a().and().b()} means {@code and(a(), b())} (TinkerPop's ConnectiveStrategy). */
    private static List<Object[]> rewriteConnectives(G.Bytecode bc) {
        List<Object[]> raw = bc.steps;
        if (bc.bound != null || !bc.source.isEmpty()) {
            return raw;
        }
        for (int j = 1; j < raw.size() - 1; j++) {
            Object[] s = raw.get(j);
            if (s.length == 1 && (s[0].equals("and") || s[0].equals("or"))) {
                G.Bytecode l = new G.Bytecode();
                l.steps.addAll(raw.subList(0, j));
                G.Bytecode r = new G.Bytecode();
                r.steps.addAll(raw.subList(j + 1, raw.size()));
                List<Object[]> out = new ArrayList<>();
                out.add(new Object[] {s[0], l, r});
                return out;
            }
        }
        return raw;
    }

    /**
     * V()/E() directly followed by hasId(ids) / has(T.id, id) become V(ids)/E(ids) (TinkerGraph's GraphStep folding), which matters
     * because graph lookups by id use the id manager: an Integer id finds a vertex (converted to Long) but no edge.
     */
    private static List<Step> foldGraphStepIds(List<Step> in) {
        List<Step> out = new ArrayList<>();
        for (int i = 0; i < in.size(); i++) {
            Step s = in.get(i);
            if ((s.name.equals("V") || s.name.equals("E")) && s.args.length == 0 && i + 1 < in.size()) {
                int k = i + 1;
                List<Step> labels = new ArrayList<>();
                while (k < in.size() && in.get(k).name.equals("as")) {
                    labels.add(in.get(k));
                    k++;
                }
                if (k >= in.size()) {
                    out.add(s);
                    continue;
                }
                Step n = in.get(k);
                Object[] ids = null;
                if (n.name.equals("hasId") && n.args.length > 0 && java.util.Arrays.stream(n.args).noneMatch(a -> a instanceof G.P)) {
                    ids = n.args;
                } else if (n.name.equals("has") && n.args.length == 2 && n.args[0] == G.T.id && !(n.args[1] instanceof G.P)
                        && !(n.args[1] instanceof G.Bytecode)) {
                    ids = new Object[] {n.args[1]};
                }
                if (ids != null) {
                    out.add(new Step(s.name, ids));
                    out.addAll(labels);
                    i = k;
                    continue;
                }
            }
            out.add(s);
        }
        return out;
    }

    // ------------------------------------------------------------------ iterator helpers

    static <A, B> Iterator<B> flat(Iterator<A> in, Function<A, Iterator<B>> f) {
        return new Iterator<>() {
            Iterator<B> cur = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                while (!cur.hasNext()) {
                    if (!in.hasNext()) {
                        return false;
                    }
                    cur = f.apply(in.next());
                }
                return true;
            }

            @Override
            public B next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return cur.next();
            }
        };
    }

    static <A> Iterator<A> filter(Iterator<A> in, Predicate<A> p) {
        return new Iterator<>() {
            A nxt;
            boolean has;

            @Override
            public boolean hasNext() {
                while (!has) {
                    if (!in.hasNext()) {
                        return false;
                    }
                    A a = in.next();
                    if (p.test(a)) {
                        nxt = a;
                        has = true;
                    }
                }
                return true;
            }

            @Override
            public A next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                has = false;
                return nxt;
            }
        };
    }

    static <A, B> Iterator<B> map(Iterator<A> in, Function<A, B> f) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return in.hasNext();
            }

            @Override
            public B next() {
                return f.apply(in.next());
            }
        };
    }

    /** Maps in chunks (one store round trip per chunk); {@code f} returns the outputs of a chunk in order. */
    static <A, B> Iterator<B> batched(Iterator<A> in, int size, Function<List<A>, List<B>> f) {
        return new Iterator<>() {
            Iterator<B> cur = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                while (!cur.hasNext()) {
                    if (!in.hasNext()) {
                        return false;
                    }
                    List<A> chunk = new ArrayList<>(size);
                    while (chunk.size() < size && in.hasNext()) {
                        chunk.add(in.next());
                    }
                    cur = f.apply(chunk).iterator();
                }
                return true;
            }

            @Override
            public B next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return cur.next();
            }
        };
    }

    static <A> Iterator<A> once(A a) {
        return Collections.singletonList(a).iterator();
    }

    static <A> Iterator<A> lazy(java.util.function.Supplier<Iterator<A>> s) {
        return new Iterator<>() {
            Iterator<A> it;

            private Iterator<A> it() {
                if (it == null) {
                    it = s.get();
                }
                return it;
            }

            @Override
            public boolean hasNext() {
                return it().hasNext();
            }

            @Override
            public A next() {
                return it().next();
            }
        };
    }

    static List<Tr> drain(Iterator<Tr> it) {
        List<Tr> l = new ArrayList<>();
        while (it.hasNext()) {
            l.add(it.next());
        }
        return l;
    }

    // ------------------------------------------------------------------ entry points

    /** Executes a top-level traversal; the results are the traversers' current objects. */
    static Iterator<Object> execute(G.Bytecode bc, Ctx ctx) {
        List<Step> steps = steps(bc);
        for (Object[] s : bc.source) {
            String n = (String) s[0];
            switch (n) {
                case "withSideEffect" -> ctx.side.put((String) s[1], s.length > 2 ? s[2] : null);
                case "withSack" -> {
                    ctx.hasSack = true;
                    ctx.sackInit = s.length > 1 ? s[1] : null;
                    if (ctx.sackInit instanceof G.Closure c) {
                        ctx.sackInit = ctx.call(c);
                    }
                }
                default -> {
                    // withStrategies, withBulk, withPath, with(...), withComputer: no effect on this engine
                }
            }
        }
        ctx.hasRepeat = containsRepeat(bc);
        Tr start = new Tr(null, null, ctx.hasSack ? ctx.sackInit : null, null);
        Iterator<Tr> it = run(steps, once(start), ctx);
        return map(it, t -> t.v);
    }

    private static boolean containsRepeat(G.Bytecode bc) {
        for (Object[] s : bc.steps) {
            // repeat() gives loops; path-tracking steps make the traversers path-based, which also support loops()
            if (s[0] instanceof String n && (n.equals("repeat") || n.equals("as") || n.equals("path") || n.equals("simplePath") || n.equals("cyclicPath")
                    || n.equals("tree") || n.equals("select"))) {
                return true;
            }
            for (int i = 1; i < s.length; i++) {
                if (s[i] instanceof G.Bytecode b && containsRepeat(b)) {
                    return true;
                }
            }
        }
        return false;
    }

    static Iterator<Tr> run(List<Step> steps, Iterator<Tr> in, Ctx ctx) {
        Iterator<Tr> it = in;
        for (int i = 0; i < steps.size(); i++) {
            Step s = steps.get(i);
            // push label/id/property filters that directly follow V() down into the store scan
            if (s.name.equals("V") && s.args.length == 0 && i + 1 < steps.size()) {
                Sigs.check(s);
                it = graphV(s, it, ctx, pushdown(steps, i + 1));
            } else {
                it = step(s, it, ctx);
            }
        }
        return it;
    }

    private static GraphStore.VFilter pushdown(List<Step> steps, int from) {
        List<String> labels = null;
        Map<String, String> props = new LinkedHashMap<>();
        for (int i = from; i < steps.size(); i++) {
            Step s = steps.get(i);
            if (s.name.equals("hasLabel") && s.args.length > 0 && labels == null) {
                boolean ok = true;
                List<String> l = new ArrayList<>();
                for (Object a : s.args) {
                    if (a instanceof String x) {
                        l.add(x);
                    } else {
                        ok = false;
                    }
                }
                if (ok) {
                    labels = l;
                } else {
                    break;
                }
            } else if (s.name.equals("has") && s.args.length == 2 && s.args[0] instanceof String k && s.args[1] instanceof String v) {
                props.put(k, v);
            } else if (s.name.equals("has") || s.name.equals("hasNot") || s.name.equals("hasId") || s.name.equals("hasKey")
                    || s.name.equals("hasValue") || s.name.equals("identity")) {
                continue;
            } else {
                break;
            }
        }
        return labels == null && props.isEmpty() ? GraphStore.VFilter.NONE : new GraphStore.VFilter(labels, props);
    }

    // ------------------------------------------------------------------ graph access

    private static Iterator<Tr> graphV(Step s, Iterator<Tr> in, Ctx ctx, GraphStore.VFilter f) {
        return flat(in, t -> lazy(() -> map(ctx.store.allVertices(f), v -> t.child(v))));
    }

    static List<Object> flatIds(Object[] args) {
        List<Object> ids = new ArrayList<>();
        for (Object a : args) {
            if (a instanceof Collection<?> c) {
                ids.addAll(c);
            } else if (a != null) {
                ids.add(a instanceof G.Element e ? e.id() : a);
            }
        }
        return ids;
    }

    // ------------------------------------------------------------------ property helpers

    static Object token(Object o, G.T t) {
        switch (t) {
            case id:
                if (o instanceof G.Element e) {
                    return e.id();
                }
                if (o instanceof G.VProp p) {
                    return p.id;
                }
                return NONE;
            case label:
                if (o instanceof G.Element e) {
                    return e.label();
                }
                if (o instanceof G.VProp p) {
                    return p.key;
                }
                if (o instanceof G.Prop p) {
                    return p.key;
                }
                return NONE;
            case key:
                if (o instanceof G.VProp p) {
                    return p.key;
                }
                if (o instanceof G.Prop p) {
                    return p.key;
                }
                return NONE;
            default:
                if (o instanceof G.VProp p) {
                    return p.value;
                }
                if (o instanceof G.Prop p) {
                    return p.value;
                }
                return NONE;
        }
    }

    /** The value(s) of a property key of an element / map, or NONE. */
    static Object propValue(Object o, String key) {
        if (o instanceof G.Vertex v) {
            List<G.VProp> l = v.vprops(key);
            return l.isEmpty() ? NONE : l.get(0).value;
        }
        if (o instanceof G.Edge e) {
            return e.props.containsKey(key) ? e.props.get(key) : NONE;
        }
        if (o instanceof Map<?, ?> m) {
            return m.containsKey(key) ? m.get(key) : NONE;
        }
        if (o instanceof G.VProp p) {
            return p.meta != null && p.meta.containsKey(key) ? p.meta.get(key) : NONE;
        }
        return NONE;
    }

    static List<Object> propValues(Object o, String[] keys) {
        List<Object> out = new ArrayList<>();
        if (o instanceof G.Vertex v) {
            if (keys.length == 0) {
                for (List<G.VProp> l : v.props.values()) {
                    for (G.VProp p : l) {
                        out.add(p.value);
                    }
                }
            } else {
                for (String k : keys) {
                    for (G.VProp p : v.vprops(k)) {
                        out.add(p.value);
                    }
                }
            }
        } else if (o instanceof G.Edge e) {
            if (keys.length == 0) {
                out.addAll(e.props.values());
            } else {
                for (String k : keys) {
                    if (e.props.containsKey(k)) {
                        out.add(e.props.get(k));
                    }
                }
            }
        } else if (o instanceof G.VProp p && p.meta != null) {
            if (keys.length == 0) {
                out.addAll(p.meta.values());
            } else {
                for (String k : keys) {
                    if (p.meta.containsKey(k)) {
                        out.add(p.meta.get(k));
                    }
                }
            }
        } else if (o instanceof Map<?, ?>) {
            throw G.GremlinError.script("values() cannot be applied to a Map");
        } else if (!(o instanceof G.VProp) && !(o instanceof G.Prop)) {
            throw G.GremlinError.script("The provided traverser does not map to a value: " + o);
        }
        return out;
    }

    static List<Object> props(Object o, String[] keys) {
        List<Object> out = new ArrayList<>();
        if (o instanceof G.Vertex v) {
            if (keys.length == 0) {
                for (List<G.VProp> l : v.props.values()) {
                    out.addAll(l);
                }
            } else {
                for (String k : keys) {
                    out.addAll(v.vprops(k));
                }
            }
        } else if (o instanceof G.Edge e) {
            List<String> ks = keys.length == 0 ? new ArrayList<>(e.props.keySet()) : List.of(keys);
            for (String k : ks) {
                if (e.props.containsKey(k)) {
                    out.add(new G.Prop(k, e.props.get(k), e));
                }
            }
        } else if (o instanceof G.VProp p && p.meta != null) {
            List<String> ks = keys.length == 0 ? new ArrayList<>(p.meta.keySet()) : List.of(keys);
            for (String k : ks) {
                if (p.meta.containsKey(k)) {
                    out.add(new G.Prop(k, p.meta.get(k), p));
                }
            }
        } else if (!(o instanceof G.VProp) && !(o instanceof G.Prop)) {
            throw G.GremlinError.script("The provided traverser does not map to a value: " + o);
        }
        return out;
    }

    // ------------------------------------------------------------------ predicates

    static boolean test(G.P p, Object x) {
        switch (p.op) {
            case "and":
                return test(p.left, x) && test(p.right, x);
            case "or":
                return test(p.left, x) || test(p.right, x);
            default:
                break;
        }
        if (p.text) {
            if (!(x instanceof String s)) {
                return false;
            }
            Object a = p.value();
            if (!(a instanceof String t)) {
                return false;
            }
            return switch (p.op) {
                case "containing" -> s.contains(t);
                case "notContaining" -> !s.contains(t);
                case "startingWith" -> s.startsWith(t);
                case "notStartingWith" -> !s.startsWith(t);
                case "endingWith" -> s.endsWith(t);
                case "notEndingWith" -> !s.endsWith(t);
                case "regex" -> regex(p, t).matcher(s).find();
                case "notRegex" -> !regex(p, t).matcher(s).find();
                default -> throw G.GremlinError.script("unknown text predicate " + p.op);
            };
        }
        switch (p.op) {
            case "eq":
                return Cmp.eq(x, p.value());
            case "neq":
                return !Cmp.eq(x, p.value());
            case "lt", "lte", "gt", "gte": {
                Integer c = Cmp.compare(x, p.value());
                if (c == null) {
                    return false;
                }
                return switch (p.op) {
                    case "lt" -> c < 0;
                    case "lte" -> c <= 0;
                    case "gt" -> c > 0;
                    default -> c >= 0;
                };
            }
            case "within":
                for (Object a : p.args) {
                    if (Cmp.eq(x, a)) {
                        return true;
                    }
                }
                return false;
            case "without":
                for (Object a : p.args) {
                    if (Cmp.eq(x, a)) {
                        return false;
                    }
                }
                return true;
            case "between": {
                Integer lo = Cmp.compare(x, p.args.get(0));
                Integer hi = Cmp.compare(x, p.args.get(1));
                return lo != null && hi != null && lo >= 0 && hi < 0;
            }
            case "not_between": {
                Integer lo = Cmp.compare(x, p.args.get(0));
                Integer hi = Cmp.compare(x, p.args.get(1));
                return lo == null || hi == null || lo < 0 || hi >= 0;
            }
            case "inside": {
                Integer lo = Cmp.compare(x, p.args.get(0));
                Integer hi = Cmp.compare(x, p.args.get(1));
                return lo != null && hi != null && lo > 0 && hi < 0;
            }
            case "outside": {
                Integer lo = Cmp.compare(x, p.args.get(0));
                Integer hi = Cmp.compare(x, p.args.get(1));
                return lo != null && hi != null && (lo < 0 || hi > 0);
            }
            default:
                throw G.GremlinError.script("unknown predicate " + p.op);
        }
    }

    private static java.util.regex.Pattern regex(G.P p, String t) {
        java.util.regex.Pattern pat = p.pattern;
        if (pat == null) {
            try {
                pat = java.util.regex.Pattern.compile(t);
            } catch (java.util.regex.PatternSyntaxException e) {
                throw G.GremlinError.script(e.getMessage());
            }
            p.pattern = pat;
        }
        return pat;
    }

    /** Copy of {@code p} with each String argument replaced through {@code f} (where(P) label references). */
    static G.P mapArgs(G.P p, Function<Object, Object> f) {
        if (p.left != null) {
            return new G.P(p.op, mapArgs(p.left, f), mapArgs(p.right, f));
        }
        List<Object> a = new ArrayList<>();
        for (Object x : p.args) {
            Object r = f.apply(x);
            boolean set = p.op.equals("within") || p.op.equals("without");
            if (set && r instanceof G.BulkSet bs) {
                a.addAll(bs.keySet());
            } else if (set && r instanceof Collection<?> c && x instanceof String) {
                a.addAll(c);
            } else {
                a.add(r);
            }
        }
        return new G.P(p.op, p.text, a);
    }

    // ------------------------------------------------------------------ modulators

    /** Evaluates one by()-style modulator argument list against a traverser. */
    static Object byValue(Object[] a, Tr t, Ctx ctx) {
        if (a.length == 0) {
            return t.v;
        }
        Object k = a[0];
        if (k instanceof G.Order || k instanceof G.Scope) {
            return t.v;
        }
        if (k instanceof String key) {
            return propValue(t.v, key);
        }
        if (k instanceof G.T tok) {
            return token(t.v, tok);
        }
        if (k instanceof G.Bytecode bc) {
            Iterator<Tr> it = sub(bc, t, ctx);
            if (it.hasNext()) {
                return it.next().v;
            }
            return NONE;
        }
        if (k instanceof G.Closure c) {
            return ctx.call(c, t.v);
        }
        if (k instanceof G.Column col) {
            if (t.v instanceof Map<?, ?> m) {
                return col == G.Column.keys ? new ArrayList<>(m.keySet()) : new ArrayList<>(m.values());
            }
            if (t.v instanceof Map.Entry<?, ?> e) {
                return col == G.Column.keys ? e.getKey() : e.getValue();
            }
            return NONE;
        }
        throw G.GremlinError.script("unsupported by() argument: " + k);
    }

    /** by() value; NONE when the property is missing or the traversal yields nothing (callers then filter the traverser or omit the key). */
    static Object byStrict(Object[] a, Tr t, Ctx ctx) {
        return byValue(a, t, ctx);
    }

    static Comparator<Object> comparator(Object[] mod) {
        G.Order o = G.Order.asc;
        for (Object a : mod) {
            if (a instanceof G.Order x) {
                o = x;
            }
        }
        switch (o) {
            case desc, decr:
                return (x, y) -> Cmp.order(y, x);
            default:
                return Cmp::order;
        }
    }

    static G.TraverserView viewOf(Tr t) {
        return new G.TraverserView(t.v, pathOf(t.path), t.sack, t.loops == null ? 0 : t.loops.n, 1);
    }

    // ------------------------------------------------------------------ sub traversals

    static Iterator<Tr> sub(G.Bytecode bc, Tr t, Ctx ctx) {
        return run(steps(bc), once(t), ctx);
    }

    static boolean exists(G.Bytecode bc, Tr t, Ctx ctx) {
        return sub(bc, t, ctx).hasNext();
    }

    // ------------------------------------------------------------------ step dispatch

    static Iterator<Tr> step(Step s, Iterator<Tr> in, Ctx ctx) {
        Sigs.check(s);
        Iterator<Tr> it = Steps.apply(s, in, ctx);
        if (ctx.deadlineNanos != 0) {
            return map(it, t -> {
                ctx.tick();
                return t;
            });
        }
        return it;
    }
}
