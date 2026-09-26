package com.sayonora.wire.gremlinwire;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/** The value model of the Gremlin interpreter: graph elements, paths, predicates, traversal bytecode and the token enums. */
final class G {

    private G() {
    }

    /** A Gremlin Server status code carrying error. */
    static final class GremlinError extends RuntimeException {
        final int code;
        /** An error found while translating the request into a traversal (unknown step, bad arguments): 599 for a bytecode request. */
        boolean translation;

        GremlinError(int code, String message) {
            super(message);
            this.code = code;
        }

        static GremlinError translation(String m) {
            GremlinError e = new GremlinError(597, m);
            e.translation = true;
            return e;
        }

        static GremlinError script(String m) {
            return new GremlinError(597, m);
        }

        static GremlinError args(String m) {
            return new GremlinError(499, m);
        }
    }

    enum T { id, label, key, value }

    enum Direction { OUT, IN, BOTH }

    enum Order { asc, desc, shuffle, incr, decr }

    enum Scope { local, global }

    enum Cardinality { single, list, set }

    enum Column { keys, values }

    enum Pick { any, none }

    enum Pop { first, last, all, mixed }

    enum Operator { sum, minus, mult, div, min, max, assign, and, or, addAll, sumLong }

    enum Merge { onCreate, onMatch, outV, inV }

    enum Barrier { normSack }

    enum WithOptions { tokens, none, ids, labels, keys, values, all, indexer, list, map }

    interface Element {
        Object id();

        String label();
    }

    /** A vertex property value (multi-properties: several per key). */
    static final class VProp {
        Object id;
        final String key;
        Object value;
        LinkedHashMap<String, Object> meta;
        Vertex vertex;

        VProp(Object id, String key, Object value) {
            this.id = id;
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof VProp p && Objects.equals(p.id, id) && p.key.equals(key) && Objects.equals(p.vertex, vertex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, key);
        }
    }

    /** An edge property or a meta-property. */
    static final class Prop {
        final String key;
        final Object value;
        final Object element;

        Prop(String key, Object value, Object element) {
            this.key = key;
            this.value = value;
            this.element = element;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Prop p && p.key.equals(key) && Objects.equals(p.value, value) && Objects.equals(p.element, element);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    static final class Vertex implements Element {
        final Object id;
        final String label;
        Map<String, List<VProp>> props = new java.util.HashMap<>();

        Vertex(Object id, String label) {
            this.id = id;
            this.label = label;
        }

        @Override
        public Object id() {
            return id;
        }

        @Override
        public String label() {
            return label;
        }

        List<VProp> vprops(String key) {
            List<VProp> l = props.get(key);
            return l == null ? List.of() : l;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Vertex v && v.id.equals(id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "v[" + id + "]";
        }
    }

    static final class Edge implements Element {
        final Object id;
        final String label;
        final Object outId;
        final Object inId;
        String outLabel;
        String inLabel;
        Map<String, Object> props = new java.util.HashMap<>();

        Edge(Object id, String label, Object outId, String outLabel, Object inId, String inLabel) {
            this.id = id;
            this.label = label;
            this.outId = outId;
            this.outLabel = outLabel;
            this.inId = inId;
            this.inLabel = inLabel;
        }

        @Override
        public Object id() {
            return id;
        }

        @Override
        public String label() {
            return label;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Edge e && e.id.equals(id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "e[" + id + "][" + outId + "-" + label + "->" + inId + "]";
        }
    }

    static final class Path {
        final List<Object> objects;
        final List<Set<String>> labels;

        Path(List<Object> objects, List<Set<String>> labels) {
            this.objects = objects;
            this.labels = labels;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Path p && p.objects.equals(objects) && p.labels.equals(labels);
        }

        @Override
        public int hashCode() {
            return objects.hashCode();
        }

        @Override
        public String toString() {
            return "path" + objects;
        }
    }

    /** Result of tree(): vertex/value keyed nested maps. */
    static final class Tree extends java.util.HashMap<Object, Tree> {
    }

    /** Multiset used by aggregate()/store() side effects (serialised as g:BulkSet). */
    static final class BulkSet extends LinkedHashMap<Object, Long> {
        void add(Object o) {
            merge(o, 1L, Long::sum);
        }
    }

    /** A predicate: P (comparison / collection / logic) or TextP (string). */
    static final class P {
        final String op;
        final boolean text;
        final List<Object> args;
        final P left;
        final P right;
        /** Compiled regex for regex/notRegex. */
        Pattern pattern;

        P(String op, boolean text, List<Object> args) {
            this.op = op;
            this.text = text;
            this.args = args;
            this.left = null;
            this.right = null;
        }

        P(String op, P l, P r) {
            this.op = op;
            this.text = false;
            this.args = List.of();
            this.left = l;
            this.right = r;
        }

        Object value() {
            return args.isEmpty() ? null : args.get(0);
        }

        P negate() {
            return switch (op) {
                case "eq" -> new P("neq", text, args);
                case "neq" -> new P("eq", text, args);
                case "lt" -> new P("gte", text, args);
                case "lte" -> new P("gt", text, args);
                case "gt" -> new P("lte", text, args);
                case "gte" -> new P("lt", text, args);
                case "within" -> new P("without", text, args);
                case "without" -> new P("within", text, args);
                case "inside" -> new P("outside", text, args);
                case "outside" -> new P("inside", text, args);
                case "between" -> new P("not_between", text, args);
                case "containing" -> new P("notContaining", true, args);
                case "notContaining" -> new P("containing", true, args);
                case "startingWith" -> new P("notStartingWith", true, args);
                case "notStartingWith" -> new P("startingWith", true, args);
                case "endingWith" -> new P("notEndingWith", true, args);
                case "notEndingWith" -> new P("endingWith", true, args);
                case "regex" -> new P("notRegex", true, args);
                case "notRegex" -> new P("regex", true, args);
                case "and" -> new P("or", left.negate(), right.negate());
                case "or" -> new P("and", left.negate(), right.negate());
                case "not_between" -> new P("between", text, args);
                default -> throw GremlinError.script("cannot negate predicate " + op);
            };
        }

        @Override
        public String toString() {
            return left != null ? "(" + left + " " + op + " " + right + ")" : op + args;
        }
    }

    /** A bytecode step instruction: name + arguments (arguments may be nested {@link Bytecode}). */
    static final class Bytecode {
        final List<Object[]> source = new ArrayList<>();
        final List<Object[]> steps = new ArrayList<>();
        /** Set on traversals started from a script's g (executes terminal methods against this source). */
        Object bound;
        /** True after iterate()/toList()/... consumed a script traversal. */
        boolean consumed;
        /** The Engine's normalized step list (cache). */
        Object norm;

        Bytecode add(String name, Object... args) {
            Object[] s = new Object[args.length + 1];
            s[0] = name;
            System.arraycopy(args, 0, s, 1, args.length);
            steps.add(s);
            return this;
        }

        Bytecode copy() {
            Bytecode b = new Bytecode();
            b.source.addAll(source);
            b.steps.addAll(steps);
            b.bound = bound;
            return b;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Object[] s : steps) {
                sb.append(s[0]).append(s.length > 1 ? java.util.Arrays.toString(java.util.Arrays.copyOfRange(s, 1, s.length)) : "()");
            }
            return sb.toString();
        }
    }

    /** A Groovy-style closure kept as source and evaluated by the script interpreter. */
    record Closure(List<String> params, Object body, Object scope) {
    }

    /** A traverser as a script closure sees it ({@code it.get()}). */
    static final class TraverserView {
        final Object value;
        final Object path;
        final Object sack;
        final int loops;
        final long bulk;

        TraverserView(Object value, Object path, Object sack, int loops, long bulk) {
            this.value = value;
            this.path = path;
            this.sack = sack;
            this.loops = loops;
            this.bulk = bulk;
        }
    }

    /** The value of the script variable {@code graph} when returned (serialised as tinker:graph). */
    record GraphValue(List<Vertex> vertices, List<Edge> edges) {
    }

    /** A wire-level traverser (bulked result). */
    static final class WireTraverser {
        final Object value;
        final long bulk;

        WireTraverser(Object value, long bulk) {
            this.value = value;
            this.bulk = bulk;
        }
    }

    /** The result of a metaproperty-free Map.Entry (script results that are maps are iterated by entry). */
    static Map<Object, Object> single(Object k, Object v) {
        Map<Object, Object> m = new LinkedHashMap<>();
        m.put(k, v);
        return m;
    }

    static boolean isCollection(Object o) {
        return o instanceof Collection<?>;
    }

    static Set<Object> setOf(Collection<?> c) {
        return new LinkedHashSet<>(c);
    }

    static UUID uuid(String s) {
        return UUID.fromString(s);
    }
}
