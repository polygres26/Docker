package com.sayonora.wire.gremlinwire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Static names, predicate factories and the small library of Groovy/Java methods scripts may call. */
final class Builtins {

    private Builtins() {
    }

    static final Set<String> STATICS = Set.of("T", "P", "TextP", "Order", "Scope", "Column", "Cardinality", "Pop", "Operator", "Direction", "Pick", "Merge",
            "WithOptions", "Barrier", "Math", "UUID", "System", "Integer", "Long", "Double", "Float", "String", "Collections", "Boolean", "BigDecimal", "Arrays",
            "Instant", "Date", "Character", "List", "Objects");

    static final Map<String, Object> ENUM_CONSTS = new HashMap<>();

    static {
        for (G.T t : G.T.values()) {
            ENUM_CONSTS.put(t.name(), t);
        }
        for (G.Column c : G.Column.values()) {
            ENUM_CONSTS.put(c.name(), c);
        }
        for (G.Order o : G.Order.values()) {
            ENUM_CONSTS.put(o.name(), o);
        }
        for (G.Scope s : G.Scope.values()) {
            ENUM_CONSTS.put(s.name(), s);
        }
        for (G.Cardinality c : G.Cardinality.values()) {
            ENUM_CONSTS.put(c.name(), c);
        }
        for (G.Pop p : G.Pop.values()) {
            ENUM_CONSTS.put(p.name(), p);
        }
        for (G.Operator o : G.Operator.values()) {
            ENUM_CONSTS.put(o.name(), o);
        }
        ENUM_CONSTS.put("any", G.Pick.any);
        ENUM_CONSTS.put("none", G.Pick.none);
        ENUM_CONSTS.put("IN", G.Direction.IN);
        ENUM_CONSTS.put("OUT", G.Direction.OUT);
        ENUM_CONSTS.put("BOTH", G.Direction.BOTH);
        for (G.Merge m : G.Merge.values()) {
            ENUM_CONSTS.put(m.name(), m);
        }
        ENUM_CONSTS.put("normSack", G.Barrier.normSack);
    }

    /** Names of the traversal steps a bare call or {@code __.} call may build. */
    static final Set<String> STEPS = Set.of("V", "E", "addE", "addV", "aggregate", "and", "as", "barrier", "both", "bothE", "bothV", "branch", "by", "cap", "choose",
            "coalesce", "coin", "constant", "count", "cyclicPath", "dedup", "drop", "elementMap", "emit", "filter", "flatMap", "fold", "from", "group", "groupCount",
            "has", "hasId", "hasKey", "hasLabel", "hasNot", "hasValue", "id", "identity", "in", "inE", "inV", "index", "inject", "is", "key", "label", "limit",
            "local", "loops", "map", "math", "max", "mean", "mergeE", "mergeV", "min", "none", "not", "option", "optional", "or", "order", "otherV", "out", "outE",
            "outV", "path", "project", "properties", "property", "propertyMap", "range", "repeat", "sack", "sample", "select", "sideEffect", "simplePath", "skip",
            "store", "subgraph", "sum", "tail", "timeLimit", "times", "to", "toE", "toV", "tree", "unfold", "union", "until", "value", "valueMap", "values", "where",
            "with", "asString", "toLower", "toUpper", "trim", "lTrim", "rTrim", "length", "split", "replace", "substring", "concat", "reverse", "discard", "match",
            "profile", "explain");

    private static G.GremlinError err(String m) {
        return G.GremlinError.script(m);
    }

    // ------------------------------------------------------------------ bare calls

    static Object bareCall(String name, Object[] a) {
        switch (name) {
            case "eq", "neq", "lt", "lte", "gt", "gte":
                need(name, a, 1);
                return new G.P(name, false, new ArrayList<>(Arrays.asList(a[0])));
            case "within", "without": {
                List<Object> l = new ArrayList<>();
                for (Object x : a) {
                    if (x instanceof Collection<?> c) {
                        l.addAll(c);
                    } else {
                        l.add(x);
                    }
                }
                return new G.P(name, false, l);
            }
            case "between", "inside", "outside":
                need(name, a, 2);
                return new G.P(name, false, new ArrayList<>(Arrays.asList(a[0], a[1])));
            case "containing", "notContaining", "startingWith", "notStartingWith", "endingWith", "notEndingWith", "regex", "notRegex":
                need(name, a, 1);
                return new G.P(name, true, new ArrayList<>(Arrays.asList(a[0])));
            case "not":
                if (a.length == 1 && a[0] instanceof G.P p) {
                    return p.negate();
                }
                // bare not(traversal) resolves to the static P.not(P) in a Groovy script and fails there (use __.not(...))
                throw err("No signature of method: static org.apache.tinkerpop.gremlin.process.traversal.P.not() is applicable for argument types: ("
                        + typeNames(a) + ")");
            case "println", "print":
                return null;
            default:
                break;
        }
        if (STEPS.contains(name)) {
            G.Bytecode b = new G.Bytecode();
            b.add(name, a);
            return b;
        }
        throw err("No signature of method: Script1." + name + "() is applicable for argument types: (" + typeNames(a) + ")");
    }

    private static String typeNames(Object[] a) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < a.length; i++) {
            sb.append(i > 0 ? ", " : "").append(a[i] == null ? "null" : a[i].getClass().getName());
        }
        return sb.toString();
    }

    private static void need(String n, Object[] a, int c) {
        if (a.length != c) {
            throw err("No signature of method: P." + n + "() is applicable for argument types: (" + typeNames(a) + ")");
        }
    }

    // ------------------------------------------------------------------ static classes

    static Object staticMethod(String cls, String name, Object[] a, Script.Env env) {
        switch (cls) {
            case "__": {
                if (STEPS.contains(name)) {
                    G.Bytecode b = new G.Bytecode();
                    b.add(name, a);
                    return b;
                }
                throw err("No signature of method: static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__." + name + "() is applicable");
            }
            case "P", "TextP": {
                if (name.equals("not") && a.length == 1 && a[0] instanceof G.P p) {
                    return p.negate();
                }
                return bareCall(name, a);
            }
            case "Math":
                return math(name, a);
            case "UUID":
                if (name.equals("randomUUID")) {
                    return UUID.randomUUID();
                }
                if (name.equals("fromString")) {
                    return UUID.fromString((String) a[0]);
                }
                break;
            case "System":
                if (name.equals("currentTimeMillis")) {
                    return System.currentTimeMillis();
                }
                if (name.equals("nanoTime")) {
                    return System.nanoTime();
                }
                break;
            case "Integer":
                if (name.equals("parseInt") || name.equals("valueOf")) {
                    return Integer.parseInt(String.valueOf(a[0]));
                }
                break;
            case "Long":
                if (name.equals("parseLong") || name.equals("valueOf")) {
                    return Long.parseLong(String.valueOf(a[0]));
                }
                break;
            case "Double":
                if (name.equals("parseDouble") || name.equals("valueOf")) {
                    return Double.parseDouble(String.valueOf(a[0]));
                }
                break;
            case "Float":
                if (name.equals("parseFloat") || name.equals("valueOf")) {
                    return Float.parseFloat(String.valueOf(a[0]));
                }
                break;
            case "Boolean":
                if (name.equals("parseBoolean") || name.equals("valueOf")) {
                    return Boolean.parseBoolean(String.valueOf(a[0]));
                }
                break;
            case "String":
                if (name.equals("valueOf")) {
                    return String.valueOf(a[0]);
                }
                if (name.equals("format")) {
                    return String.format((String) a[0], Arrays.copyOfRange(a, 1, a.length));
                }
                break;
            case "Collections":
                if (name.equals("emptyList")) {
                    return new ArrayList<>();
                }
                if (name.equals("emptyMap")) {
                    return new LinkedHashMap<>();
                }
                if (name.equals("emptySet")) {
                    return new LinkedHashSet<>();
                }
                break;
            case "Arrays":
                if (name.equals("asList")) {
                    return new ArrayList<>(Arrays.asList(a.length == 1 && a[0] instanceof Object[] arr ? arr : a));
                }
                break;
            case "List":
                if (name.equals("of")) {
                    return new ArrayList<>(Arrays.asList(a));
                }
                break;
            case "Instant":
                if (name.equals("now")) {
                    return java.time.Instant.now();
                }
                break;
            case "Date":
                break;
            default:
                if (name.equals("valueOf") && a.length == 1) {
                    Object v = enumValue(cls, String.valueOf(a[0]));
                    if (v != null) {
                        return v;
                    }
                }
                break;
        }
        throw err("No signature of method: static " + cls + "." + name + "() is applicable for argument types: (" + typeNames(a) + ")");
    }

    private static Object math(String name, Object[] a) {
        double x = a.length > 0 ? ((Number) a[0]).doubleValue() : 0;
        switch (name) {
            case "abs":
                return a[0] instanceof Integer i ? (Object) Math.abs(i) : a[0] instanceof Long l ? (Object) Math.abs(l) : (Object) Math.abs(x);
            case "max":
                return Cmp.order(a[0], a[1]) >= 0 ? a[0] : a[1];
            case "min":
                return Cmp.order(a[0], a[1]) <= 0 ? a[0] : a[1];
            case "pow":
                return Math.pow(x, ((Number) a[1]).doubleValue());
            case "sqrt":
                return Math.sqrt(x);
            case "floor":
                return Math.floor(x);
            case "ceil":
                return Math.ceil(x);
            case "round":
                return Math.round(x);
            case "random":
                return Math.random();
            case "sin":
                return Math.sin(x);
            case "cos":
                return Math.cos(x);
            case "log":
                return Math.log(x);
            case "exp":
                return Math.exp(x);
            default:
                throw err("No signature of method: static java.lang.Math." + name + "() is applicable");
        }
    }

    static Object enumValue(String cls, String name) {
        try {
            return switch (cls) {
                case "T" -> G.T.valueOf(name);
                case "Order" -> G.Order.valueOf(name);
                case "Scope" -> G.Scope.valueOf(name);
                case "Column" -> G.Column.valueOf(name);
                case "Cardinality" -> G.Cardinality.valueOf(name);
                case "Pop" -> G.Pop.valueOf(name);
                case "Operator" -> G.Operator.valueOf(name);
                case "Direction" -> G.Direction.valueOf(name.toUpperCase(java.util.Locale.ROOT));
                case "Pick" -> G.Pick.valueOf(name);
                case "Merge" -> G.Merge.valueOf(name);
                case "WithOptions" -> G.WithOptions.valueOf(name);
                case "Barrier" -> G.Barrier.valueOf(name);
                default -> null;
            };
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------ properties

    static Object property(Object t, String name, Script.Env env) {
        if (t instanceof Script.Static s) {
            Object v = enumValue(s.name(), name);
            if (v != null) {
                return v;
            }
            if (s.name().equals("Direction")) {
                if (name.equals("from")) {
                    return G.Direction.OUT;
                }
                if (name.equals("to")) {
                    return G.Direction.IN;
                }
            }
            throw err("No such property: " + name + " for class: " + s.name());
        }
        if (t instanceof Map<?, ?> m) {
            return m.get(name);
        }
        if (t instanceof G.TraverserView v) {
            return switch (name) {
                case "value", "object" -> v.value;
                case "sack" -> v.sack;
                case "path" -> v.path;
                case "loops" -> v.loops;
                default -> throw err("No such property: " + name + " for class: Traverser");
            };
        }
        if (t instanceof G.Vertex v) {
            return switch (name) {
                case "id" -> v.id;
                case "label" -> v.label;
                default -> {
                    List<G.VProp> l = v.vprops(name);
                    yield l.isEmpty() ? null : l.get(0).value;
                }
            };
        }
        if (t instanceof G.Edge e) {
            return switch (name) {
                case "id" -> e.id;
                case "label" -> e.label;
                default -> e.props.get(name);
            };
        }
        if (t instanceof G.VProp p) {
            return switch (name) {
                case "value" -> p.value;
                case "key", "label" -> p.key;
                case "id" -> p.id;
                default -> null;
            };
        }
        if (t instanceof G.Prop p) {
            return switch (name) {
                case "value" -> p.value;
                case "key" -> p.key;
                default -> null;
            };
        }
        if (t instanceof Map.Entry<?, ?> e) {
            return switch (name) {
                case "key" -> e.getKey();
                case "value" -> e.getValue();
                default -> null;
            };
        }
        if (t instanceof Collection<?> c) {
            if (name.equals("size")) {
                return c.size();
            }
            if (name.equals("empty")) {
                return c.isEmpty();
            }
            List<Object> out = new ArrayList<>();
            for (Object x : c) {
                out.add(property(x, name, env));
            }
            return out;
        }
        if (t instanceof String s && name.equals("length")) {
            return s.length();
        }
        if (t == null) {
            throw err("Cannot get property '" + name + "' on null object");
        }
        throw err("No such property: " + name + " for class: " + t.getClass().getName());
    }

    static Object index(Object t, Object i) {
        if (t instanceof List<?> l && i instanceof Number n) {
            int idx = n.intValue();
            if (idx < 0) {
                idx += l.size();
            }
            return idx >= 0 && idx < l.size() ? l.get(idx) : null;
        }
        if (t instanceof Map<?, ?> m) {
            return m.get(i);
        }
        if (t instanceof String s && i instanceof Number n) {
            int idx = n.intValue();
            return idx >= 0 && idx < s.length() ? String.valueOf(s.charAt(idx)) : null;
        }
        if (t instanceof Object[] arr && i instanceof Number n) {
            return arr[n.intValue()];
        }
        if (t == null) {
            throw err("Cannot invoke method getAt() on null object");
        }
        throw err("No signature of method: " + t.getClass().getName() + ".getAt() is applicable for argument types: (" + (i == null ? "null" : i.getClass().getName()) + ")");
    }

    // ------------------------------------------------------------------ instance methods

    @SuppressWarnings("unchecked")
    static Object method(Object t, String name, Object[] a, Script.Env env) {
        if (t == null) {
            throw err("Cannot invoke method " + name + "() on null object");
        }
        Script.Session ses = env.session;
        if (t instanceof G.P p) {
            switch (name) {
                case "and":
                    return new G.P("and", p, (G.P) a[0]);
                case "or":
                    return new G.P("or", p, (G.P) a[0]);
                case "negate":
                    return p.negate();
                case "test":
                    return Engine.test(p, a[0]);
                default:
                    break;
            }
        }
        if (t instanceof Script.GraphHandle) {
            switch (name) {
                case "traversal": {
                    G.Bytecode g = new G.Bytecode();
                    g.bound = env;
                    return g;
                }
                case "tx":
                    return new Script.TxHandle();
                case "toString":
                    return "tinkergraph[warp]";
                case "features":
                    return new LinkedHashMap<>();
                default:
                    break;
            }
        }
        if (t instanceof Script.TxHandle) {
            switch (name) {
                case "commit", "rollback", "close", "open":
                    throw new G.GremlinError(597, "Graph does not support transactions");
                case "isOpen":
                    return false;
                default:
                    break;
            }
        }
        if (t instanceof String s) {
            switch (name) {
                case "length", "size":
                    return s.length();
                case "toUpperCase":
                    return s.toUpperCase();
                case "toLowerCase":
                    return s.toLowerCase();
                case "trim":
                    return s.trim();
                case "contains":
                    return s.contains(String.valueOf(a[0]));
                case "startsWith":
                    return s.startsWith((String) a[0]);
                case "endsWith":
                    return s.endsWith((String) a[0]);
                case "substring":
                    return a.length == 1 ? s.substring(((Number) a[0]).intValue()) : s.substring(((Number) a[0]).intValue(), ((Number) a[1]).intValue());
                case "split": {
                    List<Object> out = new ArrayList<>();
                    for (String x : s.split(java.util.regex.Pattern.quote((String) a[0]), -1)) {
                        out.add(x);
                    }
                    return out;
                }
                case "replace":
                    return s.replace((String) a[0], (String) a[1]);
                case "toString":
                    return s;
                case "toInteger":
                    return Integer.parseInt(s);
                case "toLong":
                    return Long.parseLong(s);
                case "toDouble":
                    return Double.parseDouble(s);
                case "isEmpty":
                    return s.isEmpty();
                case "reverse":
                    return new StringBuilder(s).reverse().toString();
                case "charAt":
                    return String.valueOf(s.charAt(((Number) a[0]).intValue()));
                case "indexOf":
                    return s.indexOf((String) a[0]);
                case "plus":
                    return s + a[0];
                case "equals":
                    return s.equals(a[0]);
                default:
                    break;
            }
        }
        if (t instanceof Number n) {
            switch (name) {
                case "intValue":
                    return n.intValue();
                case "longValue":
                    return n.longValue();
                case "doubleValue":
                    return n.doubleValue();
                case "floatValue":
                    return n.floatValue();
                case "toString":
                    return n.toString();
                case "abs":
                    return n instanceof Integer ? (Object) Math.abs(n.intValue()) : (Object) Math.abs(n.doubleValue());
                case "plus":
                    return Cmp.add(n, (Number) a[0]);
                case "minus":
                    return Cmp.sub(n, (Number) a[0]);
                case "multiply":
                    return Cmp.mul(n, (Number) a[0]);
                case "div":
                    return Cmp.groovyDiv(n, (Number) a[0]);
                case "equals":
                    return Cmp.eq(n, a[0]);
                default:
                    break;
            }
        }
        if (t instanceof Collection<?> c) {
            Object r = collectionMethod(c, name, a, ses);
            if (r != NO) {
                return r;
            }
        }
        if (t instanceof Map<?, ?> m) {
            Map<Object, Object> mm = (Map<Object, Object>) m;
            switch (name) {
                case "size":
                    return m.size();
                case "isEmpty":
                    return m.isEmpty();
                case "get", "getAt":
                    return m.get(a[0]);
                case "put":
                    return mm.put(a[0], a[1]);
                case "containsKey":
                    return m.containsKey(a[0]);
                case "keySet":
                    return new LinkedHashSet<>(m.keySet());
                case "values":
                    return new ArrayList<>(m.values());
                case "entrySet":
                    return new ArrayList<Object>(m.entrySet());
                case "remove":
                    return mm.remove(a[0]);
                case "each": {
                    for (Map.Entry<?, ?> e : new ArrayList<>(m.entrySet())) {
                        Script.callClosure(ses, (G.Closure) a[0], e.getKey(), e.getValue());
                    }
                    return t;
                }
                case "toString":
                    return Script.str(t);
                default:
                    break;
            }
        }
        if (t instanceof Map.Entry<?, ?> e) {
            switch (name) {
                case "getKey":
                    return e.getKey();
                case "getValue":
                    return e.getValue();
                default:
                    break;
            }
        }
        if (t instanceof G.TraverserView v) {
            switch (name) {
                case "get":
                    return v.value;
                case "path":
                    return v.path;
                case "sack":
                    return v.sack;
                case "loops":
                    return v.loops;
                case "bulk":
                    return v.bulk;
                default:
                    break;
            }
        }
        if (t instanceof G.Vertex vx && (name.equals("vertices") || name.equals("edges"))) {
            G.Direction dir = a.length > 0 && a[0] instanceof G.Direction d ? d : G.Direction.BOTH;
            String[] labels = Steps.strs(a.length > 0 && a[0] instanceof G.Direction ? Arrays.copyOfRange(a, 1, a.length) : a);
            List<G.Edge> es = ses.store.adjacent(List.of(vx.id), dir, labels).getOrDefault(vx.id, List.of());
            if (name.equals("edges")) {
                return new ArrayList<Object>(es).iterator();
            }
            List<Object> ids = new ArrayList<>();
            for (G.Edge e : es) {
                ids.add(e.outId.equals(vx.id) ? e.inId : e.outId);
            }
            return new ArrayList<Object>(ses.store.vertices(ids)).iterator();
        }
        if (t instanceof G.Element el) {
            switch (name) {
                case "id":
                    return el.id();
                case "label":
                    return el.label();
                case "value":
                    if (a.length == 1) {
                        Object v = Engine.propValue(el, (String) a[0]);
                        if (v == Engine.NONE) {
                            throw err("Property does not exist");
                        }
                        return v;
                    }
                    break;
                case "property": {
                    if (a.length == 1) {
                        List<Object> l = Engine.props(el, new String[] {(String) a[0]});
                        return l.isEmpty() ? null : l.get(0);
                    }
                    break;
                }
                case "keys":
                    return new LinkedHashSet<Object>(el instanceof G.Vertex v ? v.props.keySet() : ((G.Edge) el).props.keySet());
                case "properties":
                    return Engine.props(el, Steps.strs(a));
                case "toString":
                    return el.toString();
                case "equals":
                    return el.equals(a[0]);
                default:
                    break;
            }
        }
        if (t instanceof G.VProp p) {
            switch (name) {
                case "value":
                    return p.value;
                case "key", "label":
                    return p.key;
                case "id":
                    return p.id;
                default:
                    break;
            }
        }
        if (t instanceof G.Prop p) {
            switch (name) {
                case "value":
                    return p.value;
                case "key":
                    return p.key;
                default:
                    break;
            }
        }
        if (t instanceof G.Path p) {
            switch (name) {
                case "size":
                    return p.objects.size();
                case "objects":
                    return p.objects;
                case "get": {
                    if (a[0] instanceof Number n) {
                        return p.objects.get(n.intValue());
                    }
                    for (int i = 0; i < p.labels.size(); i++) {
                        if (p.labels.get(i).contains(a[0])) {
                            return p.objects.get(i);
                        }
                    }
                    return null;
                }
                default:
                    break;
            }
        }
        if (t instanceof Iterator<?> it) {
            switch (name) {
                case "hasNext":
                    return it.hasNext();
                case "next":
                    return it.next();
                default:
                    break;
            }
        }
        if (t instanceof UUID u && name.equals("toString")) {
            return u.toString();
        }
        if (t instanceof Date d && name.equals("getTime")) {
            return d.getTime();
        }
        if (t instanceof java.util.Random r) {
            if (name.equals("nextInt")) {
                return a.length == 0 ? r.nextInt() : r.nextInt(((Number) a[0]).intValue());
            }
            if (name.equals("nextDouble")) {
                return r.nextDouble();
            }
        }
        if (name.equals("toString")) {
            return Script.str(t);
        }
        if (name.equals("equals") && a.length == 1) {
            return Cmp.eq(t, a[0]);
        }
        if (name.equals("hashCode")) {
            return t.hashCode();
        }
        if (name.equals("size") && t instanceof Object[] arr) {
            return arr.length;
        }
        throw err("No signature of method: " + t.getClass().getName() + "." + name + "() is applicable for argument types: (" + typeNames(a) + ")");
    }

    private static final Object NO = new Object();

    @SuppressWarnings("unchecked")
    private static Object collectionMethod(Collection<?> c, String name, Object[] a, Script.Session ses) {
        Collection<Object> cc = (Collection<Object>) c;
        switch (name) {
            case "size", "count":
                return c.size();
            case "isEmpty":
                return c.isEmpty();
            case "get", "getAt":
                return c instanceof List<?> l ? l.get(((Number) a[0]).intValue()) : NO;
            case "first":
                return c.isEmpty() ? null : c.iterator().next();
            case "last": {
                Object last = null;
                for (Object x : c) {
                    last = x;
                }
                return last;
            }
            case "contains":
                return c.contains(a[0]);
            case "add", "leftShift":
                cc.add(a[0]);
                return true;
            case "addAll":
                cc.addAll((Collection<Object>) a[0]);
                return true;
            case "remove":
                return cc.remove(a[0]);
            case "join": {
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (Object x : c) {
                    sb.append(first ? "" : (a.length > 0 ? a[0] : "")).append(Script.str(x));
                    first = false;
                }
                return sb.toString();
            }
            case "sort": {
                List<Object> l = new ArrayList<>(c);
                l.sort(Cmp::order);
                return l;
            }
            case "reverse": {
                List<Object> l = new ArrayList<>(c);
                Collections.reverse(l);
                return l;
            }
            case "unique", "toSet":
                return new LinkedHashSet<>(c);
            case "toList", "asList":
                return new ArrayList<>(c);
            case "sum": {
                Number acc = 0;
                for (Object x : c) {
                    acc = Cmp.add(acc, (Number) x);
                }
                return acc;
            }
            case "max": {
                Object b = null;
                for (Object x : c) {
                    b = b == null || Cmp.order(x, b) > 0 ? x : b;
                }
                return b;
            }
            case "min": {
                Object b = null;
                for (Object x : c) {
                    b = b == null || Cmp.order(x, b) < 0 ? x : b;
                }
                return b;
            }
            case "take": {
                List<Object> l = new ArrayList<>();
                int n = ((Number) a[0]).intValue();
                for (Object x : c) {
                    if (l.size() >= n) {
                        break;
                    }
                    l.add(x);
                }
                return l;
            }
            case "drop": {
                List<Object> l = new ArrayList<>();
                int n = ((Number) a[0]).intValue();
                int i = 0;
                for (Object x : c) {
                    if (i++ >= n) {
                        l.add(x);
                    }
                }
                return l;
            }
            case "collect": {
                List<Object> out = new ArrayList<>();
                for (Object x : new ArrayList<>(c)) {
                    out.add(Script.callClosure(ses, (G.Closure) a[0], x));
                }
                return out;
            }
            case "findAll": {
                List<Object> out = new ArrayList<>();
                for (Object x : new ArrayList<>(c)) {
                    if (Steps.truthy(Script.callClosure(ses, (G.Closure) a[0], x))) {
                        out.add(x);
                    }
                }
                return out;
            }
            case "find": {
                for (Object x : new ArrayList<>(c)) {
                    if (Steps.truthy(Script.callClosure(ses, (G.Closure) a[0], x))) {
                        return x;
                    }
                }
                return null;
            }
            case "any": {
                for (Object x : new ArrayList<>(c)) {
                    if (Steps.truthy(Script.callClosure(ses, (G.Closure) a[0], x))) {
                        return true;
                    }
                }
                return false;
            }
            case "each": {
                for (Object x : new ArrayList<>(c)) {
                    Script.callClosure(ses, (G.Closure) a[0], x);
                }
                return c;
            }
            case "plus":
                return Script.arith("+", c, a[0]);
            case "iterator":
                return new ArrayList<>(c).iterator();
            default:
                return NO;
        }
    }
}
