package com.sayonora.wire.gremlinwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** GraphSON 3.0 (typed) and 2.0 codec of the value model. */
final class GraphSon {

    /** Plain (untyped) collections: GraphSON 2.0 and the untyped 3.0 flavour. */
    private final boolean v2;
    private final boolean untyped;

    GraphSon(boolean v2, boolean untyped) {
        this.v2 = v2 || untyped;
        this.untyped = untyped;
    }

    static final GraphSon V3 = new GraphSon(false, false);
    static final GraphSon V2 = new GraphSon(true, false);
    /** application/vnd.gremlin-v3.0+json;types=false (the HTTP endpoint's default). */
    static final GraphSon UNTYPED = new GraphSon(true, true);

    // ------------------------------------------------------------------ writing

    private JsonElement typed(String type, JsonElement value) {
        if (untyped) {
            return value;
        }
        JsonObject o = new JsonObject();
        o.addProperty("@type", type);
        o.add("@value", value);
        return o;
    }

    private static JsonElement num(Number n) {
        return new JsonPrimitive(n);
    }

    JsonElement write(Object o) {
        if (o == null) {
            return JsonNull.INSTANCE;
        }
        if (o instanceof String s) {
            return new JsonPrimitive(s);
        }
        if (o instanceof Boolean b) {
            return new JsonPrimitive(b);
        }
        if (o instanceof Integer i) {
            return typed("g:Int32", num(i));
        }
        if (o instanceof Long l) {
            return typed("g:Int64", num(l));
        }
        if (o instanceof Double d) {
            return typed("g:Double", d.isNaN() ? new JsonPrimitive("NaN") : d.isInfinite() ? new JsonPrimitive(d > 0 ? "Infinity" : "-Infinity") : num(d));
        }
        if (o instanceof Float f) {
            return typed("g:Float", f.isNaN() ? new JsonPrimitive("NaN") : f.isInfinite() ? new JsonPrimitive(f > 0 ? "Infinity" : "-Infinity") : num(f));
        }
        if (o instanceof Short s) {
            return typed("gx:Int16", num(s));
        }
        if (o instanceof Byte b) {
            return typed("gx:Byte", num(b & 0xff));
        }
        if (o instanceof BigDecimal d) {
            return typed("gx:BigDecimal", new JsonPrimitive(d));
        }
        if (o instanceof BigInteger d) {
            return typed("gx:BigInteger", new JsonPrimitive(d));
        }
        if (o instanceof Character c) {
            return typed("gx:Char", new JsonPrimitive(String.valueOf(c)));
        }
        if (o instanceof Date d) {
            return typed("g:Date", num(d.getTime()));
        }
        if (o instanceof java.time.Instant d) {
            return typed("gx:Instant", new JsonPrimitive(d.toString()));
        }
        if (o instanceof UUID u) {
            return typed("g:UUID", new JsonPrimitive(u.toString()));
        }
        if (o instanceof byte[] b) {
            return typed("gx:ByteBuffer", new JsonPrimitive(Base64.getEncoder().encodeToString(b)));
        }
        if (o instanceof G.Vertex v) {
            return typed("g:Vertex", vertex(v));
        }
        if (o instanceof G.Edge e) {
            return typed("g:Edge", edge(e));
        }
        if (o instanceof G.VProp p) {
            return typed("g:VertexProperty", vprop(p, false));
        }
        if (o instanceof G.Prop p) {
            JsonObject j = new JsonObject();
            j.addProperty("key", p.key);
            j.add("value", write(p.value));
            return typed("g:Property", j);
        }
        if (o instanceof G.Path p) {
            JsonObject j = new JsonObject();
            JsonArray labels = new JsonArray();
            for (Set<String> s : p.labels) {
                JsonArray a = new JsonArray();
                for (String x : s) {
                    a.add(x);
                }
                labels.add(v2 ? a : typed("g:Set", a));
            }
            JsonArray objs = new JsonArray();
            for (Object x : p.objects) {
                objs.add(write(x));
            }
            j.add("labels", v2 ? labels : typed("g:List", labels));
            j.add("objects", v2 ? objs : typed("g:List", objs));
            return typed("g:Path", j);
        }
        if (o instanceof G.Tree t) {
            JsonArray a = new JsonArray();
            for (Map.Entry<Object, G.Tree> e : t.entrySet()) {
                JsonObject j = new JsonObject();
                j.add("key", write(e.getKey()));
                j.add("value", write(e.getValue()));
                a.add(j);
            }
            return typed("g:Tree", a);
        }
        if (o instanceof G.GraphValue gv) {
            JsonObject j = new JsonObject();
            JsonArray vs = new JsonArray();
            for (G.Vertex v : gv.vertices()) {
                vs.add(write(v));
            }
            JsonArray es = new JsonArray();
            for (G.Edge e : gv.edges()) {
                es.add(write(e));
            }
            j.add("vertices", vs);
            j.add("edges", es);
            return typed("tinker:graph", j);
        }
        if (o instanceof G.WireTraverser t) {
            JsonObject j = new JsonObject();
            j.add("bulk", write(t.bulk));
            j.add("value", write(t.value));
            return typed("g:Traverser", j);
        }
        if (o instanceof G.T e) {
            return typed("g:T", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.Direction e) {
            return typed("g:Direction", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.Column e) {
            return typed("g:Column", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.Scope e) {
            return typed("g:Scope", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.Order e) {
            return typed("g:Order", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.Cardinality e) {
            return typed("g:Cardinality", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.Merge e) {
            return typed("g:Merge", new JsonPrimitive(e.name()));
        }
        if (o instanceof G.P p) {
            return pred(p);
        }
        if (o instanceof G.BulkSet bs) {
            JsonArray a = new JsonArray();
            for (Map.Entry<Object, Long> e : bs.entrySet()) {
                if (v2) {
                    for (long i = 0; i < e.getValue(); i++) {
                        a.add(write(e.getKey()));
                    }
                } else {
                    a.add(write(e.getKey()));
                    a.add(write(e.getValue()));
                }
            }
            return v2 ? a : typed("g:BulkSet", a);
        }
        if (o instanceof Map<?, ?> m) {
            if (v2) {
                JsonObject j = new JsonObject();
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    j.add(String.valueOf(plainKey(e.getKey())), write(e.getValue()));
                }
                return j;
            }
            JsonArray a = new JsonArray();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                a.add(write(e.getKey()));
                a.add(write(e.getValue()));
            }
            return typed("g:Map", a);
        }
        if (o instanceof Map.Entry<?, ?> e) {
            return write(G.single(e.getKey(), e.getValue()));
        }
        if (o instanceof Set<?> s) {
            JsonArray a = new JsonArray();
            for (Object x : s) {
                a.add(write(x));
            }
            return v2 ? a : typed("g:Set", a);
        }
        if (o instanceof Collection<?> c) {
            JsonArray a = new JsonArray();
            for (Object x : c) {
                a.add(write(x));
            }
            return v2 ? a : typed("g:List", a);
        }
        if (o instanceof Object[] arr) {
            JsonArray a = new JsonArray();
            for (Object x : arr) {
                a.add(write(x));
            }
            return v2 ? a : typed("g:List", a);
        }
        if (o instanceof G.Bytecode b) {
            return bytecode(b);
        }
        if (o instanceof Enum<?> e) {
            return new JsonPrimitive(e.name());
        }
        return new JsonPrimitive(String.valueOf(o));
    }

    private static Object plainKey(Object k) {
        return k instanceof G.T t ? t.name() : k;
    }

    private JsonObject vertex(G.Vertex v) {
        JsonObject j = new JsonObject();
        j.add("id", write(v.id));
        j.addProperty("label", v.label);
        if (!v.props.isEmpty()) {
            JsonObject props = new JsonObject();
            for (Map.Entry<String, List<G.VProp>> e : v.props.entrySet()) {
                JsonArray a = new JsonArray();
                for (G.VProp p : e.getValue()) {
                    a.add(typed("g:VertexProperty", vprop(p, false)));
                }
                props.add(e.getKey(), a);
            }
            j.add("properties", props);
        }
        return j;
    }

    private JsonObject vprop(G.VProp p, boolean withVertexId) {
        JsonObject j = new JsonObject();
        j.add("id", write(p.id));
        j.add("value", write(p.value));
        j.addProperty("label", p.key);
        if (withVertexId && p.vertex != null) {
            j.add("vertex", write(p.vertex.id));
        }
        if (p.meta != null && !p.meta.isEmpty()) {
            JsonObject m = new JsonObject();
            for (Map.Entry<String, Object> e : p.meta.entrySet()) {
                m.add(e.getKey(), write(e.getValue()));
            }
            j.add("properties", m);
        }
        return j;
    }

    private JsonObject edge(G.Edge e) {
        JsonObject j = new JsonObject();
        j.add("id", write(e.id));
        j.addProperty("label", e.label);
        j.addProperty("inVLabel", e.inLabel);
        j.addProperty("outVLabel", e.outLabel);
        j.add("inV", write(e.inId));
        j.add("outV", write(e.outId));
        if (!e.props.isEmpty()) {
            JsonObject props = new JsonObject();
            for (Map.Entry<String, Object> p : e.props.entrySet()) {
                JsonObject pj = new JsonObject();
                pj.addProperty("key", p.getKey());
                pj.add("value", write(p.getValue()));
                props.add(p.getKey(), typed("g:Property", pj));
            }
            j.add("properties", props);
        }
        return j;
    }

    private JsonElement pred(G.P p) {
        JsonObject j = new JsonObject();
        j.addProperty("predicate", p.op);
        if (p.left != null) {
            JsonArray a = new JsonArray();
            a.add(pred(p.left));
            a.add(pred(p.right));
            j.add("value", a);
        } else if (p.op.equals("within") || p.op.equals("without")) {
            JsonArray a = new JsonArray();
            for (Object x : p.args) {
                a.add(write(x));
            }
            j.add("value", untyped ? a : typed("g:List", a));
        } else if (p.args.size() == 1) {
            j.add("value", write(p.args.get(0)));
        } else {
            JsonArray a = new JsonArray();
            for (Object x : p.args) {
                a.add(write(x));
            }
            j.add("value", a);
        }
        return typed(p.text ? "g:TextP" : "g:P", j);
    }

    private JsonElement bytecode(G.Bytecode b) {
        JsonObject j = new JsonObject();
        JsonArray steps = new JsonArray();
        for (Object[] s : b.steps) {
            JsonArray a = new JsonArray();
            for (Object x : s) {
                a.add(write(x));
            }
            steps.add(a);
        }
        j.add("step", steps);
        return typed("g:Bytecode", j);
    }

    // ------------------------------------------------------------------ reading

    Object read(JsonElement e) {
        if (e == null || e.isJsonNull()) {
            return null;
        }
        if (e.isJsonPrimitive()) {
            JsonPrimitive p = e.getAsJsonPrimitive();
            if (p.isBoolean()) {
                return p.getAsBoolean();
            }
            if (p.isString()) {
                return p.getAsString();
            }
            BigDecimal d = p.getAsBigDecimal();
            String s = p.getAsString();
            if (s.contains(".") || s.contains("e") || s.contains("E")) {
                return d;
            }
            try {
                long l = d.longValueExact();
                return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? (Object) (int) l : (Object) l;
            } catch (ArithmeticException ex) {
                return d.toBigInteger();
            }
        }
        if (e.isJsonArray()) {
            List<Object> l = new ArrayList<>();
            for (JsonElement x : e.getAsJsonArray()) {
                l.add(read(x));
            }
            return l;
        }
        JsonObject o = e.getAsJsonObject();
        if (o.has("@type") && o.has("@value")) {
            return readTyped(o.get("@type").getAsString(), o.get("@value"));
        }
        Map<Object, Object> m = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> x : o.entrySet()) {
            m.put(x.getKey(), read(x.getValue()));
        }
        return m;
    }

    private static double dbl(JsonElement v) {
        if (v.isJsonPrimitive() && v.getAsJsonPrimitive().isString()) {
            return switch (v.getAsString()) {
                case "NaN" -> Double.NaN;
                case "Infinity" -> Double.POSITIVE_INFINITY;
                case "-Infinity" -> Double.NEGATIVE_INFINITY;
                default -> Double.parseDouble(v.getAsString());
            };
        }
        return v.getAsDouble();
    }

    private Object readTyped(String type, JsonElement v) {
        switch (type) {
            case "g:Int32":
                return v.getAsInt();
            case "g:Int64":
                return v.getAsLong();
            case "g:Double":
                return dbl(v);
            case "g:Float":
                return (float) dbl(v);
            case "gx:Int16":
                return v.getAsShort();
            case "gx:Byte":
                return (byte) v.getAsInt();
            case "gx:BigDecimal":
                return new BigDecimal(v.getAsString());
            case "gx:BigInteger":
                return new BigInteger(v.getAsString());
            case "gx:Char":
                return v.getAsString().charAt(0);
            case "g:Date", "g:Timestamp":
                return new Date(v.getAsLong());
            case "gx:Instant":
                return java.time.Instant.parse(v.getAsString());
            case "g:UUID":
                return UUID.fromString(v.getAsString());
            case "gx:ByteBuffer":
                return Base64.getDecoder().decode(v.getAsString());
            case "g:List": {
                List<Object> l = new ArrayList<>();
                for (JsonElement x : v.getAsJsonArray()) {
                    l.add(read(x));
                }
                return l;
            }
            case "g:Set": {
                Set<Object> l = new LinkedHashSet<>();
                for (JsonElement x : v.getAsJsonArray()) {
                    l.add(read(x));
                }
                return l;
            }
            case "g:BulkSet": {
                List<Object> l = new ArrayList<>();
                JsonArray a = v.getAsJsonArray();
                for (int i = 0; i + 1 < a.size(); i += 2) {
                    long bulk = ((Number) read(a.get(i + 1))).longValue();
                    Object item = read(a.get(i));
                    for (long b = 0; b < bulk; b++) {
                        l.add(item);
                    }
                }
                return l;
            }
            case "g:Map": {
                Map<Object, Object> m = new LinkedHashMap<>();
                JsonArray a = v.getAsJsonArray();
                for (int i = 0; i + 1 < a.size(); i += 2) {
                    m.put(read(a.get(i)), read(a.get(i + 1)));
                }
                return m;
            }
            case "g:T":
                return G.T.valueOf(v.getAsString());
            case "g:Direction":
                return G.Direction.valueOf(v.getAsString().toUpperCase(java.util.Locale.ROOT));
            case "g:Order":
                return G.Order.valueOf(v.getAsString());
            case "g:Scope":
                return G.Scope.valueOf(v.getAsString());
            case "g:Cardinality":
                return G.Cardinality.valueOf(v.getAsString());
            case "g:Column":
                return G.Column.valueOf(v.getAsString());
            case "g:Pick":
                return G.Pick.valueOf(v.getAsString());
            case "g:Pop":
                return G.Pop.valueOf(v.getAsString());
            case "g:Operator":
                return G.Operator.valueOf(v.getAsString());
            case "g:Merge":
                return G.Merge.valueOf(v.getAsString());
            case "g:Barrier":
                return G.Barrier.valueOf(v.getAsString());
            case "g:P", "g:TextP": {
                JsonObject o = v.getAsJsonObject();
                String op = o.get("predicate").getAsString();
                JsonElement val = o.get("value");
                List<Object> args = new ArrayList<>();
                Object rv = read(val);
                boolean multi = op.equals("within") || op.equals("without") || op.equals("and") || op.equals("or") || op.equals("between")
                        || op.equals("inside") || op.equals("outside");
                if (multi && rv instanceof Collection<?> c) {
                    args.addAll(c);
                } else {
                    args.add(rv);
                }
                return makeP(op, args, type.equals("g:TextP"));
            }
            case "g:Bytecode": {
                JsonObject o = v.getAsJsonObject();
                G.Bytecode b = new G.Bytecode();
                if (o.has("source")) {
                    for (JsonElement s : o.get("source").getAsJsonArray()) {
                        b.source.add(instr(s.getAsJsonArray()));
                    }
                }
                if (o.has("step")) {
                    for (JsonElement s : o.get("step").getAsJsonArray()) {
                        b.steps.add(instr(s.getAsJsonArray()));
                    }
                }
                return b;
            }
            case "g:Binding": {
                return read(v.getAsJsonObject().get("value"));
            }
            case "g:Lambda": {
                JsonObject o = v.getAsJsonObject();
                String script = o.get("script").getAsString();
                return Script.lambda(script);
            }
            case "g:Vertex": {
                JsonObject o = v.getAsJsonObject();
                G.Vertex vx = new G.Vertex(read(o.get("id")), o.get("label").getAsString());
                return vx;
            }
            case "g:Traverser": {
                JsonObject o = v.getAsJsonObject();
                return new G.WireTraverser(read(o.get("value")), ((Number) read(o.get("bulk"))).longValue());
            }
            case "g:Class", "g:TraversalStrategy":
                return v.isJsonObject() ? null : v.getAsString();
            default:
                if (type.equals("g:Edge") || type.equals("g:Path") || type.equals("g:Property") || type.equals("g:VertexProperty")) {
                    throw new G.GremlinError(499, "cannot use " + type + " as a request argument");
                }
                throw new G.GremlinError(499, "Unsupported GraphSON type " + type);
        }
    }

    private Object[] instr(JsonArray a) {
        Object[] out = new Object[a.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = read(a.get(i));
        }
        return out;
    }

    /** Builds a predicate from its wire form (operator name + arguments). */
    static G.P makeP(String op, List<Object> args, boolean text) {
        if (op.equals("and") || op.equals("or")) {
            if (args.size() != 2 || !(args.get(0) instanceof G.P) || !(args.get(1) instanceof G.P)) {
                throw new G.GremlinError(499, "malformed and/or predicate");
            }
            return new G.P(op, (G.P) args.get(0), (G.P) args.get(1));
        }
        if (op.equals("not") && args.size() == 1 && args.get(0) instanceof G.P p) {
            return p.negate();
        }
        List<Object> a = new ArrayList<>(args);
        if ((op.equals("within") || op.equals("without")) && a.size() == 1 && a.get(0) instanceof Collection<?> c) {
            a = new ArrayList<>(c);
        }
        return new G.P(op, text, a);
    }
}
