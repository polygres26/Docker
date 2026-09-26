package com.sayonora.warp.boltwire;

import com.sayonora.warp.boltwire.Values.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** The Cypher scalar / list / string / math function library and the aggregate function registry. */
final class Funcs {

    private Funcs() {
    }

    static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max", "collect", "percentilecont",
            "percentiledisc", "stdev", "stdevp");

    static boolean isAggregate(String name) {
        return AGGREGATES.contains(name.toLowerCase(Locale.ROOT));
    }

    /** name (lower case) to {min, max} argument counts; -1 = unbounded. */
    private static final Map<String, int[]> ARITY = new LinkedHashMap<>();

    static {
        String[][] one = {{"abs", "1"}, {"ceil", "1"}, {"floor", "1"}, {"sign", "1"}, {"sqrt", "1"}, {"exp", "1"},
                {"log", "1"}, {"log10", "1"}, {"sin", "1"}, {"cos", "1"}, {"tan", "1"}, {"cot", "1"}, {"asin", "1"},
                {"acos", "1"}, {"atan", "1"}, {"degrees", "1"}, {"radians", "1"}, {"haversin", "1"}, {"isnan", "1"},
                {"tointeger", "1"}, {"tofloat", "1"}, {"toboolean", "1"}, {"tostring", "1"}, {"tointegerornull", "1"},
                {"tofloatornull", "1"}, {"tobooleanornull", "1"}, {"tostringornull", "1"}, {"tolower", "1"},
                {"toupper", "1"}, {"lower", "1"}, {"upper", "1"}, {"trim", "1"}, {"ltrim", "1"}, {"rtrim", "1"},
                {"reverse", "1"}, {"head", "1"}, {"last", "1"}, {"tail", "1"}, {"size", "1"}, {"length", "1"},
                {"char_length", "1"}, {"character_length", "1"}, {"keys", "1"}, {"labels", "1"}, {"type", "1"},
                {"nodes", "1"}, {"relationships", "1"}, {"rels", "1"}, {"startnode", "1"}, {"endnode", "1"}, {"id", "1"},
                {"elementid", "1"}, {"properties", "1"}, {"isempty", "1"}, {"valuetype", "1"},
                {"tobytearray", "1"}, {"tostringlist", "1"}, {"tobooleanlist", "1"}, {"tointegerlist", "1"},
                {"tofloatlist", "1"}};
        for (String[] s : one) {
            ARITY.put(s[0], new int[] {1, 1});
        }
        ARITY.put("atan2", new int[] {2, 2});
        ARITY.put("round", new int[] {1, 3});
        ARITY.put("left", new int[] {2, 2});
        ARITY.put("right", new int[] {2, 2});
        ARITY.put("replace", new int[] {3, 3});
        ARITY.put("split", new int[] {2, 2});
        ARITY.put("substring", new int[] {2, 3});
        ARITY.put("range", new int[] {2, 3});
        ARITY.put("coalesce", new int[] {1, -1});
        ARITY.put("nullif", new int[] {2, 2});
        ARITY.put("pi", new int[] {0, 0});
        ARITY.put("e", new int[] {0, 0});
        ARITY.put("rand", new int[] {0, 0});
        ARITY.put("randomuuid", new int[] {0, 0});
        ARITY.put("timestamp", new int[] {0, 0});
        ARITY.put("point", new int[] {1, 1});
                ARITY.put("point.distance", new int[] {2, 2});
        ARITY.put("normalize", new int[] {1, 2});
        ARITY.put("count", new int[] {1, 1});
        ARITY.put("sum", new int[] {1, 1});
        ARITY.put("avg", new int[] {1, 1});
        ARITY.put("min", new int[] {1, 1});
        ARITY.put("max", new int[] {1, 1});
        ARITY.put("collect", new int[] {1, 1});
        ARITY.put("stdev", new int[] {1, 1});
        ARITY.put("stdevp", new int[] {1, 1});
        ARITY.put("percentilecont", new int[] {2, 2});
        ARITY.put("percentiledisc", new int[] {2, 2});
    }

    static List<String> names() {
        return new ArrayList<>(ARITY.keySet());
    }

    /** Null when the function does not exist; {min,max} otherwise. */
    static int[] arity(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        int[] a = ARITY.get(n);
        if (a != null) {
            return a;
        }
        return Temporal.arity(n);
    }

    // ---------------------------------------------------------------------------------------- scalar functions

    static Object call(Eval ev, String name, List<Object> a, Map<String, Object> row) {
        String n = name.toLowerCase(Locale.ROOT);
        switch (n) {
            case "coalesce" -> {
                for (Object o : a) {
                    if (o != null) {
                        return o;
                    }
                }
                return null;
            }
            case "nullif" -> {
                return Boolean.TRUE.equals(Values.equal(a.get(0), a.get(1))) ? null : a.get(0);
            }
            case "pi" -> {
                return Math.PI;
            }
            case "e" -> {
                return Math.E;
            }
            case "rand" -> {
                return Math.random();
            }
            case "randomuuid" -> {
                return java.util.UUID.randomUUID().toString();
            }
            case "timestamp" -> {
                return ev.x.startedAt.toEpochMilli();
            }
            case "range" -> {
                return range(a);
            }
            default -> {
            }
        }
        Object x0 = a.isEmpty() ? null : a.get(0);
        switch (n) {
            case "isempty" -> {
                if (x0 == null) {
                    return null;
                }
                if (x0 instanceof List<?> l) {
                    return l.isEmpty();
                }
                if (x0 instanceof Map<?, ?> m) {
                    return m.isEmpty();
                }
                if (x0 instanceof String s) {
                    return s.isEmpty();
                }
                throw Eval.typeMismatch("String, Map or List", x0);
            }
            case "valuetype" -> {
                return Values.valueTypeName(x0);
            }
            default -> {
            }
        }
        if (!n.startsWith("point") && !Temporal.handlesNull(n)) {
            for (Object o : a) {
                if (o == null) {
                    return null; // every remaining function is null-propagating
                }
            }
        }
        switch (n) {
            case "abs" -> {
                if (x0 instanceof Long l) {
                    return Math.abs(l);
                }
                if (x0 instanceof Double d) {
                    return Math.abs(d);
                }
                throw Eval.typeMismatch("Integer or Float", x0);
            }
            case "ceil" -> {
                return Math.ceil(num(x0));
            }
            case "floor" -> {
                return Math.floor(num(x0));
            }
            case "round" -> {
                return round(a);
            }
            case "sign" -> {
                double d = num(x0);
                return d > 0 ? 1L : d < 0 ? -1L : 0L;
            }
            case "sqrt" -> {
                return Math.sqrt(num(x0));
            }
            case "exp" -> {
                return Math.exp(num(x0));
            }
            case "log" -> {
                return Math.log(num(x0));
            }
            case "log10" -> {
                return Math.log10(num(x0));
            }
            case "sin" -> {
                return Math.sin(num(x0));
            }
            case "cos" -> {
                return Math.cos(num(x0));
            }
            case "tan" -> {
                return Math.tan(num(x0));
            }
            case "cot" -> {
                return 1.0 / Math.tan(num(x0));
            }
            case "asin" -> {
                return Math.asin(num(x0));
            }
            case "acos" -> {
                return Math.acos(num(x0));
            }
            case "atan" -> {
                return Math.atan(num(x0));
            }
            case "atan2" -> {
                if (a.get(1) == null) {
                    return null;
                }
                return Math.atan2(num(x0), num(a.get(1)));
            }
            case "degrees" -> {
                return Math.toDegrees(num(x0));
            }
            case "radians" -> {
                return Math.toRadians(num(x0));
            }
            case "haversin" -> {
                return (1 - Math.cos(num(x0))) / 2;
            }
            case "isnan" -> {
                return Double.isNaN(num(x0));
            }
            case "tointeger" -> {
                return toInteger(x0, true);
            }
            case "tointegerornull" -> {
                return toInteger(x0, false);
            }
            case "tofloat" -> {
                return toFloat(x0, true);
            }
            case "tofloatornull" -> {
                return toFloat(x0, false);
            }
            case "toboolean" -> {
                return toBoolean(x0, true);
            }
            case "tobooleanornull" -> {
                return toBoolean(x0, false);
            }
            case "tostring" -> {
                return toStringFn(x0, true);
            }
            case "tostringornull" -> {
                return toStringFn(x0, false);
            }
            case "tolower", "lower" -> {
                return string(x0, n).toLowerCase(Locale.ROOT);
            }
            case "toupper", "upper" -> {
                return string(x0, n).toUpperCase(Locale.ROOT);
            }
            case "trim" -> {
                return trim(string(x0, n), true, true);
            }
            case "ltrim" -> {
                return trim(string(x0, n), true, false);
            }
            case "rtrim" -> {
                return trim(string(x0, n), false, true);
            }
            case "left" -> {
                String s = string(x0, n);
                if (a.get(1) == null) {
                    throw CypherException.argument("Invalid input 'null' for argument 'length'");
                }
                long len = Eval.toLong(a.get(1));
                if (len < 0) {
                    throw new CypherException("Neo.DatabaseError.Statement.ExecutionFailed", "Invalid input for length value in function 'left()': Expected a non-negative value");
                }
                int[] cps = s.codePoints().toArray();
                return new String(cps, 0, (int) Math.min(len, cps.length));
            }
            case "right" -> {
                String s = string(x0, n);
                if (a.get(1) == null) {
                    throw CypherException.argument("Invalid input 'null' for argument 'length'");
                }
                long len = Eval.toLong(a.get(1));
                if (len < 0) {
                    throw new CypherException("Neo.DatabaseError.Statement.ExecutionFailed", "Invalid input for length value in function 'right()': Expected a non-negative value");
                }
                int[] cps = s.codePoints().toArray();
                int from = (int) Math.max(0, cps.length - len);
                return new String(cps, from, cps.length - from);
            }
            case "replace" -> {
                if (a.get(1) == null || a.get(2) == null) {
                    return null;
                }
                return string(x0, n).replace(string(a.get(1), n), string(a.get(2), n));
            }
            case "split" -> {
                String s = string(x0, n);
                if (a.get(1) == null) {
                    return null;
                }
                String d = string(a.get(1), n);
                List<Object> out = new ArrayList<>();
                if (d.isEmpty()) {
                    s.codePoints().forEach(cp -> out.add(new String(Character.toChars(cp))));
                    return out;
                }
                int from = 0;
                while (true) {
                    int i = s.indexOf(d, from);
                    if (i < 0) {
                        out.add(s.substring(from));
                        return out;
                    }
                    out.add(s.substring(from, i));
                    from = i + d.length();
                }
            }
            case "substring" -> {
                String s = string(x0, n);
                if (a.get(1) == null) {
                    throw CypherException.argument("Invalid input 'null' for argument 'start'");
                }
                long start = Eval.toLong(a.get(1));
                if (start < 0) {
                    throw new CypherException("Neo.DatabaseError.Statement.ExecutionFailed", "Invalid input for start value in function 'substring()': Expected a non-negative value");
                }
                int[] cps = s.codePoints().toArray();
                if (start >= cps.length) {
                    return "";
                }
                long len = cps.length - start;
                if (a.size() > 2) {
                    if (a.get(2) == null) {
                        throw CypherException.argument("Invalid input 'null' for argument 'length'");
                    }
                    long l = Eval.toLong(a.get(2));
                    if (l < 0) {
                        throw new CypherException("Neo.DatabaseError.Statement.ExecutionFailed", "Invalid input for length value in function 'substring()': Expected a non-negative value");
                    }
                    len = Math.min(len, l);
                }
                return new String(cps, (int) start, (int) len);
            }
            case "reverse" -> {
                if (x0 instanceof String s) {
                    return new StringBuilder(s).reverse().toString();
                }
                if (x0 instanceof List<?> l) {
                    List<Object> out = new ArrayList<>(l);
                    Collections.reverse(out);
                    return out;
                }
                throw Eval.typeMismatch("String or List", x0);
            }
            case "head" -> {
                List<?> l = list(x0);
                return l.isEmpty() ? null : l.get(0);
            }
            case "last" -> {
                List<?> l = list(x0);
                return l.isEmpty() ? null : l.get(l.size() - 1);
            }
            case "tail" -> {
                List<?> l = list(x0);
                return l.isEmpty() ? new ArrayList<>() : new ArrayList<>(l.subList(1, l.size()));
            }
            case "size" -> {
                if (x0 instanceof List<?> l) {
                    return (long) l.size();
                }
                if (x0 instanceof String s) {
                    return (long) s.codePointCount(0, s.length());
                }
                throw Eval.typeMismatch("String or List", x0);
            }
            case "char_length", "character_length" -> {
                String s = string(x0, n);
                return (long) s.codePointCount(0, s.length());
            }
            case "length" -> {
                if (x0 instanceof PathV p) {
                    return (long) p.rels().size();
                }
                if (x0 instanceof List<?> || x0 instanceof String) {
                    throw CypherException.syntax("Type mismatch: expected Path but was " + Values.typeName(x0));
                }
                throw Eval.typeMismatch("Path", x0);
            }
            case "keys" -> {
                Map<String, Object> m;
                if (x0 instanceof NodeV nn) {
                    Exec.checkLive(nn);
                    m = nn.props;
                } else if (x0 instanceof RelV r) {
                    Exec.checkLive(r);
                    m = r.props;
                } else if (x0 instanceof Map<?, ?> mm) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> cast = (Map<String, Object>) mm;
                    m = cast;
                } else {
                    throw Eval.typeMismatch("Map, Node or Relationship", x0);
                }
                List<Object> out = new ArrayList<>();
                boolean isMap = x0 instanceof Map;
                for (Map.Entry<String, Object> e : m.entrySet()) {
                    if (isMap || e.getValue() != null) {
                        out.add(e.getKey());
                    }
                }
                return out;
            }
            case "properties" -> {
                if (x0 instanceof NodeV nn) {
                    Exec.checkLive(nn);
                    return new LinkedHashMap<>(nn.props);
                }
                if (x0 instanceof RelV r) {
                    Exec.checkLive(r);
                    return new LinkedHashMap<>(r.props);
                }
                if (x0 instanceof Map<?, ?> mm) {
                    return new LinkedHashMap<>(mm);
                }
                throw Eval.typeMismatch("Map, Node or Relationship", x0);
            }
            case "labels" -> {
                if (x0 instanceof NodeV nn) {
                    Exec.checkLive(nn);
                    return new ArrayList<Object>(nn.labels);
                }
                throw Eval.typeMismatch("Node", x0);
            }
            case "type" -> {
                if (x0 instanceof RelV r) {
                    return r.type;
                }
                throw Eval.typeMismatch("Relationship", x0);
            }
            case "nodes" -> {
                if (x0 instanceof PathV p) {
                    return new ArrayList<Object>(p.nodes());
                }
                throw Eval.typeMismatch("Path", x0);
            }
            case "relationships", "rels" -> {
                if (x0 instanceof PathV p) {
                    return new ArrayList<Object>(p.rels());
                }
                throw Eval.typeMismatch("Path", x0);
            }
            case "startnode" -> {
                if (x0 instanceof RelV r) {
                    return endpoint(ev, r.start);
                }
                throw Eval.typeMismatch("Relationship", x0);
            }
            case "endnode" -> {
                if (x0 instanceof RelV r) {
                    return endpoint(ev, r.end);
                }
                throw Eval.typeMismatch("Relationship", x0);
            }
            case "id" -> {
                if (x0 instanceof NodeV nn) {
                    return nn.id;
                }
                if (x0 instanceof RelV r) {
                    return r.id;
                }
                throw Eval.typeMismatch("Node or Relationship", x0);
            }
            case "elementid" -> {
                if (x0 instanceof NodeV nn) {
                    return elementId(4, nn.id);
                }
                if (x0 instanceof RelV r) {
                    return elementId(5, r.id);
                }
                throw Eval.typeMismatch("Node or Relationship", x0);
            }
            case "point" -> {
                return Temporal.point(x0);
            }
            case "point.distance" -> {
                return Temporal.distance(a.get(0), a.get(1));
            }
            case "normalize" -> {
                return java.text.Normalizer.normalize(string(x0, n), java.text.Normalizer.Form.NFC);
            }
            case "tostringlist", "tobooleanlist", "tointegerlist", "tofloatlist" -> {
                List<?> l = list(x0);
                List<Object> out = new ArrayList<>();
                for (Object o : l) {
                    out.add(switch (n) {
                        case "tostringlist" -> toStringFn(o, false);
                        case "tobooleanlist" -> toBoolean(o, false);
                        case "tointegerlist" -> toInteger(o, false);
                        default -> toFloat(o, false);
                    });
                }
                return out;
            }
            default -> {
            }
        }
        Object t = Temporal.call(ev, n, a);
        if (t != Temporal.NOT_TEMPORAL) {
            return t;
        }
        throw CypherException.syntax("Unknown function '" + name + "'");
    }

    static String elementId(int kind, long id) {
        return kind + ":warp:" + id;
    }

    private static Object endpoint(Eval ev, long id) {
        try {
            NodeV n = ev.x.node(id);
            if (n == null) {
                throw new CypherException(CypherException.ENTITY_NOT_FOUND, "Node with id " + id + " not found");
            }
            return n;
        } catch (java.sql.SQLException e) {
            throw ev.x.sql(e);
        }
    }

    private static double num(Object o) {
        if (o instanceof Long l) {
            return l;
        }
        if (o instanceof Double d) {
            return d;
        }
        throw Eval.typeMismatch("Integer or Float", o);
    }

    private static String string(Object o, String fn) {
        if (o instanceof String s) {
            return s;
        }
        throw Eval.typeMismatch("String", o);
    }

    private static List<?> list(Object o) {
        if (o instanceof List<?> l) {
            return l;
        }
        throw Eval.typeMismatch("List", o);
    }

    private static String trim(String s, boolean left, boolean right) {
        int b = 0, e = s.length();
        if (left) {
            while (b < e && isWs(s.charAt(b))) {
                b++;
            }
        }
        if (right) {
            while (e > b && isWs(s.charAt(e - 1))) {
                e--;
            }
        }
        return s.substring(b, e);
    }

    private static boolean isWs(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c);
    }

    private static Object round(List<Object> a) {
        Object v = a.get(0);
        if (v == null) {
            return null;
        }
        double d = num(v);
        if (a.size() == 1) {
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return d;
            }
            return Math.floor(d + 0.5);
        }
        Object p = a.get(1);
        if (p == null) {
            return null;
        }
        long prec = Eval.toLong(p);
        String mode = a.size() > 2 && a.get(2) != null ? String.valueOf(a.get(2)).toUpperCase(Locale.ROOT) : "HALF_UP";
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return d;
        }
        java.math.RoundingMode rm = switch (mode) {
            case "UP" -> java.math.RoundingMode.UP;
            case "DOWN" -> java.math.RoundingMode.DOWN;
            case "CEILING" -> java.math.RoundingMode.CEILING;
            case "FLOOR" -> java.math.RoundingMode.FLOOR;
            case "HALF_DOWN" -> java.math.RoundingMode.HALF_DOWN;
            case "HALF_EVEN" -> java.math.RoundingMode.HALF_EVEN;
            default -> java.math.RoundingMode.HALF_UP;
        };
        return new java.math.BigDecimal(Double.toString(d)).setScale((int) prec, rm).doubleValue();
    }

    private static Object range(List<Object> a) {
        if (a.get(0) == null || a.get(1) == null || (a.size() > 2 && a.get(2) == null)) {
            return null;
        }
        long start = Eval.toLong(a.get(0)), end = Eval.toLong(a.get(1));
        long step = a.size() > 2 ? Eval.toLong(a.get(2)) : 1;
        if (step == 0) {
            throw CypherException.argument("step argument to range() cannot be zero");
        }
        List<Object> out = new ArrayList<>();
        if (step > 0) {
            for (long i = start; i <= end; i += step) {
                out.add(i);
                if (out.size() > 50_000_000) {
                    throw CypherException.argument("range() result too large");
                }
                if (i > Long.MAX_VALUE - step) {
                    break;
                }
            }
        } else {
            for (long i = start; i >= end; i += step) {
                out.add(i);
                if (out.size() > 50_000_000) {
                    throw CypherException.argument("range() result too large");
                }
                if (i < Long.MIN_VALUE - step) {
                    break;
                }
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------------------------- conversions

    static String str(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof String s) {
            return s;
        }
        if (v instanceof Long || v instanceof Boolean) {
            return v.toString();
        }
        if (v instanceof Double d) {
            return doubleToString(d);
        }
        return null;
    }

    static String doubleToString(double d) {
        if (Double.isNaN(d)) {
            return "NaN";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "Infinity" : "-Infinity";
        }
        return Double.toString(d);
    }

    private static Object toInteger(Object v, boolean strict) {
        if (v == null) {
            return null;
        }
        if (v instanceof Long) {
            return v;
        }
        if (v instanceof Double d) {
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return null;
            }
            return (long) d.doubleValue();
        }
        if (v instanceof Boolean b) {
            return b ? 1L : 0L;
        }
        if (v instanceof String s) {
            String t = s.strip();
            try {
                return Long.parseLong(t);
            } catch (NumberFormatException e) {
                try {
                    if (t.matches("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?")) {
                        double d = Double.parseDouble(t);
                        if (d >= 9.223372036854775807E18 || d < -9.223372036854775808E18) {
                            return null;
                        }
                        return (long) d;
                    }
                } catch (NumberFormatException e2) {
                    // fall through
                }
                return null;
            }
        }
        if (strict) {
            throw CypherException.type("Invalid input for function 'toInteger()': Expected a String, Float, Integer or Boolean, got: "
                    + describe(v));
        }
        return null;
    }

    private static Object toFloat(Object v, boolean strict) {
        if (v == null) {
            return null;
        }
        if (v instanceof Double) {
            return v;
        }
        if (v instanceof Long l) {
            return (double) l;
        }
        if (v instanceof String s) {
            String t = s.strip();
            if (t.matches("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?") || t.equals("Infinity") || t.equals("-Infinity")
                    || t.equals("NaN")) {
                return Double.parseDouble(t);
            }
            return null;
        }
        if (strict) {
            throw CypherException.type("Invalid input for function 'toFloat()': Expected a String, Float or Integer, got: "
                    + describe(v));
        }
        return null;
    }

    private static Object toBoolean(Object v, boolean strict) {
        if (v == null) {
            return null;
        }
        if (v instanceof Boolean) {
            return v;
        }
        if (v instanceof String s) {
            if (s.equalsIgnoreCase("true")) {
                return true;
            }
            if (s.equalsIgnoreCase("false")) {
                return false;
            }
            return null;
        }
        if (v instanceof Long l) {
            return l != 0;
        }
        if (strict) {
            throw CypherException.type("Invalid input for function 'toBoolean()': Expected a String, Boolean or Integer, got: "
                    + describe(v));
        }
        return null;
    }

    private static Object toStringFn(Object v, boolean strict) {
        if (v == null) {
            return null;
        }
        if (v instanceof String || v instanceof Long || v instanceof Boolean || v instanceof Double) {
            return str(v);
        }
        String t = Temporal.toStringValue(v);
        if (t != null) {
            return t;
        }
        if (strict) {
            throw CypherException.type("Invalid input for function 'toString()': Expected a String, Float, Integer, Boolean, "
                    + "Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime, got: " + describe(v));
        }
        return null;
    }

    static String describe(Object v) {
        return Values.typeName(v) + "(" + v + ")";
    }

    // ---------------------------------------------------------------------------------------- aggregation

    /** One aggregate function instance for one group. */
    static final class Agg {
        final String fn;
        final boolean distinct;
        long count;
        Object sum = 0L;
        boolean sumFloat;
        double dsum;
        Object best;
        List<Object> items;
        java.util.Set<Object> seen;
        double mean, m2;
        boolean sumSeen;
        Object pctArg;
        Object durSum;

        Agg(String fn, boolean distinct) {
            this.fn = fn.toLowerCase(Locale.ROOT);
            this.distinct = distinct;
            if (distinct) {
                seen = new java.util.HashSet<>();
            }
            if (this.fn.equals("collect") || this.fn.startsWith("percentile")) {
                items = new ArrayList<>();
            }
        }

        void add(Object v, Object extra) {
            if (v == null) {
                return;
            }
            if (distinct && !seen.add(Values.groupKey(v))) {
                return;
            }
            switch (fn) {
                case "count" -> count++;
                case "collect" -> items.add(v);
                case "sum", "avg" -> {
                    count++;
                    if (v instanceof DurationV d) {
                        durSum = durSum == null ? d : Temporal.addDurations((DurationV) durSum, d);
                    } else if (v instanceof Long l) {
                        if (!sumFloat) {
                            try {
                                sum = Math.addExact((Long) sum, l);
                            } catch (ArithmeticException e) {
                                sumFloat = true;
                            }
                        }
                        dsum += l;
                    } else if (v instanceof Double d) {
                        sumFloat = true;
                        dsum += d;
                    } else {
                        throw Eval.typeMismatch("Integer, Float or Duration", v);
                    }
                }
                case "min", "max" -> {
                    if (best == null) {
                        best = v;
                    } else {
                        int c = Values.order(v, best);
                        if (fn.equals("min") ? c < 0 : c > 0) {
                            best = v;
                        }
                    }
                }
                case "stdev", "stdevp" -> {
                    if (!Values.isNumber(v)) {
                        throw Eval.typeMismatch("Integer or Float", v);
                    }
                    double x = ((Number) v).doubleValue();
                    count++;
                    double delta = x - mean;
                    mean += delta / count;
                    m2 += delta * (x - mean);
                }
                case "percentilecont", "percentiledisc" -> {
                    if (!Values.isNumber(v)) {
                        throw Eval.typeMismatch("Integer or Float", v);
                    }
                    items.add(v);
                    pctArg = extra;
                }
                default -> throw CypherException.syntax("Unknown aggregate " + fn);
            }
        }

        Object result() {
            switch (fn) {
                case "count" -> {
                    return count;
                }
                case "collect" -> {
                    return items;
                }
                case "sum" -> {
                    if (durSum != null) {
                        return durSum;
                    }
                    return sumFloat ? (Object) dsum : sum;
                }
                case "avg" -> {
                    if (count == 0) {
                        return null;
                    }
                    if (durSum != null) {
                        return Temporal.divideDuration((DurationV) durSum, count);
                    }
                    return dsum / count;
                }
                case "min", "max" -> {
                    return best;
                }
                case "stdev" -> {
                    return count < 2 ? 0.0 : Math.sqrt(m2 / (count - 1));
                }
                case "stdevp" -> {
                    return count == 0 ? 0.0 : Math.sqrt(m2 / count);
                }
                case "percentilecont", "percentiledisc" -> {
                    if (items.isEmpty()) {
                        return null;
                    }
                    if (pctArg == null || !Values.isNumber(pctArg)) {
                        throw CypherException.argument("Invalid input for percentile: expected a number");
                    }
                    double p = ((Number) pctArg).doubleValue();
                    if (p < 0 || p > 1) {
                        throw CypherException.argument("Invalid input '" + p + "' is not a valid argument, must be a number in the range 0.0 to 1.0");
                    }
                    List<Object> sorted = new ArrayList<>(items);
                    sorted.sort(Values::order);
                    int n = sorted.size();
                    if (fn.equals("percentiledisc")) {
                        int idx = (int) Math.max(0, Math.ceil(p * n) - 1);
                        return sorted.get(Math.min(idx, n - 1));
                    }
                    double pos = p * (n - 1);
                    int lo = (int) Math.floor(pos), hi = (int) Math.ceil(pos);
                    if (pos == lo) {
                        return sorted.get(lo);
                    }
                    double a = ((Number) sorted.get(lo)).doubleValue(), b = ((Number) sorted.get(hi)).doubleValue();
                    return a + (b - a) * (pos - lo);
                }
                default -> throw CypherException.syntax("Unknown aggregate " + fn);
            }
        }
    }
}
