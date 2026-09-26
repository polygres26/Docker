package com.sayonora.wire.cosmoswire;

import static com.sayonora.wire.cosmoswire.CosmosJson.NULL;
import static com.sayonora.wire.cosmoswire.CosmosJson.bool;
import static com.sayonora.wire.cosmoswire.CosmosJson.dbl;
import static com.sayonora.wire.cosmoswire.CosmosJson.isBool;
import static com.sayonora.wire.cosmoswire.CosmosJson.isNum;
import static com.sayonora.wire.cosmoswire.CosmosJson.isStr;
import static com.sayonora.wire.cosmoswire.CosmosJson.num;
import static com.sayonora.wire.cosmoswire.CosmosJson.str;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.cosmoswire.CosmosSql.*;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Expression evaluation of the Cosmos SQL dialect (three-valued logic with undefined = Java null). */
final class CosmosEval {

    /** Variable bindings (aliases) chained to the enclosing scope of a correlated subquery; also carries aggregate results. */
    static final class Scope {
        final Scope parent;
        final String name;
        final JsonElement value;
        final IdentityHashMap<Call, JsonElement> aggs;

        Scope(Scope parent, String name, JsonElement value) {
            this(parent, name, value, parent == null ? null : parent.aggs);
        }

        private Scope(Scope parent, String name, JsonElement value, IdentityHashMap<Call, JsonElement> aggs) {
            this.parent = parent;
            this.name = name;
            this.value = value;
            this.aggs = aggs;
        }

        Scope withAggs(IdentityHashMap<Call, JsonElement> a) {
            return new Scope(parent, name, value, a);
        }

        Scope bind(String n, JsonElement v) {
            return new Scope(this, n, v);
        }

        boolean has(String n) {
            for (Scope s = this; s != null; s = s.parent) {
                if (n.equals(s.name)) {
                    return true;
                }
            }
            return false;
        }

        JsonElement get(String n) {
            for (Scope s = this; s != null; s = s.parent) {
                if (n.equals(s.name)) {
                    return s.value;
                }
            }
            return null;
        }
    }

    final Map<String, JsonElement> params;
    final long nowMillis;
    private final Map<String, Pattern> patterns = new java.util.HashMap<>();
    private final CosmosQuery.SubRunner subRunner;

    CosmosEval(Map<String, JsonElement> params, long nowMillis, CosmosQuery.SubRunner subRunner) {
        this.params = params;
        this.nowMillis = nowMillis;
        this.subRunner = subRunner;
    }

    static boolean isTrue(JsonElement e) {
        return isBool(e) && e.getAsBoolean();
    }

    JsonElement eval(Expr e, Scope s) {
        if (e instanceof Lit l) {
            return l.value();
        }
        if (e instanceof Ident i) {
            if (s == null || !s.has(i.name())) {
                throw CosmosException.badRequest("Identifier '" + i.name() + "' could not be resolved.");
            }
            return s.get(i.name());
        }
        if (e instanceof Param p) {
            if (!params.containsKey(p.name())) {
                throw CosmosException.badRequest("Parameter '" + p.name() + "' was not supplied.");
            }
            return params.get(p.name());
        }
        if (e instanceof Prop p) {
            return CosmosJson.prop(eval(p.base(), s), p.name());
        }
        if (e instanceof Index x) {
            JsonElement b = eval(x.base(), s);
            JsonElement i = eval(x.index(), s);
            if (b == null || i == null) {
                return null;
            }
            if (b.isJsonArray() && isNum(i)) {
                double d = dbl(i);
                JsonArray a = b.getAsJsonArray();
                return d == Math.rint(d) && d >= 0 && d < a.size() ? a.get((int) d) : null;
            }
            if (b.isJsonObject() && isStr(i)) {
                return b.getAsJsonObject().get(i.getAsString());
            }
            return null;
        }
        if (e instanceof Unary u) {
            return unary(u.op(), eval(u.e(), s));
        }
        if (e instanceof Binary b) {
            return binary(b, s);
        }
        if (e instanceof Ternary t) {
            JsonElement c = eval(t.cond(), s);
            if (c == null || !isBool(c)) {
                return null;
            }
            return c.getAsBoolean() ? eval(t.a(), s) : eval(t.b(), s);
        }
        if (e instanceof InList in) {
            JsonElement v = eval(in.e(), s);
            if (v == null) {
                return null;
            }
            boolean found = false;
            for (Expr x : in.list()) {
                JsonElement y = eval(x, s);
                if (y != null && CosmosJson.equal(v, y)) {
                    found = true;
                    break;
                }
            }
            return bool(found != in.negated());
        }
        if (e instanceof Between bt) {
            JsonElement v = eval(bt.e(), s);
            JsonElement lo = eval(bt.lo(), s);
            JsonElement hi = eval(bt.hi(), s);
            JsonElement a = order(">=", v, lo);
            JsonElement b = order("<=", v, hi);
            JsonElement r = and(a, b);
            return bt.negated() ? not(r) : r;
        }
        if (e instanceof Like lk) {
            JsonElement v = eval(lk.e(), s);
            JsonElement p = eval(lk.pattern(), s);
            JsonElement esc = lk.escape() == null ? null : eval(lk.escape(), s);
            if (!isStr(v) || !isStr(p)) {
                return null;
            }
            boolean m = likePattern(p.getAsString(), isStr(esc) && esc.getAsString().length() == 1 ? esc.getAsString().charAt(0) : 0)
                    .matcher(v.getAsString()).matches();
            return bool(m != lk.negated());
        }
        if (e instanceof ArrayLit a) {
            JsonArray out = new JsonArray();
            for (Expr x : a.items()) {
                JsonElement v = eval(x, s);
                out.add(v == null ? NULL : v);
            }
            return out;
        }
        if (e instanceof ObjLit o) {
            JsonObject out = new JsonObject();
            for (Map.Entry<String, Expr> en : o.fields().entrySet()) {
                JsonElement v = eval(en.getValue(), s);
                if (v != null) {
                    out.add(en.getKey(), v);
                }
            }
            return out;
        }
        if (e instanceof Sub sq) {
            List<JsonElement> rows = subRunner.run(sq.query(), s);
            return switch (sq.kind()) {
                case EXISTS -> bool(!rows.isEmpty());
                case ARRAY -> {
                    JsonArray a = new JsonArray();
                    rows.forEach(a::add);
                    yield a;
                }
                default -> rows.isEmpty() ? null : rows.get(0);
            };
        }
        if (e instanceof Call c) {
            return call(c, s);
        }
        throw CosmosException.badRequest("Unsupported expression");
    }

    // ------------------------------------------------------------------------------------------ operators

    static JsonElement not(JsonElement v) {
        return isBool(v) ? bool(!v.getAsBoolean()) : null;
    }

    /** Three-valued AND: false wins over undefined. */
    static JsonElement and(JsonElement a, JsonElement b) {
        if (isBool(a) && !a.getAsBoolean() || isBool(b) && !b.getAsBoolean()) {
            return CosmosJson.FALSE;
        }
        if (isBool(a) && isBool(b)) {
            return CosmosJson.TRUE;
        }
        return null;
    }

    static JsonElement or(JsonElement a, JsonElement b) {
        if (isBool(a) && a.getAsBoolean() || isBool(b) && b.getAsBoolean()) {
            return CosmosJson.TRUE;
        }
        if (isBool(a) && isBool(b)) {
            return CosmosJson.FALSE;
        }
        return null;
    }

    private JsonElement unary(String op, JsonElement v) {
        switch (op) {
            case "NOT":
                return not(v);
            case "-":
                return isNum(v) ? num(-dbl(v)) : null;
            case "+":
                return isNum(v) ? v : null;
            case "~":
                return isNum(v) ? num((long) ~(int) (long) dbl(v)) : null;
            default:
                return null;
        }
    }

    private JsonElement binary(Binary b, Scope s) {
        String op = b.op();
        // short-circuiting operators
        if (op.equals("AND")) {
            JsonElement l = eval(b.l(), s);
            if (isBool(l) && !l.getAsBoolean()) {
                return l;
            }
            return and(l, eval(b.r(), s));
        }
        if (op.equals("OR")) {
            JsonElement l = eval(b.l(), s);
            if (isBool(l) && l.getAsBoolean()) {
                return l;
            }
            return or(l, eval(b.r(), s));
        }
        if (op.equals("??")) {
            JsonElement l = eval(b.l(), s);
            return l != null ? l : eval(b.r(), s);
        }
        JsonElement l = eval(b.l(), s);
        JsonElement r = eval(b.r(), s);
        switch (op) {
            case "=":
                return l == null || r == null ? null : bool(CosmosJson.equal(l, r));
            case "!=":
                return l == null || r == null ? null : bool(!CosmosJson.equal(l, r));
            case "<":
            case "<=":
            case ">":
            case ">=":
                return order(op, l, r);
            case "||":
                return isStr(l) && isStr(r) ? str(l.getAsString() + r.getAsString()) : null;
            default:
                break;
        }
        if (!isNum(l) || !isNum(r)) {
            return null;
        }
        double x = dbl(l);
        double y = dbl(r);
        switch (op) {
            case "+":
                return num(x + y);
            case "-":
                return num(x - y);
            case "*":
                return num(x * y);
            case "/":
                return y == 0 ? null : num(x / y);
            case "%":
                return y == 0 ? null : num(x % y);
            case "&":
                return num((long) ((int) (long) x & (int) (long) y));
            case "|":
                return num((long) ((int) (long) x | (int) (long) y));
            case "^":
                return num((long) ((int) (long) x ^ (int) (long) y));
            case "<<":
                return num((long) ((int) (long) x << (int) (long) y));
            case ">>":
                return num((long) ((int) (long) x >> (int) (long) y));
            case ">>>":
                return num((long) ((int) (long) x >>> (int) (long) y));
            default:
                return null;
        }
    }

    /** Ordering comparison: defined only between two values of the same primitive type. */
    static JsonElement order(String op, JsonElement a, JsonElement b) {
        if (a == null || b == null) {
            return null;
        }
        int ra = CosmosJson.rank(a);
        if (ra != CosmosJson.rank(b) || ra > 4) {
            return null;
        }
        int c = CosmosJson.compare(a, b);
        return bool(switch (op) {
            case "<" -> c < 0;
            case "<=" -> c <= 0;
            case ">" -> c > 0;
            default -> c >= 0;
        });
    }

    static Pattern likePattern(String like, char escape) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            if (escape != 0 && c == escape && i + 1 < like.length()) {
                sb.append(Pattern.quote(String.valueOf(like.charAt(++i))));
            } else if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else {
                sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(sb.toString(), Pattern.DOTALL);
    }

    // ------------------------------------------------------------------------------------------ functions

    private JsonElement call(Call c, Scope s) {
        String name = c.name().toUpperCase(Locale.ROOT);
        if (CosmosSql.AGGREGATES.contains(name)) {
            if (s == null || s.aggs == null || !s.aggs.containsKey(c)) {
                throw CosmosException.badRequest("Aggregate function " + name + " is not allowed in this position.");
            }
            return s.aggs.get(c);
        }
        List<JsonElement> a = new ArrayList<>(c.args().size());
        for (Expr x : c.args()) {
            a.add(eval(x, s));
        }
        return CosmosFunctions.call(this, name, a);
    }

    Pattern regex(String pattern, String modifiers) {
        String key = modifiers + "/" + pattern;
        Pattern p = patterns.get(key);
        if (p == null) {
            int f = 0;
            for (char m : modifiers.toCharArray()) {
                switch (m) {
                    case 'i' -> f |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
                    case 'm' -> f |= Pattern.MULTILINE;
                    case 's' -> f |= Pattern.DOTALL;
                    case 'x' -> f |= Pattern.COMMENTS;
                    default -> {
                        return null;
                    }
                }
            }
            try {
                p = Pattern.compile(pattern, f);
            } catch (PatternSyntaxException ex) {
                return null;
            }
            patterns.put(key, p);
        }
        return p;
    }

    static JsonElement parseJson(String s) {
        try {
            return JsonParser.parseString(s);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
