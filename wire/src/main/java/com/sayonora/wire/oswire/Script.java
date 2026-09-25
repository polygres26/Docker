package com.sayonora.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.List;

/**
 * A small interpreter for the subset of Painless that update / update_by_query scripts use in practice:
 * {@code ctx._source.a.b = expr}, {@code += -= *= /=}, {@code ++ --}, {@code ctx._source.remove('f')},
 * {@code ctx._source.list.add(x)} / {@code .remove(x)}, {@code ctx.op = 'noop'|'delete'}, {@code if / else},
 * {@code def x = ...}, {@code params.x}, string/number/boolean literals, arithmetic/comparison/logic/ternary,
 * {@code .size() .length() .contains() .toLowerCase() .toUpperCase()}. Anything else fails with {@code script_exception}
 * (the full Painless language is not implemented).
 */
final class Script {

    final String source;
    final JsonObject params;

    Script(JsonObject spec) {
        if (spec.has("id") || spec.has("stored")) {
            throw new OpenSearchException("illegal_argument_exception", "stored scripts are not supported by Warp's OpenSearch frontend");
        }
        String lang = spec.has("lang") ? spec.get("lang").getAsString() : "painless";
        if (!lang.equals("painless") && !lang.equals("expression")) {
            throw new OpenSearchException("illegal_argument_exception", "script_lang not supported [" + lang + "]");
        }
        JsonElement src = spec.has("source") ? spec.get("source") : spec.get("inline");
        if (src == null) {
            throw new OpenSearchException("parsing_exception", "Unknown key for a START_OBJECT in [script].");
        }
        this.source = src.getAsString();
        this.params = spec.has("params") ? spec.getAsJsonObject("params") : new JsonObject();
    }

    /** Runs the script; {@code ctx} holds {@code _source} and {@code op}. */
    void run(JsonObject ctx) {
        try {
            Interp in = new Interp(source, ctx, params);
            in.program();
        } catch (OpenSearchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw scriptError("runtime error: " + e.getMessage());
        }
    }

    private OpenSearchException scriptError(String msg) {
        OpenSearchException e = new OpenSearchException("script_exception", msg);
        JsonArray stack = new JsonArray();
        stack.add(source);
        e.extra.add("script_stack", stack);
        e.extra.addProperty("script", source);
        e.extra.addProperty("lang", "painless");
        return e;
    }

    private static final class Interp {
        final String s;
        int p;
        final JsonObject ctx;
        final JsonObject params;
        final java.util.Map<String, Object> locals = new java.util.HashMap<>();
        boolean skip; // skipping (false branch of an if)

        Interp(String s, JsonObject ctx, JsonObject params) {
            this.s = s;
            this.ctx = ctx;
            this.params = params;
        }

        RuntimeException fail(String m) {
            OpenSearchException e = new OpenSearchException("script_exception", "compile error: " + m + " at [" + p + "] in [" + s + "]");
            JsonArray stack = new JsonArray();
            stack.add(s);
            e.extra.add("script_stack", stack);
            e.extra.addProperty("script", s);
            e.extra.addProperty("lang", "painless");
            return e;
        }

        void ws() {
            while (p < s.length() && Character.isWhitespace(s.charAt(p))) {
                p++;
            }
        }

        boolean peek(String t) {
            ws();
            return s.startsWith(t, p);
        }

        boolean eat(String t) {
            ws();
            if (s.startsWith(t, p)) {
                p += t.length();
                return true;
            }
            return false;
        }

        boolean eatWord(String w) {
            ws();
            if (s.startsWith(w, p) && (p + w.length() >= s.length() || !Character.isJavaIdentifierPart(s.charAt(p + w.length())))) {
                p += w.length();
                return true;
            }
            return false;
        }

        void program() {
            ws();
            while (p < s.length()) {
                statement();
                ws();
            }
        }

        void statement() {
            ws();
            if (eat("{")) {
                while (!peek("}")) {
                    if (p >= s.length()) {
                        throw fail("unterminated block");
                    }
                    statement();
                }
                eat("}");
                return;
            }
            if (eat(";")) {
                return;
            }
            if (eatWord("if")) {
                eat("(");
                Object c = expr();
                eat(")");
                boolean save = skip;
                skip = save || !truthy(c);
                statement();
                skip = save;
                if (eatWord("else")) {
                    skip = save || truthy(c);
                    statement();
                    skip = save;
                }
                return;
            }
            if (eatWord("def") || eatWord("int") || eatWord("long") || eatWord("double") || eatWord("String") || eatWord("boolean")) {
                String name = ident();
                Object v = null;
                if (eat("=")) {
                    v = expr();
                }
                if (!skip) {
                    locals.put(name, v);
                }
                eat(";");
                return;
            }
            if (eatWord("return")) {
                expr();
                eat(";");
                p = s.length();
                return;
            }
            // assignment / call statement
            int start = p;
            Ref ref = lvalue();
            ws();
            if (ref != null && (peek("=") && !peek("==") || peek("+=") || peek("-=") || peek("*=") || peek("/=") || peek("%="))) {
                String op = s.startsWith("=", p) ? "=" : s.substring(p, p + 2);
                p += op.length();
                Object v = expr();
                if (!skip) {
                    Object cur = op.equals("=") ? null : ref.get();
                    ref.set(switch (op) {
                        case "=" -> v;
                        case "+=" -> add(cur, v);
                        case "-=" -> arith('-', cur, v);
                        case "*=" -> arith('*', cur, v);
                        case "/=" -> arith('/', cur, v);
                        default -> arith('%', cur, v);
                    });
                }
                eat(";");
                return;
            }
            if (ref != null && (peek("++") || peek("--"))) {
                boolean inc = s.startsWith("++", p);
                p += 2;
                if (!skip) {
                    ref.set(arith(inc ? '+' : '-', ref.get(), 1L));
                }
                eat(";");
                return;
            }
            // expression statement (method call)
            p = start;
            expr();
            eat(";");
        }

        String ident() {
            ws();
            int st = p;
            while (p < s.length() && Character.isJavaIdentifierPart(s.charAt(p))) {
                p++;
            }
            if (st == p) {
                throw fail("expected identifier");
            }
            return s.substring(st, p);
        }

        /** An assignable location: ctx._source path, ctx.op, or a local. */
        interface Ref {
            Object get();

            void set(Object v);
        }

        Ref lvalue() {
            ws();
            int save = p;
            if (!Character.isJavaIdentifierStart(peekChar())) {
                return null;
            }
            String root = ident();
            if (root.equals("ctx")) {
                if (eat(".")) {
                    String f = ident();
                    if (f.equals("op") && !peek(".")) {
                        return new Ref() {
                            public Object get() {
                                return ctx.has("op") ? ctx.get("op").getAsString() : "index";
                            }

                            public void set(Object v) {
                                ctx.addProperty("op", String.valueOf(v));
                            }
                        };
                    }
                    if (f.equals("_source")) {
                        List<String> path = new ArrayList<>();
                        while (true) {
                            int before = p;
                            if (eat(".")) {
                                ws();
                                int idStart = p;
                                String id = ident();
                                ws();
                                if (peek("(")) {
                                    p = before;
                                    break;
                                }
                                path.add(id);
                            } else if (peek("[")) {
                                eat("[");
                                Object k = expr();
                                eat("]");
                                path.add(String.valueOf(k));
                            } else {
                                break;
                            }
                        }
                        return sourceRef(path);
                    }
                }
                p = save;
                return null;
            }
            if (locals.containsKey(root) && !peek(".")) {
                return new Ref() {
                    public Object get() {
                        return locals.get(root);
                    }

                    public void set(Object v) {
                        locals.put(root, v);
                    }
                };
            }
            p = save;
            return null;
        }

        char peekChar() {
            return p < s.length() ? s.charAt(p) : '\0';
        }

        Ref sourceRef(List<String> path) {
            JsonObject src = ctx.getAsJsonObject("_source");
            return new Ref() {
                public Object get() {
                    JsonElement cur = src;
                    for (String k : path) {
                        if (cur == null || !cur.isJsonObject()) {
                            return null;
                        }
                        cur = cur.getAsJsonObject().get(k);
                    }
                    return fromJson(cur);
                }

                public void set(Object v) {
                    JsonObject cur = src;
                    for (int i = 0; i < path.size() - 1; i++) {
                        JsonElement n = cur.get(path.get(i));
                        if (n == null || !n.isJsonObject()) {
                            n = new JsonObject();
                            cur.add(path.get(i), n);
                        }
                        cur = n.getAsJsonObject();
                    }
                    if (path.isEmpty()) {
                        throw fail("cannot assign to ctx._source");
                    }
                    cur.add(path.get(path.size() - 1), toJson(v));
                }
            };
        }

        // ---- expressions (precedence climbing) ----

        Object expr() {
            Object c = or();
            if (eat("?")) {
                Object a = expr();
                eat(":");
                Object b = expr();
                return truthy(c) ? a : b;
            }
            return c;
        }

        Object or() {
            Object l = and();
            while (peek("||")) {
                p += 2;
                Object r = and();
                l = truthy(l) || truthy(r);
            }
            return l;
        }

        Object and() {
            Object l = eq();
            while (peek("&&")) {
                p += 2;
                Object r = eq();
                l = truthy(l) && truthy(r);
            }
            return l;
        }

        Object eq() {
            Object l = cmp();
            while (true) {
                if (peek("==")) {
                    p += 2;
                    l = equal(l, cmp());
                } else if (peek("!=")) {
                    p += 2;
                    l = !equal(l, cmp());
                } else {
                    return l;
                }
            }
        }

        Object cmp() {
            Object l = additive();
            while (true) {
                if (peek("<=")) {
                    p += 2;
                    l = compare(l, additive()) <= 0;
                } else if (peek(">=")) {
                    p += 2;
                    l = compare(l, additive()) >= 0;
                } else if (peek("<") && !peek("<<")) {
                    p++;
                    l = compare(l, additive()) < 0;
                } else if (peek(">") && !peek(">>")) {
                    p++;
                    l = compare(l, additive()) > 0;
                } else {
                    return l;
                }
            }
        }

        Object additive() {
            Object l = mul();
            while (true) {
                ws();
                if (peek("+") && !peek("++") && !peek("+=")) {
                    p++;
                    l = add(l, mul());
                } else if (peek("-") && !peek("--") && !peek("-=")) {
                    p++;
                    l = arith('-', l, mul());
                } else {
                    return l;
                }
            }
        }

        Object mul() {
            Object l = unary();
            while (true) {
                ws();
                if (peek("*") && !peek("*=")) {
                    p++;
                    l = arith('*', l, unary());
                } else if (peek("/") && !peek("/=")) {
                    p++;
                    l = arith('/', l, unary());
                } else if (peek("%") && !peek("%=")) {
                    p++;
                    l = arith('%', l, unary());
                } else {
                    return l;
                }
            }
        }

        Object unary() {
            ws();
            if (eat("!")) {
                return !truthy(unary());
            }
            if (peek("-") && !peek("--")) {
                p++;
                return arith('-', 0L, unary());
            }
            return postfix(primary());
        }

        Object primary() {
            ws();
            if (eat("(")) {
                Object v = expr();
                eat(")");
                return v;
            }
            char c = peekChar();
            if (c == '\'' || c == '"') {
                p++;
                StringBuilder sb = new StringBuilder();
                while (p < s.length() && s.charAt(p) != c) {
                    if (s.charAt(p) == '\\' && p + 1 < s.length()) {
                        p++;
                    }
                    sb.append(s.charAt(p++));
                }
                p++;
                return sb.toString();
            }
            if (Character.isDigit(c)) {
                int st = p;
                while (p < s.length() && (Character.isDigit(s.charAt(p)) || s.charAt(p) == '.')) {
                    p++;
                }
                String n = s.substring(st, p);
                if (p < s.length() && (s.charAt(p) == 'L' || s.charAt(p) == 'l' || s.charAt(p) == 'f' || s.charAt(p) == 'd')) {
                    p++;
                }
                return n.contains(".") ? (Object) Double.parseDouble(n) : (Object) Long.parseLong(n);
            }
            String id = ident();
            switch (id) {
                case "true":
                    return true;
                case "false":
                    return false;
                case "null":
                    return null;
                case "params": {
                    if (eat(".")) {
                        return fromJson(params.get(ident()));
                    }
                    if (eat("[")) {
                        Object k = expr();
                        eat("]");
                        return fromJson(params.get(String.valueOf(k)));
                    }
                    return params;
                }
                case "ctx": {
                    eat(".");
                    String f = ident();
                    if (f.equals("op")) {
                        return ctx.has("op") ? ctx.get("op").getAsString() : "index";
                    }
                    if (f.equals("_source")) {
                        return ctx.getAsJsonObject("_source");
                    }
                    return fromJson(ctx.get(f));
                }
                case "Math": {
                    eat(".");
                    String fn = ident();
                    eat("(");
                    List<Object> args = new ArrayList<>();
                    while (!peek(")")) {
                        args.add(expr());
                        eat(",");
                    }
                    eat(")");
                    double a = args.isEmpty() ? 0 : toD(args.get(0));
                    return switch (fn) {
                        case "max" -> Math.max(a, toD(args.get(1)));
                        case "min" -> Math.min(a, toD(args.get(1)));
                        case "abs" -> Math.abs(a);
                        case "floor" -> Math.floor(a);
                        case "ceil" -> Math.ceil(a);
                        case "round" -> (Object) Math.round(a);
                        case "sqrt" -> Math.sqrt(a);
                        case "pow" -> Math.pow(a, toD(args.get(1)));
                        default -> throw fail("unknown Math." + fn);
                    };
                }
                default:
                    if (locals.containsKey(id)) {
                        return locals.get(id);
                    }
                    throw fail("cannot resolve symbol [" + id + "]");
            }
        }

        Object postfix(Object base) {
            while (true) {
                ws();
                if (peek(".") && !Character.isDigit(p + 1 < s.length() ? s.charAt(p + 1) : ' ')) {
                    p++;
                    String m = ident();
                    if (peek("(")) {
                        eat("(");
                        List<Object> args = new ArrayList<>();
                        while (!peek(")")) {
                            args.add(expr());
                            eat(",");
                        }
                        eat(")");
                        base = call(base, m, args);
                    } else {
                        base = field(base, m);
                    }
                } else if (peek("[")) {
                    eat("[");
                    Object k = expr();
                    eat("]");
                    base = base instanceof JsonArray a ? fromJson(a.get(((Number) k).intValue())) : field(base, String.valueOf(k));
                } else {
                    return base;
                }
            }
        }

        Object field(Object base, String name) {
            if (base instanceof JsonObject o) {
                return fromJson(o.get(name));
            }
            if (base instanceof JsonArray a && name.equals("length")) {
                return (long) a.size();
            }
            if (base == null) {
                throw fail("null pointer accessing [" + name + "]");
            }
            throw fail("no field [" + name + "]");
        }

        Object call(Object base, String m, List<Object> args) {
            if (base instanceof JsonObject o && o.has("_source") && false) {
                return null;
            }
            switch (m) {
                case "remove" -> {
                    if (base instanceof JsonObject o) {
                        if (!skip) {
                            o.remove(String.valueOf(args.get(0)));
                        }
                        return null;
                    }
                    if (base instanceof JsonArray a) {
                        if (!skip) {
                            JsonElement target = toJson(args.get(0));
                            if (args.get(0) instanceof Long idx && a.size() > idx && !(a.get(0).isJsonPrimitive() && a.get(0).getAsJsonPrimitive().isString())) {
                                a.remove(idx.intValue());
                            } else {
                                a.remove(target);
                            }
                        }
                        return true;
                    }
                }
                case "add" -> {
                    if (base instanceof JsonArray a) {
                        if (!skip) {
                            a.add(toJson(args.get(0)));
                        }
                        return true;
                    }
                }
                case "put" -> {
                    if (base instanceof JsonObject o) {
                        if (!skip) {
                            o.add(String.valueOf(args.get(0)), toJson(args.get(1)));
                        }
                        return null;
                    }
                }
                case "containsKey" -> {
                    if (base instanceof JsonObject o) {
                        return o.has(String.valueOf(args.get(0)));
                    }
                }
                case "size", "length" -> {
                    if (base instanceof JsonArray a) {
                        return (long) a.size();
                    }
                    if (base instanceof JsonObject o) {
                        return (long) o.size();
                    }
                    if (base instanceof String str) {
                        return (long) str.length();
                    }
                }
                case "isEmpty" -> {
                    if (base instanceof JsonArray a) {
                        return a.isEmpty();
                    }
                    if (base instanceof JsonObject o) {
                        return o.size() == 0;
                    }
                    if (base instanceof String str) {
                        return str.isEmpty();
                    }
                }
                case "contains" -> {
                    if (base instanceof JsonArray a) {
                        return a.contains(toJson(args.get(0)));
                    }
                    if (base instanceof String str) {
                        return str.contains(String.valueOf(args.get(0)));
                    }
                }
                case "toLowerCase" -> {
                    return String.valueOf(base).toLowerCase(java.util.Locale.ROOT);
                }
                case "toUpperCase" -> {
                    return String.valueOf(base).toUpperCase(java.util.Locale.ROOT);
                }
                case "startsWith" -> {
                    return String.valueOf(base).startsWith(String.valueOf(args.get(0)));
                }
                case "endsWith" -> {
                    return String.valueOf(base).endsWith(String.valueOf(args.get(0)));
                }
                case "equals" -> {
                    return equal(base, args.get(0));
                }
                default -> {
                }
            }
            throw fail("dynamic method [" + m + "] not supported");
        }

        // ---- value helpers ----

        static Object fromJson(JsonElement e) {
            if (e == null || e.isJsonNull()) {
                return null;
            }
            if (e.isJsonPrimitive()) {
                JsonPrimitive pr = e.getAsJsonPrimitive();
                if (pr.isBoolean()) {
                    return pr.getAsBoolean();
                }
                if (pr.isNumber()) {
                    java.math.BigDecimal bd = pr.getAsBigDecimal();
                    return bd.scale() <= 0 && !pr.getAsString().contains(".") ? (Object) bd.longValue() : (Object) bd.doubleValue();
                }
                return pr.getAsString();
            }
            return e;
        }

        static JsonElement toJson(Object o) {
            if (o == null) {
                return JsonNull.INSTANCE;
            }
            if (o instanceof JsonElement e) {
                return e.deepCopy();
            }
            if (o instanceof Boolean b) {
                return new JsonPrimitive(b);
            }
            if (o instanceof Number n) {
                if (n instanceof Double d && d == Math.rint(d) && Math.abs(d) < 1e15 && false) {
                    return new JsonPrimitive(d.longValue());
                }
                return new JsonPrimitive(n);
            }
            return new JsonPrimitive(String.valueOf(o));
        }

        static boolean truthy(Object o) {
            if (o instanceof Boolean b) {
                return b;
            }
            return o != null;
        }

        static double toD(Object o) {
            if (o instanceof Number n) {
                return n.doubleValue();
            }
            throw new IllegalArgumentException("not a number: " + o);
        }

        static boolean equal(Object a, Object b) {
            if (a == null || b == null) {
                return a == b;
            }
            if (a instanceof Number x && b instanceof Number y) {
                return x.doubleValue() == y.doubleValue();
            }
            return a.equals(b) || String.valueOf(a).equals(String.valueOf(b)) && !(a instanceof JsonElement);
        }

        static int compare(Object a, Object b) {
            if (a instanceof Number x && b instanceof Number y) {
                return Double.compare(x.doubleValue(), y.doubleValue());
            }
            return String.valueOf(a).compareTo(String.valueOf(b));
        }

        static Object add(Object a, Object b) {
            if (a instanceof String || b instanceof String) {
                return String.valueOf(a) + String.valueOf(b);
            }
            if (a == null && b instanceof Number) {
                throw new IllegalArgumentException("cannot add to null");
            }
            return arith('+', a, b);
        }

        static Object arith(char op, Object a, Object b) {
            if (!(a instanceof Number x) || !(b instanceof Number y)) {
                throw new IllegalArgumentException("cannot apply [" + op + "] to [" + a + "] and [" + b + "]");
            }
            boolean ints = !(x instanceof Double) && !(y instanceof Double) && !(x instanceof Float) && !(y instanceof Float);
            if (ints) {
                long l = x.longValue(), r = y.longValue();
                return switch (op) {
                    case '+' -> (Object) (l + r);
                    case '-' -> (Object) (l - r);
                    case '*' -> (Object) (l * r);
                    case '/' -> {
                        if (r == 0) {
                            throw new ArithmeticException("/ by zero");
                        }
                        yield (Object) (l / r);
                    }
                    default -> (Object) (l % r);
                };
            }
            double l = x.doubleValue(), r = y.doubleValue();
            return switch (op) {
                case '+' -> (Object) (l + r);
                case '-' -> (Object) (l - r);
                case '*' -> (Object) (l * r);
                case '/' -> (Object) (l / r);
                default -> (Object) (l % r);
            };
        }
    }
}
