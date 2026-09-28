package com.sayonora.warp.s3wire;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import org.w3c.dom.Element;

/*
 * Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors
 * (S3SelectEvaluator's WHERE grammar and CSV/JSON row semantics, and the event-stream framing of S3SelectService); this
 * implementation is otherwise written from scratch on Gson, with a full expression parser (arithmetic, functions, aggregates).
 */

/**
 * SelectObjectContent: a SQL subset over CSV / JSON (lines or document) objects, evaluated in memory.
 * Supported: {@code SELECT * | expr [AS alias], ... FROM S3Object[*][.path] [alias] [WHERE expr] [LIMIT n]}, expressions with
 * {@code AND OR NOT}, comparisons, {@code BETWEEN IN LIKE (ESCAPE) IS [NOT] NULL}, {@code + - * / %}, {@code ||},
 * {@code CAST LOWER UPPER TRIM SUBSTRING CHAR_LENGTH COALESCE NULLIF} and aggregates {@code COUNT SUM AVG MIN MAX}.
 * CSV cells are strings that compare numerically when the other operand is a number (S3 Select coerces
 * implicitly for comparisons). Not supported: Parquet, BZIP2, nested-loop joins, timestamp functions.
 */
final class S3Select {
    private static final Gson GSON = new Gson();

    private S3Select() {
    }

    // ---- request ------------------------------------------------------------------------------

    record Request(String expression, String inType, String header, String inFieldDelim, String inRecordDelim,
            String inQuote, String inQuoteEscape, String jsonType, String compression, String outType,
            String outFieldDelim, String outRecordDelim, String outQuote, String quoteFields, String comments) {
    }

    static Request parseRequest(byte[] xml) {
        Element r = S3Xml.parse(xml);
        if (!"SelectObjectContentRequest".equals(S3Xml.name(r))) {
            throw S3Cfg.malformed();
        }
        String expr = S3Xml.text(r, "Expression");
        if (expr == null || expr.isBlank()) {
            throw new S3WireException(400, "InvalidRequest", "Expression is required");
        }
        String type = S3Xml.text(r, "ExpressionType");
        if (type != null && !type.trim().equalsIgnoreCase("SQL")) {
            throw new S3WireException(400, "InvalidExpressionType", "The ExpressionType is invalid. Only SQL is supported.");
        }
        List<Element> in = S3Xml.children(r, "InputSerialization");
        List<Element> out = S3Xml.children(r, "OutputSerialization");
        if (in.isEmpty() || out.isEmpty()) {
            throw new S3WireException(400, "MissingRequiredParameter", "InputSerialization and OutputSerialization are required");
        }
        String inType = null;
        Element ie = null;
        for (String t : new String[] {"CSV", "JSON", "Parquet"}) {
            List<Element> c = S3Xml.children(in.get(0), t);
            if (!c.isEmpty()) {
                inType = t;
                ie = c.get(0);
            }
        }
        if (inType == null) {
            throw new S3WireException(400, "InvalidRequest", "InputSerialization must contain CSV, JSON or Parquet");
        }
        String outType = null;
        Element oe = null;
        for (String t : new String[] {"CSV", "JSON"}) {
            List<Element> c = S3Xml.children(out.get(0), t);
            if (!c.isEmpty()) {
                outType = t;
                oe = c.get(0);
            }
        }
        if (outType == null) {
            throw new S3WireException(400, "InvalidRequest", "OutputSerialization must contain CSV or JSON");
        }
        return new Request(expr.trim(), inType, or(S3Xml.text(ie, "FileHeaderInfo"), "NONE"),
                or(S3Xml.text(ie, "FieldDelimiter"), ","), or(S3Xml.text(ie, "RecordDelimiter"), "\n"),
                or(S3Xml.text(ie, "QuoteCharacter"), "\""), or(S3Xml.text(ie, "QuoteEscapeCharacter"), "\""),
                or(S3Xml.text(ie, "Type"), "DOCUMENT"), or(S3Xml.text(in.get(0), "CompressionType"), "NONE"), outType,
                or(S3Xml.text(oe, "FieldDelimiter"), ","), or(S3Xml.text(oe, "RecordDelimiter"), "\n"),
                or(S3Xml.text(oe, "QuoteCharacter"), "\""), or(S3Xml.text(oe, "QuoteFields"), "ASNEEDED"),
                S3Xml.text(ie, "Comments"));
    }

    private static String or(String v, String d) {
        return v == null || v.isEmpty() ? d : v;
    }

    // ---- lexer ----------------------------------------------------------------------------------

    private enum T { ID, QID, STR, NUM, OP, EOF }

    private record Tok(T t, String v) {
        boolean kw(String k) {
            return t == T.ID && v.equalsIgnoreCase(k);
        }

        boolean op(String o) {
            return t == T.OP && v.equals(o);
        }
    }

    private static S3WireException syntax(String m) {
        return new S3WireException(400, "ParseUnexpectedToken", m);
    }

    private static List<Tok> lex(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '\'') {
                StringBuilder sb = new StringBuilder();
                i++;
                boolean closed = false;
                while (i < s.length()) {
                    char ch = s.charAt(i);
                    if (ch == '\'') {
                        if (i + 1 < s.length() && s.charAt(i + 1) == '\'') {
                            sb.append('\'');
                            i += 2;
                            continue;
                        }
                        i++;
                        closed = true;
                        break;
                    }
                    sb.append(ch);
                    i++;
                }
                if (!closed) {
                    throw syntax("Unterminated string literal");
                }
                out.add(new Tok(T.STR, sb.toString()));
            } else if (c == '"') {
                int j = s.indexOf('"', i + 1);
                if (j < 0) {
                    throw syntax("Unterminated quoted identifier");
                }
                out.add(new Tok(T.QID, s.substring(i + 1, j)));
                i = j + 1;
            } else if (Character.isDigit(c) || c == '.' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1))) {
                int j = i;
                while (j < s.length() && (Character.isDigit(s.charAt(j)) || s.charAt(j) == '.')) {
                    j++;
                }
                if (j < s.length() && (s.charAt(j) == 'e' || s.charAt(j) == 'E')) {
                    j++;
                    if (j < s.length() && (s.charAt(j) == '+' || s.charAt(j) == '-')) {
                        j++;
                    }
                    while (j < s.length() && Character.isDigit(s.charAt(j))) {
                        j++;
                    }
                }
                out.add(new Tok(T.NUM, s.substring(i, j)));
                i = j;
            } else if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < s.length() && (Character.isLetterOrDigit(s.charAt(j)) || s.charAt(j) == '_')) {
                    j++;
                }
                out.add(new Tok(T.ID, s.substring(i, j)));
                i = j;
            } else {
                String two = i + 1 < s.length() ? s.substring(i, i + 2) : "";
                if (List.of("<=", ">=", "<>", "!=", "||").contains(two)) {
                    out.add(new Tok(T.OP, two));
                    i += 2;
                } else if ("=<>+-*/%(),.[]".indexOf(c) >= 0) {
                    out.add(new Tok(T.OP, String.valueOf(c)));
                    i++;
                } else {
                    throw syntax("Unexpected character '" + c + "'");
                }
            }
        }
        out.add(new Tok(T.EOF, ""));
        return out;
    }

    // ---- AST ------------------------------------------------------------------------------------

    private interface Expr {
        Object eval(Row r);
    }

    /** A record: cells by position, names (CSV header or JSON member names) and the raw JSON document if any. */
    private static final class Row {
        final List<String> names;
        final List<Object> cells;
        final JsonElement doc;

        Row(List<String> names, List<Object> cells, JsonElement doc) {
            this.names = names;
            this.cells = cells;
            this.doc = doc;
        }
    }

    private record Proj(Expr expr, String name, boolean star, Agg agg) {
    }

    private static final class Agg {
        final String fn;
        final Expr arg; // null = COUNT(*)
        long count;
        double sum;
        boolean any;
        Object min;
        Object max;

        Agg(String fn, Expr arg) {
            this.fn = fn;
            this.arg = arg;
        }

        void add(Row r) {
            if (arg == null) {
                count++;
                return;
            }
            Object v = arg.eval(r);
            if (v == null) {
                return;
            }
            count++;
            if (fn.equals("SUM") || fn.equals("AVG")) {
                sum += num(v);
            }
            if (!any || cmp(v, min) < 0) {
                min = v;
            }
            if (!any || cmp(v, max) > 0) {
                max = v;
            }
            any = true;
        }

        Object result() {
            return switch (fn) {
                case "COUNT" -> count;
                case "SUM" -> any ? norm(sum) : null;
                case "AVG" -> any ? (Object) (sum / count) : null;
                case "MIN" -> min;
                default -> max;
            };
        }
    }

    private static Object norm(double d) {
        return d == Math.rint(d) && Math.abs(d) < 9e15 ? (Object) (long) d : (Object) d;
    }

    // ---- parser ---------------------------------------------------------------------------------

    private static final class Query {
        List<Proj> projections = new ArrayList<>();
        List<String> fromPath = new ArrayList<>();
        String alias;
        Expr where;
        long limit = -1;
        boolean aggregate;
    }

    private static final class Parser {
        final List<Tok> toks;
        int p;
        final Query q = new Query();

        Parser(List<Tok> toks) {
            this.toks = toks;
        }

        Tok peek() {
            return toks.get(p);
        }

        Tok next() {
            return toks.get(p++);
        }

        boolean acceptKw(String k) {
            if (peek().kw(k)) {
                p++;
                return true;
            }
            return false;
        }

        boolean acceptOp(String o) {
            if (peek().op(o)) {
                p++;
                return true;
            }
            return false;
        }

        void expectKw(String k) {
            if (!acceptKw(k)) {
                throw syntax("Expected " + k + " but found '" + peek().v() + "'");
            }
        }

        void expectOp(String o) {
            if (!acceptOp(o)) {
                throw syntax("Expected '" + o + "' but found '" + peek().v() + "'");
            }
        }

        Query parse() {
            expectKw("SELECT");
            int idx = 0;
            do {
                idx++;
                if (acceptOp("*")) {
                    q.projections.add(new Proj(null, null, true, null));
                    continue;
                }
                Expr e = expr();
                Agg agg = lastAgg;
                lastAgg = null;
                String alias = null;
                if (acceptKw("AS")) {
                    alias = ident();
                } else if (peek().t() == T.ID && !peek().kw("FROM")) {
                    alias = ident();
                }
                if (alias == null) {
                    alias = lastName != null ? lastName : "_" + idx;
                }
                lastName = null;
                if (agg != null) {
                    q.aggregate = true;
                }
                q.projections.add(new Proj(e, alias, false, agg));
            } while (acceptOp(","));
            expectKw("FROM");
            Tok t = next();
            if (!t.kw("S3Object")) {
                throw syntax("Only S3Object can be queried, found '" + t.v() + "'");
            }
            while (true) {
                if (peek().op("[") && toks.get(p + 1).op("*") && toks.get(p + 2).op("]")) {
                    p += 3;
                    q.fromPath.add("[*]");
                } else if (peek().op(".")) {
                    p++;
                    q.fromPath.add(ident());
                } else {
                    break;
                }
            }
            if (acceptKw("AS")) {
                q.alias = ident();
            } else if (peek().t() == T.ID && !peek().kw("WHERE") && !peek().kw("LIMIT")) {
                q.alias = ident();
            }
            if (acceptKw("WHERE")) {
                q.where = expr();
            }
            if (acceptKw("LIMIT")) {
                Tok n = next();
                try {
                    q.limit = Long.parseLong(n.v());
                } catch (NumberFormatException e) {
                    throw syntax("LIMIT expects an integer");
                }
            }
            if (peek().t() != T.EOF) {
                throw syntax("Unexpected token '" + peek().v() + "'");
            }
            if (q.aggregate && q.projections.stream().anyMatch(x -> x.agg() == null)) {
                throw new S3WireException(400, "ParseUnsupportedSyntax",
                        "Cannot mix aggregate and non-aggregate expressions in the select list");
            }
            return q;
        }

        String ident() {
            Tok t = next();
            if (t.t() != T.ID && t.t() != T.QID) {
                throw syntax("Expected an identifier but found '" + t.v() + "'");
            }
            return t.v();
        }

        Agg lastAgg;
        String lastName;

        Expr expr() {
            return or();
        }

        Expr or() {
            Expr l = and();
            while (acceptKw("OR")) {
                Expr a = l;
                Expr b = and();
                l = r -> {
                    Object x = a.eval(r);
                    Object y = b.eval(r);
                    if (Boolean.TRUE.equals(x) || Boolean.TRUE.equals(y)) {
                        return true;
                    }
                    return x == null || y == null ? null : (Object) false;
                };
            }
            return l;
        }

        Expr and() {
            Expr l = not();
            while (acceptKw("AND")) {
                Expr a = l;
                Expr b = not();
                l = r -> {
                    Object x = a.eval(r);
                    Object y = b.eval(r);
                    if (Boolean.FALSE.equals(x) || Boolean.FALSE.equals(y)) {
                        return false;
                    }
                    return x == null || y == null ? null : (Object) true;
                };
            }
            return l;
        }

        Expr not() {
            if (acceptKw("NOT")) {
                Expr e = not();
                return r -> {
                    Object v = e.eval(r);
                    return v == null ? null : (Object) !truthy(v);
                };
            }
            return predicate();
        }

        Expr predicate() {
            Expr l = concat();
            Tok t = peek();
            if (t.t() == T.OP && List.of("=", "<>", "!=", "<", "<=", ">", ">=").contains(t.v())) {
                p++;
                Expr rr = concat();
                String op = t.v();
                Expr a = l;
                return r -> {
                    Object x = a.eval(r);
                    Object y = rr.eval(r);
                    if (x == null || y == null) {
                        return null;
                    }
                    int c = cmp(x, y);
                    return switch (op) {
                        case "=" -> c == 0;
                        case "<>", "!=" -> c != 0;
                        case "<" -> c < 0;
                        case "<=" -> c <= 0;
                        case ">" -> c > 0;
                        default -> c >= 0;
                    };
                };
            }
            boolean negate = false;
            int save = p;
            if (t.kw("NOT")) {
                negate = true;
                p++;
                t = peek();
            }
            if (t.kw("BETWEEN")) {
                p++;
                Expr lo = concat();
                expectKw("AND");
                Expr hi = concat();
                Expr a = l;
                boolean neg = negate;
                return r -> {
                    Object x = a.eval(r);
                    Object l1 = lo.eval(r);
                    Object h1 = hi.eval(r);
                    if (x == null || l1 == null || h1 == null) {
                        return null;
                    }
                    boolean in = cmp(x, l1) >= 0 && cmp(x, h1) <= 0;
                    return neg != in;
                };
            }
            if (t.kw("IN")) {
                p++;
                expectOp("(");
                List<Expr> vals = new ArrayList<>();
                do {
                    vals.add(concat());
                } while (acceptOp(","));
                expectOp(")");
                Expr a = l;
                boolean neg = negate;
                return r -> {
                    Object x = a.eval(r);
                    if (x == null) {
                        return null;
                    }
                    boolean in = false;
                    for (Expr v : vals) {
                        Object y = v.eval(r);
                        if (y != null && cmp(x, y) == 0) {
                            in = true;
                        }
                    }
                    return neg != in;
                };
            }
            if (t.kw("LIKE")) {
                p++;
                Expr pat = concat();
                Expr esc = null;
                if (acceptKw("ESCAPE")) {
                    esc = concat();
                }
                Expr a = l;
                Expr e2 = esc;
                boolean neg = negate;
                return r -> {
                    Object x = a.eval(r);
                    Object y = pat.eval(r);
                    if (x == null || y == null) {
                        return null;
                    }
                    Object ev = e2 == null ? null : e2.eval(r);
                    return neg != like(str(x), str(y), ev == null ? null : str(ev));
                };
            }
            if (negate) {
                p = save;
            }
            if (t.kw("IS")) {
                p++;
                boolean neg = acceptKw("NOT");
                if (acceptKw("NULL") || acceptKw("MISSING")) {
                    Expr a = l;
                    return r -> (a.eval(r) == null) != neg;
                }
                throw syntax("Expected NULL after IS");
            }
            return l;
        }

        Expr concat() {
            Expr l = additive();
            while (acceptOp("||")) {
                Expr a = l;
                Expr b = additive();
                l = r -> {
                    Object x = a.eval(r);
                    Object y = b.eval(r);
                    return x == null || y == null ? null : (Object) (str(x) + str(y));
                };
            }
            return l;
        }

        Expr additive() {
            Expr l = mult();
            while (peek().op("+") || peek().op("-")) {
                boolean plus = next().v().equals("+");
                Expr a = l;
                Expr b = mult();
                l = r -> {
                    Object x = a.eval(r);
                    Object y = b.eval(r);
                    return x == null || y == null ? null : norm(plus ? num(x) + num(y) : num(x) - num(y));
                };
            }
            return l;
        }

        Expr mult() {
            Expr l = unary();
            while (peek().op("*") || peek().op("/") || peek().op("%")) {
                String op = next().v();
                Expr a = l;
                Expr b = unary();
                l = r -> {
                    Object x = a.eval(r);
                    Object y = b.eval(r);
                    if (x == null || y == null) {
                        return null;
                    }
                    double d = switch (op) {
                        case "*" -> num(x) * num(y);
                        case "/" -> num(x) / num(y);
                        default -> num(x) % num(y);
                    };
                    return norm(d);
                };
            }
            return l;
        }

        Expr unary() {
            if (acceptOp("-")) {
                Expr e = unary();
                return r -> {
                    Object v = e.eval(r);
                    return v == null ? null : norm(-num(v));
                };
            }
            if (acceptOp("+")) {
                return unary();
            }
            return primary();
        }

        Expr primary() {
            Tok t = next();
            switch (t.t()) {
                case NUM -> {
                    Object v = t.v().contains(".") || t.v().toLowerCase(Locale.ROOT).contains("e")
                            ? (Object) Double.parseDouble(t.v()) : (Object) Long.parseLong(t.v());
                    return r -> v;
                }
                case STR -> {
                    String s = t.v();
                    return r -> s;
                }
                case OP -> {
                    if (t.v().equals("(")) {
                        Expr e = expr();
                        expectOp(")");
                        return e;
                    }
                    throw syntax("Unexpected '" + t.v() + "'");
                }
                case ID, QID -> {
                    if (t.t() == T.ID) {
                        String u = t.v().toUpperCase(Locale.ROOT);
                        if (u.equals("NULL") || u.equals("MISSING")) {
                            return r -> null;
                        }
                        if (u.equals("TRUE")) {
                            return r -> true;
                        }
                        if (u.equals("FALSE")) {
                            return r -> false;
                        }
                        if (peek().op("(")) {
                            return function(u);
                        }
                    }
                    return path(t);
                }
                default -> throw syntax("Unexpected end of expression");
            }
        }

        /** {@code [alias.]name[.sub][[i]]} or {@code _N}. */
        Expr path(Tok first) {
            List<Object> steps = new ArrayList<>();
            String head = first.v();
            steps.add(head); // an alias / S3Object head is dropped at evaluation time (the alias is parsed after the select list)
            while (true) {
                if (peek().op(".")) {
                    p++;
                    if (acceptOp("*")) {
                        steps.add("*");
                    } else {
                        steps.add(ident());
                    }
                } else if (peek().op("[") && toks.get(p + 1).t() == T.NUM) {
                    p++;
                    steps.add(Integer.parseInt(next().v()));
                    expectOp("]");
                } else if (peek().op("[") && toks.get(p + 1).op("*")) {
                    p += 2;
                    expectOp("]");
                } else {
                    break;
                }
            }
            if (steps.get(steps.size() - 1) instanceof String s2 && !"*".equals(s2)) {
                lastName = s2;
            }
            final Query query = q;
            return r -> {
                List<Object> st = steps;
                if (steps.get(0) instanceof String h && (query.alias != null && h.equalsIgnoreCase(query.alias)
                        || h.equalsIgnoreCase("S3Object"))) {
                    st = steps.subList(1, steps.size());
                }
                return st.isEmpty() ? (r.doc != null ? r.doc : null) : resolve(r, st);
            };
        }

        Expr function(String fn) {
            expectOp("(");
            switch (fn) {
                case "COUNT", "SUM", "AVG", "MIN", "MAX" -> {
                    Expr arg = null;
                    if (acceptOp("*")) {
                        if (!fn.equals("COUNT")) {
                            throw syntax(fn + "(*) is not valid");
                        }
                    } else {
                        arg = expr();
                    }
                    expectOp(")");
                    Agg agg = new Agg(fn, arg);
                    lastAgg = agg;
                    return r -> agg.result();
                }
                case "CAST" -> {
                    Expr e = expr();
                    expectKw("AS");
                    String type = ident().toUpperCase(Locale.ROOT);
                    if (peek().op("(")) { // DECIMAL(10,2)
                        while (!acceptOp(")")) {
                            p++;
                        }
                    }
                    expectOp(")");
                    return r -> cast(e.eval(r), type);
                }
                case "TRIM" -> {
                    String mode = "BOTH";
                    if (peek().kw("LEADING") || peek().kw("TRAILING") || peek().kw("BOTH")) {
                        mode = next().v().toUpperCase(Locale.ROOT);
                    }
                    Expr chars = null;
                    Expr e = expr();
                    if (acceptKw("FROM")) {
                        chars = e;
                        e = expr();
                    }
                    expectOp(")");
                    Expr src = e;
                    Expr ch = chars;
                    String m = mode;
                    return r -> {
                        Object v = src.eval(r);
                        if (v == null) {
                            return null;
                        }
                        String s = str(v);
                        String set = ch == null ? " " : str(ch.eval(r));
                        int a = 0;
                        int b = s.length();
                        if (!m.equals("TRAILING")) {
                            while (a < b && set.indexOf(s.charAt(a)) >= 0) {
                                a++;
                            }
                        }
                        if (!m.equals("LEADING")) {
                            while (b > a && set.indexOf(s.charAt(b - 1)) >= 0) {
                                b--;
                            }
                        }
                        return s.substring(a, b);
                    };
                }
                case "SUBSTRING" -> {
                    Expr s = expr();
                    Expr from;
                    Expr len = null;
                    if (acceptKw("FROM")) {
                        from = expr();
                        if (acceptKw("FOR")) {
                            len = expr();
                        }
                    } else {
                        expectOp(",");
                        from = expr();
                        if (acceptOp(",")) {
                            len = expr();
                        }
                    }
                    expectOp(")");
                    Expr l2 = len;
                    return r -> {
                        Object v = s.eval(r);
                        Object f = from.eval(r);
                        if (v == null || f == null) {
                            return null;
                        }
                        String str = str(v);
                        int start = Math.max(1, (int) num(f)) - 1;
                        if (start >= str.length()) {
                            return "";
                        }
                        int end = l2 == null ? str.length() : Math.min(str.length(), start + (int) num(l2.eval(r)));
                        return str.substring(start, Math.max(start, end));
                    };
                }
                default -> {
                    List<Expr> args = new ArrayList<>();
                    if (!peek().op(")")) {
                        do {
                            args.add(expr());
                        } while (acceptOp(","));
                    }
                    expectOp(")");
                    return switch (fn) {
                        case "LOWER" -> r -> mapStr(args.get(0).eval(r), String::toLowerCase);
                        case "UPPER" -> r -> mapStr(args.get(0).eval(r), String::toUpperCase);
                        case "CHAR_LENGTH", "CHARACTER_LENGTH" -> r -> {
                            Object v = args.get(0).eval(r);
                            return v == null ? null : (Object) (long) str(v).codePointCount(0, str(v).length());
                        };
                        case "COALESCE" -> r -> {
                            for (Expr a : args) {
                                Object v = a.eval(r);
                                if (v != null) {
                                    return v;
                                }
                            }
                            return null;
                        };
                        case "NULLIF" -> r -> {
                            Object a = args.get(0).eval(r);
                            Object b = args.get(1).eval(r);
                            return a != null && b != null && cmp(a, b) == 0 ? null : a;
                        };
                        default -> throw new S3WireException(400, "UnsupportedFunction", "Unsupported function: " + fn);
                    };
                }
            }
        }
    }

    private static Object mapStr(Object v, java.util.function.UnaryOperator<String> f) {
        return v == null ? null : f.apply(str(v));
    }

    // ---- value semantics -------------------------------------------------------------------------

    private static boolean truthy(Object v) {
        return v instanceof Boolean b ? b : v != null && !"false".equalsIgnoreCase(str(v));
    }

    static String str(Object v) {
        if (v instanceof Double d) {
            return d == Math.rint(d) && Math.abs(d) < 1e15 ? String.valueOf(d.longValue()) : String.valueOf(d);
        }
        if (v instanceof JsonElement je) {
            return je.isJsonPrimitive() ? je.getAsString() : je.toString();
        }
        return String.valueOf(v);
    }

    private static double num(Object v) {
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(str(v).trim());
        } catch (NumberFormatException e) {
            throw new S3WireException(400, "CastFailed", "Attempt to convert from one data type to another using CAST failed: '"
                    + str(v) + "' is not a number");
        }
    }

    private static boolean isNumeric(Object v) {
        if (v instanceof Number) {
            return true;
        }
        if (v instanceof String s) {
            try {
                Double.parseDouble(s.trim());
                return !s.isBlank();
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    static int cmp(Object a, Object b) {
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return Boolean.compare(x, y);
        }
        boolean an = a instanceof Number;
        boolean bn = b instanceof Number;
        if ((an || bn) && isNumeric(a) && isNumeric(b)) {
            return Double.compare(num(a), num(b));
        }
        return str(a).compareTo(str(b));
    }

    private static Object cast(Object v, String type) {
        if (v == null) {
            return null;
        }
        return switch (type) {
            case "INT", "INTEGER", "BIGINT", "SMALLINT" -> (long) num(v);
            case "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC" -> num(v);
            case "STRING", "VARCHAR", "CHAR" -> str(v);
            case "BOOL", "BOOLEAN" -> truthy(v);
            case "TIMESTAMP" -> str(v);
            default -> throw new S3WireException(400, "CastFailed", "Unsupported CAST target type " + type);
        };
    }

    private static boolean like(String value, String pattern, String escape) {
        StringBuilder rx = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escape != null && !escape.isEmpty() && c == escape.charAt(0) && i + 1 < pattern.length()) {
                rx.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
            } else if (c == '%') {
                rx.append(".*");
            } else if (c == '_') {
                rx.append('.');
            } else {
                rx.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(rx.append('$').toString(), Pattern.DOTALL).matcher(value).matches();
    }

    private static Object fromJson(JsonElement e) {
        if (e == null || e.isJsonNull()) {
            return null;
        }
        if (e.isJsonPrimitive()) {
            var p = e.getAsJsonPrimitive();
            if (p.isBoolean()) {
                return p.getAsBoolean();
            }
            if (p.isNumber()) {
                double d = p.getAsDouble();
                return p.getAsString().matches("-?\\d+") ? (Object) p.getAsLong() : (Object) d;
            }
            return p.getAsString();
        }
        return e;
    }

    private static Object resolve(Row r, List<Object> steps) {
        if (r.doc == null) { // CSV: a bare column name or _N
            if (steps.size() != 1 || !(steps.get(0) instanceof String col)) {
                return null;
            }
            if (col.matches("_\\d+")) {
                int idx = Integer.parseInt(col.substring(1)) - 1;
                if (idx < 0 || idx >= r.cells.size()) {
                    throw new S3WireException(400, "InvalidColumnIndex", "Column index " + col + " is out of range");
                }
                return r.cells.get(idx);
            }
            for (int i = 0; i < r.names.size(); i++) {
                if (r.names.get(i).equals(col)) {
                    return i < r.cells.size() ? r.cells.get(i) : null;
                }
            }
            for (int i = 0; i < r.names.size(); i++) {
                if (r.names.get(i).trim().equalsIgnoreCase(col)) {
                    return i < r.cells.size() ? r.cells.get(i) : null;
                }
            }
            if (r.names.isEmpty()) {
                throw new S3WireException(400, "MissingHeaders", "Some headers in the query are missing from the file. "
                        + "Please check the file and try again.");
            }
            return null;
        }
        JsonElement cur = r.doc;
        for (Object s : steps) {
            if (cur == null) {
                return null;
            }
            if (s instanceof Integer i) {
                cur = cur.isJsonArray() && i < cur.getAsJsonArray().size() ? cur.getAsJsonArray().get(i) : null;
            } else if (cur.isJsonObject()) {
                JsonObject o = cur.getAsJsonObject();
                JsonElement next = o.get((String) s);
                if (next == null) {
                    for (Map.Entry<String, JsonElement> e : o.entrySet()) {
                        if (e.getKey().equalsIgnoreCase((String) s)) {
                            next = e.getValue();
                        }
                    }
                }
                cur = next;
            } else {
                cur = null;
            }
        }
        return fromJson(cur);
    }

    // ---- input parsing --------------------------------------------------------------------------

    static List<List<String>> parseCsv(String text, Request q) {
        char fd = q.inFieldDelim().charAt(0);
        char qc = q.inQuote().charAt(0);
        char ec = q.inQuoteEscape().charAt(0);
        String rd = q.inRecordDelim();
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        boolean wasQuoted = false;
        int i = 0;
        int n = text.length();
        while (i < n) {
            char c = text.charAt(i);
            if (inQuotes) {
                if (c == ec && ec != qc && i + 1 < n && text.charAt(i + 1) == qc) {
                    cur.append(qc);
                    i += 2;
                } else if (c == qc) {
                    if (i + 1 < n && text.charAt(i + 1) == qc && ec == qc) {
                        cur.append(qc);
                        i += 2;
                    } else {
                        inQuotes = false;
                        i++;
                    }
                } else {
                    cur.append(c);
                    i++;
                }
            } else if (c == qc && cur.length() == 0 && !wasQuoted) {
                inQuotes = true;
                wasQuoted = true;
                i++;
            } else if (c == fd) {
                row.add(cur.toString());
                cur.setLength(0);
                wasQuoted = false;
                i++;
            } else if (text.startsWith(rd, i) || c == '\r' && text.startsWith("\n", i + 1) && rd.equals("\n")) {
                if (c == '\r') {
                    i++;
                }
                i += c == '\r' ? 1 : rd.length();
                row.add(cur.toString());
                cur.setLength(0);
                wasQuoted = false;
                if (!(row.size() == 1 && row.get(0).isEmpty())) {
                    rows.add(row);
                }
                row = new ArrayList<>();
            } else {
                cur.append(c);
                i++;
            }
        }
        if (cur.length() > 0 || !row.isEmpty()) {
            row.add(cur.toString());
            rows.add(row);
        }
        if (q.comments() != null) {
            rows.removeIf(r -> !r.isEmpty() && r.get(0).startsWith(q.comments()));
        }
        return rows;
    }

    private static List<Row> loadRows(String text, Request q, Query query) {
        List<Row> rows = new ArrayList<>();
        if (q.inType().equals("CSV")) {
            List<List<String>> recs = parseCsv(text, q);
            List<String> names = new ArrayList<>();
            int start = 0;
            if (q.header().equalsIgnoreCase("USE") && !recs.isEmpty()) {
                names = recs.get(0);
                start = 1;
            } else if (q.header().equalsIgnoreCase("IGNORE") && !recs.isEmpty()) {
                start = 1;
            }
            for (int i = start; i < recs.size(); i++) {
                List<Object> cells = new ArrayList<>(recs.get(i));
                rows.add(new Row(names, cells, null));
            }
            return rows;
        }
        List<JsonElement> docs = new ArrayList<>();
        String t = text.trim();
        try {
            if (q.jsonType().equalsIgnoreCase("LINES")) {
                for (String line : text.split("\\r?\\n")) {
                    if (!line.isBlank()) {
                        docs.add(JsonParser.parseString(line));
                    }
                }
            } else {
                var reader = new com.google.gson.stream.JsonReader(new java.io.StringReader(t));
                reader.setLenient(true);
                while (reader.peek() != com.google.gson.stream.JsonToken.END_DOCUMENT) {
                    docs.add(JsonParser.parseReader(reader));
                }
            }
        } catch (RuntimeException | IOException e) {
            throw new S3WireException(400, "InvalidJsonType", "The JSON input is not valid: " + e.getMessage());
        }
        for (JsonElement d : docs) {
            for (JsonElement e : applyPath(d, query.fromPath)) {
                rows.add(new Row(List.of(), List.of(), e));
            }
        }
        return rows;
    }

    private static List<JsonElement> applyPath(JsonElement d, List<String> path) {
        List<JsonElement> cur = List.of(d);
        for (String step : path) {
            List<JsonElement> nxt = new ArrayList<>();
            for (JsonElement e : cur) {
                if (step.equals("[*]")) {
                    if (e.isJsonArray()) {
                        e.getAsJsonArray().forEach(nxt::add);
                    } else {
                        nxt.add(e);
                    }
                } else if (e.isJsonObject() && e.getAsJsonObject().has(step)) {
                    nxt.add(e.getAsJsonObject().get(step));
                }
            }
            cur = nxt;
        }
        // a top-level JSON array document behaves as a stream of records for S3Object[*]
        if (path.isEmpty() && d.isJsonArray()) {
            List<JsonElement> l = new ArrayList<>();
            d.getAsJsonArray().forEach(l::add);
            return l;
        }
        return cur;
    }

    // ---- execution --------------------------------------------------------------------------------

    /** @return the record payload (already serialized per the output serialization) */
    static byte[] run(byte[] data, Request q) {
        String text = new String(data, StandardCharsets.UTF_8);
        Query query = new Parser(lex(q.expression())).parse();
        List<Row> rows = loadRows(text, q, query);
        StringBuilder out = new StringBuilder();
        long emitted = 0;
        if (query.aggregate) {
            for (Row r : rows) {
                if (query.where == null || Boolean.TRUE.equals(query.where.eval(r))) {
                    for (Proj p : query.projections) {
                        p.agg().add(r);
                    }
                }
            }
            List<String> names = new ArrayList<>();
            List<Object> vals = new ArrayList<>();
            for (Proj p : query.projections) {
                names.add(p.name());
                vals.add(p.agg().result());
            }
            emit(out, names, vals, q);
            return out.toString().getBytes(StandardCharsets.UTF_8);
        }
        for (Row r : rows) {
            if (query.limit >= 0 && emitted >= query.limit) {
                break;
            }
            if (query.where != null && !Boolean.TRUE.equals(query.where.eval(r))) {
                continue;
            }
            List<String> names = new ArrayList<>();
            List<Object> vals = new ArrayList<>();
            for (Proj p : query.projections) {
                if (p.star()) {
                    if (r.doc != null) {
                        if (r.doc.isJsonObject()) {
                            for (Map.Entry<String, JsonElement> e : r.doc.getAsJsonObject().entrySet()) {
                                names.add(e.getKey());
                                vals.add(fromJson(e.getValue()));
                            }
                        } else {
                            names.add("_1");
                            vals.add(fromJson(r.doc));
                        }
                    } else {
                        for (int i = 0; i < r.cells.size(); i++) {
                            names.add(i < r.names.size() ? r.names.get(i) : "_" + (i + 1));
                            vals.add(r.cells.get(i));
                        }
                    }
                } else {
                    names.add(p.name());
                    vals.add(p.expr().eval(r));
                }
            }
            emit(out, names, vals, q);
            emitted++;
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void emit(StringBuilder out, List<String> names, List<Object> vals, Request q) {
        if (q.outType().equals("JSON")) {
            JsonObject o = new JsonObject();
            for (int i = 0; i < names.size(); i++) {
                Object v = vals.get(i);
                if (v == null) {
                    continue; // MISSING values are omitted
                }
                String n = names.get(i);
                if (v instanceof Long l) {
                    o.addProperty(n, l);
                } else if (v instanceof Double d) {
                    o.addProperty(n, d);
                } else if (v instanceof Boolean b) {
                    o.addProperty(n, b);
                } else if (v instanceof JsonElement je) {
                    o.add(n, je);
                } else {
                    o.addProperty(n, String.valueOf(v));
                }
            }
            out.append(GSON.toJson(o)).append(q.outRecordDelim());
            return;
        }
        for (int i = 0; i < vals.size(); i++) {
            if (i > 0) {
                out.append(q.outFieldDelim());
            }
            Object v = vals.get(i);
            String s = v == null ? "" : str(v);
            char qc = q.outQuote().charAt(0);
            boolean needs = q.quoteFields().equalsIgnoreCase("ALWAYS") || s.contains(q.outFieldDelim()) || s.indexOf(qc) >= 0
                    || s.contains("\n") || s.contains("\r") || s.contains(q.outRecordDelim());
            if (needs && v != null) {
                out.append(qc).append(s.replace(String.valueOf(qc), "" + qc + qc)).append(qc);
            } else {
                out.append(s);
            }
        }
        out.append(q.outRecordDelim());
    }

    // ---- event stream --------------------------------------------------------------------------------

    private static byte[] message(Map<String, String> headers, byte[] payload) {
        try {
            ByteArrayOutputStream h = new ByteArrayOutputStream();
            for (Map.Entry<String, String> e : headers.entrySet()) {
                byte[] name = e.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] val = e.getValue().getBytes(StandardCharsets.UTF_8);
                h.write(name.length);
                h.write(name);
                h.write(7);
                h.write(val.length >> 8);
                h.write(val.length & 0xff);
                h.write(val);
            }
            byte[] hb = h.toByteArray();
            int total = 12 + hb.length + payload.length + 4;
            ByteArrayOutputStream b = new ByteArrayOutputStream(total);
            java.io.DataOutputStream d = new java.io.DataOutputStream(b);
            d.writeInt(total);
            d.writeInt(hb.length);
            CRC32 c = new CRC32();
            c.update(b.toByteArray());
            d.writeInt((int) c.getValue());
            d.write(hb);
            d.write(payload);
            CRC32 c2 = new CRC32();
            c2.update(b.toByteArray());
            d.writeInt((int) c2.getValue());
            return b.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Records (in ≤64 KiB events), Stats and End events as one event-stream body. */
    static byte[] eventStream(byte[] records, long scanned, boolean progress) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            int off = 0;
            do {
                int n = Math.min(65536, records.length - off);
                Map<String, String> h = new LinkedHashMap<>();
                h.put(":event-type", "Records");
                h.put(":content-type", "application/octet-stream");
                h.put(":message-type", "event");
                if (n > 0) {
                    out.write(message(h, java.util.Arrays.copyOfRange(records, off, off + n)));
                }
                off += n;
            } while (off < records.length);
            Map<String, String> h = new LinkedHashMap<>();
            h.put(":event-type", "Stats");
            h.put(":content-type", "text/xml");
            h.put(":message-type", "event");
            String stats = "<Stats xmlns=\"\"><BytesScanned>" + scanned + "</BytesScanned><BytesProcessed>" + scanned
                    + "</BytesProcessed><BytesReturned>" + records.length + "</BytesReturned></Stats>";
            out.write(message(h, stats.getBytes(StandardCharsets.UTF_8)));
            Map<String, String> e = new LinkedHashMap<>();
            e.put(":event-type", "End");
            e.put(":message-type", "event");
            out.write(message(e, new byte[0]));
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return out.toByteArray();
    }
}
