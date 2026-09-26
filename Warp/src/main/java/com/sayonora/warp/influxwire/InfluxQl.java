package com.sayonora.warp.influxwire;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * InfluxQL lexer, AST and recursive-descent parser -- modelled on the real InfluxDB 1.x
 * {@code influxql} package so accepted syntax and the {@code error parsing query: found X, expected
 * Y at line L, char C} messages match. Execution lives in {@link InfluxEngine}.
 */
final class InfluxQl {

    private InfluxQl() {
    }

    // ------------------------------------------------------------------ AST

    abstract static class Expr {
    }

    static final class VarRef extends Expr {
        final String name;
        final String type; // "", tag, field, float, integer, string, boolean, unsigned

        VarRef(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    static final class Wildcard extends Expr {
        final String type; // "", "tag", "field"

        Wildcard(String type) {
            this.type = type;
        }
    }

    static final class RegexLit extends Expr {
        final Pattern pattern;
        final String source;

        RegexLit(Pattern pattern, String source) {
            this.pattern = pattern;
            this.source = source;
        }
    }

    static final class StrLit extends Expr {
        final String v;

        StrLit(String v) {
            this.v = v;
        }
    }

    static final class IntLit extends Expr {
        final long v;

        IntLit(long v) {
            this.v = v;
        }
    }

    static final class NumLit extends Expr {
        final double v;

        NumLit(double v) {
            this.v = v;
        }
    }

    static final class BoolLit extends Expr {
        final boolean v;

        BoolLit(boolean v) {
            this.v = v;
        }
    }

    static final class DurLit extends Expr {
        final long nanos;

        DurLit(long nanos) {
            this.nanos = nanos;
        }
    }

    static final class TimeLit extends Expr {
        final long nanos;

        TimeLit(long nanos) {
            this.nanos = nanos;
        }
    }

    static final class Call extends Expr {
        final String name; // lower-cased
        final List<Expr> args;

        Call(String name, List<Expr> args) {
            this.name = name;
            this.args = args;
        }
    }

    static final class Bin extends Expr {
        final String op; // + - * / % & | ^ = != < <= > >= =~ !~ AND OR
        final Expr l;
        final Expr r;

        Bin(String op, Expr l, Expr r) {
            this.op = op;
            this.l = l;
            this.r = r;
        }
    }

    static final class Paren extends Expr {
        final Expr e;

        Paren(Expr e) {
            this.e = e;
        }
    }

    static final class Field {
        final Expr expr;
        final String alias;

        Field(Expr expr, String alias) {
            this.expr = expr;
            this.alias = alias;
        }
    }

    static final class Source {
        String db;
        String rp;
        String name;
        Pattern regex;
        Select sub;
    }

    static final class Dim {
        static final int TIME = 0;
        static final int TAG = 1;
        static final int WILDCARD = 2;
        static final int REGEX = 3;
        int kind;
        String name;
        Pattern regex;
        long interval;
        long offset;
        boolean offsetNow;
    }

    enum FillKind { NULL, NONE, PREVIOUS, LINEAR, NUMBER }

    abstract static class Stmt {
        boolean readOnlyOk() {
            return false;
        }
    }

    static final class Select extends Stmt {
        List<Field> fields = new ArrayList<>();
        Source into;
        boolean intoMeasurementPlaceholder;
        List<Source> sources = new ArrayList<>();
        Expr cond;
        List<Dim> dims = new ArrayList<>();
        FillKind fill = FillKind.NULL;
        Object fillValue;
        boolean fillExplicit;
        boolean desc;
        int limit;
        int offset;
        int slimit;
        int soffset;
        boolean hasLimit;
        boolean hasSlimit;
        String tz;

        @Override
        boolean readOnlyOk() {
            return into == null;
        }
    }

    /** A statement whose parse succeeded syntactically but is semantically invalid (reported per statement). */
    static final class ErrStmt extends Stmt {
        final String msg;

        ErrStmt(String msg) {
            this.msg = msg;
        }

        @Override
        boolean readOnlyOk() {
            return true;
        }
    }

    /** SHOW ... and other metadata statements. */
    static final class Show extends Stmt {
        String kind; // DATABASES MEASUREMENTS TAG_KEYS TAG_VALUES FIELD_KEYS SERIES RETENTION_POLICIES ...
        String on;
        List<Source> from = new ArrayList<>();
        Expr cond;
        String withOp; // = != =~ !~ IN
        List<String> withKeys = new ArrayList<>();
        Pattern withRegex;
        String withMeasOp;
        String withMeas;
        Pattern withMeasRegex;
        int limit;
        int offset;
        int slimit;
        int soffset;
        boolean cardinality;
        boolean exact;
        String forUser;

        @Override
        boolean readOnlyOk() {
            return true;
        }
    }

    static final class Ddl extends Stmt {
        String kind; // CREATE_DATABASE DROP_DATABASE CREATE_RP ALTER_RP DROP_RP DROP_MEASUREMENT DROP_SERIES DELETE USER KILL
        String name;
        String on;
        String rpName;
        long duration = -1;
        boolean hasDuration;
        long shardDuration;
        int replication = 1;
        boolean isDefault;
        List<Source> from = new ArrayList<>();
        Expr cond;
        String text;
    }

    static final class Explain extends Stmt {
        Select select;
        boolean analyze;

        @Override
        boolean readOnlyOk() {
            return true;
        }
    }

    // ------------------------------------------------------------------ errors

    static final class ParseError extends RuntimeException {
        ParseError(String msg) {
            super(msg);
        }
    }

    // ------------------------------------------------------------------ lexer

    enum T { EOF, IDENT, QIDENT, STRING, INT, NUM, DUR, REGEX, OP, ILLEGAL, PARAM }

    static final java.util.Set<String> KEYWORDS = java.util.Set.of("ALL", "ALTER", "ANALYZE", "ANY", "AS", "ASC", "BEGIN", "BY",
            "CREATE", "CONTINUOUS", "DATABASE", "DATABASES", "DEFAULT", "DELETE", "DESC", "DESTINATIONS", "DIAGNOSTICS", "DISTINCT",
            "DROP", "DURATION", "END", "EVERY", "EXPLAIN", "FIELD", "FOR", "FROM", "GRANT", "GRANTS", "GROUP", "GROUPS", "IN", "INF",
            "INSERT", "INTO", "KEY", "KEYS", "KILL", "LIMIT", "MEASUREMENT", "MEASUREMENTS", "NAME", "OFFSET", "ON", "ORDER",
            "PASSWORD", "POLICY", "POLICIES", "PRIVILEGES", "QUERIES", "QUERY", "READ", "REPLICATION", "RESAMPLE", "RETENTION",
            "REVOKE", "SELECT", "SERIES", "SET", "SHOW", "SHARD", "SHARDS", "SLIMIT", "SOFFSET", "STATS", "SUBSCRIPTION",
            "SUBSCRIPTIONS", "TAG", "TO", "USER", "USERS", "VALUES", "WHERE", "WITH", "WRITE", "AND", "OR");

    static boolean isReserved(String ident) {
        return KEYWORDS.contains(ident.toUpperCase(Locale.ROOT));
    }

    record Tok(T t, String text, int off, int line, int col, String raw) {
        String show() {
            return switch (t) {
                case EOF -> "EOF";
                case STRING -> text;
                case QIDENT -> text;
                case IDENT -> isReserved(text) ? text.toUpperCase(Locale.ROOT) : text;
                case ILLEGAL -> !text.isEmpty() && (text.charAt(0) == '"' || text.charAt(0) == '\'') ? text.substring(1) : text;
                default -> raw == null ? text : raw;
            };
        }
    }

    private static final class Lexer {
        final String s;
        int i;
        int line = 1;
        int lineStart;

        Lexer(String s) {
            this.s = s;
        }

        Tok next(boolean regexOk) {
            skipWs();
            int start = i;
            int col = i - lineStart + 1;
            if (i >= s.length()) {
                return new Tok(T.EOF, "", i, line, col, null);
            }
            if (s.charAt(i) == '\'' || s.charAt(i) == '"') {
                col = i - lineStart; // influxql reports string tokens at the quote's 0-based column
            }
            char c = s.charAt(i);
            if (regexOk && c == '/') {
                col = i - lineStart; // influxql reports regex literals at the 0-based column
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < s.length()) {
                    char d = s.charAt(i);
                    if (d == '\\' && i + 1 < s.length() && s.charAt(i + 1) == '/') {
                        sb.append('/');
                        i += 2;
                        continue;
                    }
                    if (d == '/') {
                        i++;
                        return new Tok(T.REGEX, sb.toString(), start, line, col, s.substring(start, i));
                    }
                    if (d == '\\' && i + 1 < s.length()) {
                        sb.append(d).append(s.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    sb.append(d);
                    i++;
                }
                return new Tok(T.ILLEGAL, "/" + sb, start, line, col, null);
            }
            if (c == '\'' || c == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                boolean closed = false;
                while (i < s.length()) {
                    char d = s.charAt(i);
                    if (d == '\\' && i + 1 < s.length()) {
                        char n = s.charAt(i + 1);
                        if (n == 'n') {
                            sb.append('\n');
                            i += 2;
                            continue;
                        }
                        if (n == '\\' || n == c) {
                            sb.append(n);
                            i += 2;
                            continue;
                        }
                        // unknown escape: real influxql errors ("invalid escape"); keep literally
                        sb.append(d).append(n);
                        i += 2;
                        continue;
                    }
                    if (d == c) {
                        closed = true;
                        i++;
                        break;
                    }
                    if (d == '\n') {
                        line++;
                        lineStart = i + 1;
                    }
                    sb.append(d);
                    i++;
                }
                if (!closed) {
                    return new Tok(T.ILLEGAL, String.valueOf(c) + sb, start, line, col, null);
                }
                return new Tok(c == '\'' ? T.STRING : T.QIDENT, sb.toString(), start, line, col, s.substring(start, i));
            }
            if (c == '$') {
                int j = i + 1;
                if (j < s.length() && s.charAt(j) == '"') {
                    Tok q = next(false);
                    return new Tok(T.PARAM, q.text, start, line, col, null);
                }
                while (j < s.length() && (Character.isLetterOrDigit(s.charAt(j)) || s.charAt(j) == '_')) {
                    j++;
                }
                String name = s.substring(i + 1, j);
                i = j;
                return new Tok(T.PARAM, name, start, line, col, null);
            }
            if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < s.length() && (Character.isLetterOrDigit(s.charAt(j)) || s.charAt(j) == '_')) {
                    j++;
                }
                String w = s.substring(i, j);
                i = j;
                return new Tok(T.IDENT, w, start, line, col, w);
            }
            if (Character.isDigit(c) || (c == '.' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1)))) {
                int j = i;
                boolean isFloat = false;
                while (j < s.length() && Character.isDigit(s.charAt(j))) {
                    j++;
                }
                if (j < s.length() && s.charAt(j) == '.') {
                    isFloat = true;
                    j++;
                    while (j < s.length() && Character.isDigit(s.charAt(j))) {
                        j++;
                    }
                }
                String num = s.substring(i, j);
                // digits directly followed by letters form a duration literal (validated where it is used)
                int k = j;
                if (!isFloat && k < s.length() && (Character.isLetter(s.charAt(k)) || s.charAt(k) == '\u00b5')) {
                    int e = k;
                    while (e < s.length() && (Character.isLetter(s.charAt(e)) || s.charAt(e) == '\u00b5')) {
                        e++;
                    }
                    i = e;
                    return new Tok(T.DUR, s.substring(start, e), start, line, col, s.substring(start, e));
                }
                i = j;
                return new Tok(isFloat ? T.NUM : T.INT, num, start, line, col, num);
            }
            // operators
            String[] ops2 = {"!=", "<>", "<=", ">=", "=~", "!~", "::"};
            for (String o : ops2) {
                if (s.startsWith(o, i)) {
                    i += 2;
                    return new Tok(T.OP, o, start, line, col, o);
                }
            }
            i++;
            if ("+-*/%&|^=<>(),;.:".indexOf(c) >= 0) {
                return new Tok(T.OP, String.valueOf(c), start, line, col, String.valueOf(c));
            }
            return new Tok(T.ILLEGAL, String.valueOf(c), start, line, col, String.valueOf(c));
        }

        void skipWs() {
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '\n') {
                    line++;
                    i++;
                    lineStart = i;
                } else if (Character.isWhitespace(c)) {
                    i++;
                } else if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-') {
                    while (i < s.length() && s.charAt(i) != '\n') {
                        i++;
                    }
                } else if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
                    int end = s.indexOf("*/", i + 2);
                    i = end < 0 ? s.length() : end + 2;
                } else {
                    break;
                }
            }
        }
    }

    // ------------------------------------------------------------------ parser

    static List<Stmt> parse(String q, Map<String, Object> params) {
        return new Parser(q, params).parseAll();
    }

    static long parseDurationNanos(String lit) {
        int k = 0;
        while (k < lit.length() && Character.isDigit(lit.charAt(k))) {
            k++;
        }
        long n;
        try {
            n = Long.parseLong(lit.substring(0, k));
        } catch (NumberFormatException e) {
            throw new ParseError("error parsing query: invalid duration");
        }
        String u = lit.substring(k);
        return switch (u) {
            case "ns" -> n;
            case "u", "us", "µ", "µs" -> n * 1_000L;
            case "ms" -> n * 1_000_000L;
            case "s" -> n * 1_000_000_000L;
            case "m" -> n * 60_000_000_000L;
            case "h" -> n * 3_600_000_000_000L;
            case "d" -> n * 86_400_000_000_000L;
            case "w" -> n * 604_800_000_000_000L;
            default -> throw new ParseError("error parsing query: invalid duration");
        };
    }

    private static final class Parser {
        final String src;
        final Lexer lx;
        final Map<String, Object> params;
        final List<Tok> buf = new ArrayList<>();
        Tok cur;
        Tok prevTok;
        Tok lastTok;
        int lastEnd;
        int lastEndLineStart;
        boolean pushed;

        Parser(String src, Map<String, Object> params) {
            this.src = src;
            this.lx = new Lexer(src);
            this.params = params == null ? Map.of() : params;
        }

        // token handling: a one-token buffer with unscan
        Tok scan() {
            if (pushed) {
                pushed = false;
                return cur;
            }
            prevTok = cur;
            cur = lx.next(false);
            noteTok();
            return cur;
        }

        Tok scanRegex() {
            if (pushed) {
                pushed = false;
                if (cur.t == T.OP && cur.text.equals("/")) {
                    lx.i = cur.off;
                    cur = lx.next(true);
                }
                return cur;
            }
            prevTok = cur;
            cur = lx.next(true);
            noteTok();
            return cur;
        }

        Object[] mark() {
            return new Object[] {lx.i, lx.line, lx.lineStart, cur, prevTok, lastTok, lastEnd, lastEndLineStart, pushed};
        }

        void reset(Object[] m) {
            lx.i = (Integer) m[0];
            lx.line = (Integer) m[1];
            lx.lineStart = (Integer) m[2];
            cur = (Tok) m[3];
            prevTok = (Tok) m[4];
            lastTok = (Tok) m[5];
            lastEnd = (Integer) m[6];
            lastEndLineStart = (Integer) m[7];
            pushed = (Boolean) m[8];
        }

        /** True when the next tokens are DISTINCT not followed by '(' (SELECT DISTINCT field / count(DISTINCT field)). */
        boolean atBareDistinct() {
            Object[] m = mark();
            Tok a = scan();
            boolean r = false;
            if (isKw(a, "DISTINCT")) {
                Tok b = scan();
                r = !(b.t == T.OP && b.text.equals("("));
            }
            reset(m);
            return r;
        }

        void noteTok() {
            if (cur.t != T.EOF) {
                lastTok = cur;
                lastEnd = lx.i;
                lastEndLineStart = lx.lineStart;
            }
        }

        void unscan() {
            pushed = true;
        }

        Tok peek() {
            Tok t = scan();
            unscan();
            return t;
        }

        boolean isKw(Tok t, String kw) {
            return t.t == T.IDENT && t.text.equalsIgnoreCase(kw);
        }

        boolean acceptKw(String kw) {
            Tok t = scan();
            if (isKw(t, kw)) {
                return true;
            }
            unscan();
            return false;
        }

        boolean acceptOp(String op) {
            Tok t = scan();
            if (t.t == T.OP && t.text.equals(op)) {
                return true;
            }
            unscan();
            return false;
        }

        ParseError err(Tok found, String... expected) {
            int col = found.col;
            if (found.t == T.EOF && lastTok != null) {
                boolean word = lastTok.t == T.IDENT || (lastTok.t == T.OP && (lastTok.text.equals(",") || lastTok.text.equals("(")));
                col = (lastEnd - lastEndLineStart) + (word ? 2 : 1);
                if (lastTok.line != found.line && found.line > 1) {
                    col = found.col;
                }
            }
            String shown = found.show();
            if (found.t == T.PARAM && params.get(found.text) instanceof String ps) {
                shown = ps;
            }
            return new ParseError("error parsing query: found " + shown + ", expected " + String.join(", ", expected)
                    + " at line " + found.line + ", char " + col);
        }

        void expectKw(String kw) {
            Tok t = scan();
            if (!isKw(t, kw)) {
                throw err(t, kw);
            }
        }

        void expectOp(String op) {
            Tok t = scan();
            if (!(t.t == T.OP && t.text.equals(op))) {
                throw err(t, op);
            }
        }

        List<Stmt> parseAll() {
            List<Stmt> out = new ArrayList<>();
            while (true) {
                Tok t = scan();
                if (t.t == T.EOF) {
                    break;
                }
                if (t.t == T.OP && t.text.equals(";")) {
                    continue;
                }
                unscan();
                try {
                    out.add(parseStatement());
                } catch (ParseError pe) {
                    if (pe.getMessage().startsWith("\u0000")) {
                        drainStatement();
                        out.add(new ErrStmt(pe.getMessage().substring(1)));
                    } else {
                        throw pe;
                    }
                }
                Tok e = scan();
                if (e.t == T.EOF) {
                    break;
                }
                if (!(e.t == T.OP && e.text.equals(";"))) {
                    throw err(e, ";");
                }
            }
            return out;
        }

        Stmt parseStatement() {
            Tok t = scan();
            if (t.t == T.IDENT) {
                switch (t.text.toUpperCase(Locale.ROOT)) {
                    case "SELECT":
                        return parseSelect();
                    case "SHOW":
                        return parseShow();
                    case "CREATE":
                        return parseCreate();
                    case "DROP":
                        return parseDrop();
                    case "DELETE":
                        return parseDelete();
                    case "ALTER":
                        return parseAlter();
                    case "EXPLAIN": {
                        Explain ex = new Explain();
                        ex.analyze = acceptKw("ANALYZE");
                        expectKw("SELECT");
                        ex.select = parseSelect();
                        return ex;
                    }
                    case "KILL": {
                        Ddl d = new Ddl();
                        d.kind = "KILL";
                        expectKw("QUERY");
                        Tok n = scan();
                        d.name = n.text;
                        return d;
                    }
                    case "GRANT":
                    case "REVOKE":
                    case "SET": {
                        Ddl d = new Ddl();
                        d.kind = "USER";
                        d.text = t.text.toUpperCase(Locale.ROOT);
                        drainStatement();
                        return d;
                    }
                    default:
                }
            }
            throw err(t, "SELECT", "DELETE", "SHOW", "CREATE", "DROP", "EXPLAIN", "GRANT", "REVOKE", "ALTER", "SET", "KILL");
        }

        void drainStatement() {
            while (true) {
                Tok t = scan();
                if (t.t == T.EOF || (t.t == T.OP && t.text.equals(";"))) {
                    unscan();
                    return;
                }
            }
        }

        // ---------------- identifiers
        String parseIdent() {
            Tok t = scan();
            if ((t.t == T.IDENT && !isReserved(t.text)) || t.t == T.QIDENT) {
                return t.text;
            }
            throw err(t, "identifier");
        }

        String parseIdentOrString() {
            Tok t = scan();
            if ((t.t == T.IDENT && !isReserved(t.text)) || t.t == T.QIDENT || t.t == T.STRING) {
                return t.text;
            }
            throw err(t, "identifier");
        }

        // ---------------- SELECT
        Select parseSelect() {
            Select s = new Select();
            s.fields = parseFields();
            Tok t = scan();
            if (isKw(t, "INTO")) {
                s.into = parseIntoTarget();
                t = scan();
            }
            if (!isKw(t, "FROM")) {
                throw err(t, "FROM");
            }
            s.sources = parseSources();
            if (acceptKw("WHERE")) {
                s.cond = parseExpr(0);
            }
            if (acceptKw("GROUP")) {
                expectKw("BY");
                s.dims = parseDims();
                if (isFillNext()) {
                    parseFill(s);
                }
            } else if (isFillNext()) {
                parseFill(s);
            }
            if (acceptKw("ORDER")) {
                expectKw("BY");
                Tok f = scan();
                if (f.t == T.IDENT && f.text.equalsIgnoreCase("time")) {
                    Tok d = scan();
                    if (isKw(d, "DESC")) {
                        s.desc = true;
                    } else if (!isKw(d, "ASC")) {
                        unscan();
                    }
                } else if (isKw(f, "DESC")) {
                    s.desc = true;
                } else if (isKw(f, "ASC")) {
                    // ok
                } else {
                    throw new ParseError("error parsing query: only ORDER BY time supported at this time");
                }
                if (acceptOp(",")) {
                    Tok nx = scan();
                    if (nx.t == T.IDENT && !isReserved(nx.text) || nx.t == T.QIDENT) {
                        throw new ParseError("error parsing query: only ORDER BY time supported at this time");
                    }
                    throw err(nx, "identifier");
                }
            }
            if (acceptKw("LIMIT")) {
                s.limit = parseInteger();
                s.hasLimit = true;
            }
            if (acceptKw("OFFSET")) {
                s.offset = parseInteger();
            }
            if (acceptKw("SLIMIT")) {
                s.slimit = parseInteger();
                s.hasSlimit = true;
            }
            if (acceptKw("SOFFSET")) {
                s.soffset = parseInteger();
            }
            if (acceptKw("TZ")) {
                expectOp("(");
                Tok z = scan();
                s.tz = z.text;
                expectOp(")");
            }
            return s;
        }

        boolean isFillNext() {
            Tok t = peek();
            return isKw(t, "FILL");
        }

        void parseFill(Select s) {
            scan(); // fill
            Tok p = scan();
            if (!(p.t == T.OP && p.text.equals("("))) {
                throw new ParseError("error parsing query: fill must be a function call");
            }
            Tok a = scan();
            if (a.t == T.OP && a.text.equals(")")) {
                throw new ParseError("error parsing query: fill requires an argument, e.g.: 0, null, none, previous, linear");
            }
            if (a.t == T.EOF) {
                throw err(a, "identifier", "string", "number", "bool");
            }
            s.fillExplicit = true;
            if (a.t == T.IDENT && a.text.equalsIgnoreCase("null")) {
                s.fill = FillKind.NULL;
            } else if (a.t == T.IDENT && a.text.equalsIgnoreCase("none")) {
                s.fill = FillKind.NONE;
            } else if (a.t == T.IDENT && a.text.equalsIgnoreCase("previous")) {
                s.fill = FillKind.PREVIOUS;
            } else if (a.t == T.IDENT && a.text.equalsIgnoreCase("linear")) {
                s.fill = FillKind.LINEAR;
            } else {
                boolean neg = false;
                Tok n = a;
                if (a.t == T.OP && (a.text.equals("-") || a.text.equals("+"))) {
                    neg = a.text.equals("-");
                    n = scan();
                }
                if (n.t == T.INT) {
                    s.fill = FillKind.NUMBER;
                    long v = Long.parseLong(n.text);
                    s.fillValue = neg ? -v : v;
                } else if (n.t == T.NUM) {
                    s.fill = FillKind.NUMBER;
                    double v = Double.parseDouble(n.text);
                    s.fillValue = neg ? -v : v;
                } else {
                    throw new ParseError("error parsing query: expected number argument in fill()");
                }
            }
            Tok c = scan();
            if (!(c.t == T.OP && c.text.equals(")"))) {
                throw err(c, ")");
            }
        }

        int parseInteger() {
            Tok t = scan();
            if (t.t == T.PARAM) {
                Expr b = bind(t);
                if (b instanceof IntLit il) {
                    return (int) Math.min(il.v, Integer.MAX_VALUE);
                }
                throw err(t, "integer");
            }
            boolean neg = false;
            if (t.t == T.OP && t.text.equals("-")) {
                neg = true;
                t = scan();
            }
            if (t.t != T.INT) {
                throw err(t, "integer");
            }
            if (neg) {
                throw new ParseError("error parsing query: found -, expected integer at line " + t.line + ", char " + (t.col - 1));
            }
            try {
                return (int) Math.min(Long.parseLong(t.text), Integer.MAX_VALUE);
            } catch (NumberFormatException e) {
                throw err(t, "integer");
            }
        }

        Source parseIntoTarget() {
            Source src2 = new Source();
            // db.rp.meas | db..meas | rp.meas? (influxql: [[db.]rp.]meas)
            List<String> parts = new ArrayList<>();
            List<Boolean> emptyMarks = new ArrayList<>();
            parseQualifiedName(src2);
            return src2;
        }

        void parseQualifiedName(Source s) {
            List<String> parts = new ArrayList<>();
            Tok t = scanRegexOrIdent();
            if (t.t == T.REGEX) {
                s.regex = compileRegex(t);
                return;
            }
            parts.add(t.text);
            while (true) {
                Tok d = scan();
                if (d.t == T.OP && d.text.equals(".")) {
                    Tok n = scanRegexOrIdent();
                    if (n.t == T.OP && n.text.equals(".")) {
                        parts.add("");
                        unscan();
                        continue;
                    }
                    if (n.t == T.REGEX) {
                        s.regex = compileRegex(n);
                        break;
                    }
                    if (n.t != T.IDENT && n.t != T.QIDENT) {
                        throw err(n, "identifier");
                    }
                    parts.add(n.text);
                } else {
                    unscan();
                    break;
                }
            }
            int n = parts.size() + (s.regex != null ? 1 : 0);
            if (s.regex != null) {
                if (parts.size() == 1) {
                    s.rp = parts.get(0);
                } else if (parts.size() == 2) {
                    s.db = parts.get(0);
                    s.rp = parts.get(1);
                }
                return;
            }
            if (parts.size() == 1) {
                s.name = parts.get(0);
            } else if (parts.size() == 2) {
                s.rp = parts.get(0);
                s.name = parts.get(1);
            } else if (parts.size() == 3) {
                s.db = parts.get(0);
                s.rp = parts.get(1);
                s.name = parts.get(2);
            } else {
                throw new ParseError("error parsing query: too many segments in " + String.join(".", parts));
            }
            if (s.db != null && s.db.isEmpty()) {
                s.db = null;
            }
            if (s.rp != null && s.rp.isEmpty()) {
                s.rp = null;
            }
        }

        Tok scanRegexOrIdent() {
            Tok t = scanRegex();
            if (t.t == T.REGEX || (t.t == T.IDENT && !isReserved(t.text)) || t.t == T.QIDENT) {
                return t;
            }
            if (t.t == T.OP && t.text.equals(".")) {
                return t;
            }
            throw err(t, "identifier");
        }

        Pattern compileRegex(Tok t) {
            try {
                return Pattern.compile(goRegexToJava(t.text));
            } catch (PatternSyntaxException e) {
                String desc = e.getDescription();
                String detail = t.text;
                if (desc.startsWith("Unclosed character class")) {
                    desc = "missing closing ]";
                    detail = t.text.substring(Math.max(0, t.text.lastIndexOf('[')));
                } else if (desc.startsWith("Unclosed group")) {
                    desc = "missing closing )";
                }
                throw new ParseError("error parsing query: error parsing regexp: " + desc + ": `" + detail + "` at line " + t.line + ", char " + (t.col));
            }
        }

        List<Source> parseSources() {
            List<Source> out = new ArrayList<>();
            do {
                Source s = new Source();
                Tok t = peek();
                if (t.t == T.OP && t.text.equals("(")) {
                    scan();
                    Tok kw = scan();
                    if (!isKw(kw, "SELECT")) {
                        throw err(kw, "SELECT");
                    }
                    s.sub = parseSelect();
                    expectOp(")");
                } else {
                    parseQualifiedName(s);
                }
                out.add(s);
            } while (acceptOp(","));
            return out;
        }

        List<Field> parseFields() {
            List<Field> out = new ArrayList<>();
            do {
                Expr e = parseFieldExpr();
                String alias = null;
                if (acceptKw("AS")) {
                    alias = parseIdent();
                }
                out.add(new Field(e, alias));
            } while (acceptOp(","));
            return out;
        }

        Expr parseFieldExpr() {
            if (atBareDistinct()) {
                scan();
                Tok nx = peek();
                if (nx.t == T.IDENT && isReserved(nx.text) || nx.t == T.EOF) {
                    scan();
                    throw err(nx, "identifier");
                }
                Expr inner = parseExpr(0);
                return new Call("distinct", new ArrayList<>(List.of(inner)));
            }
            Tok t = peek();
            if (t.t == T.OP && t.text.equals("*")) {
                scan();
                return new Wildcard(parseCastType(true));
            }
            if (t.t == T.REGEX || (t.t == T.OP && t.text.equals("/"))) {
                Tok r = scanRegex();
                if (r.t == T.REGEX) {
                    return new RegexLit(compileRegex(r), r.text);
                }
            }
            return parseExpr(0);
        }

        String parseCastType(boolean wildcard) {
            Tok t = scan();
            if (t.t == T.OP && t.text.equals("::")) {
                Tok ty = scan();
                if (ty.t == T.IDENT || ty.t == T.QIDENT) {
                    String x = ty.text.toLowerCase(Locale.ROOT);
                    if (wildcard) {
                        if (x.equals("field") || x.equals("tag")) {
                            return x;
                        }
                        throw err(ty, "field", "tag");
                    }
                    switch (x) {
                        case "float", "integer", "string", "boolean", "field", "tag", "unsigned":
                            return x;
                        default:
                            throw err(ty, "float", "integer", "unsigned", "string", "boolean", "field", "tag");
                    }
                }
                throw err(ty, "float", "integer", "string", "boolean", "field", "tag");
            }
            unscan();
            return "";
        }

        List<Dim> parseDims() {
            List<Dim> out = new ArrayList<>();
            do {
                Tok t = scanRegex();
                Dim d = new Dim();
                if (t.t == T.OP && t.text.equals("*")) {
                    d.kind = Dim.WILDCARD;
                } else if (t.t == T.REGEX) {
                    d.kind = Dim.REGEX;
                    d.regex = compileRegex(t);
                } else if ((t.t == T.IDENT || t.t == T.QIDENT) && peekIsOp("(") && t.text.equalsIgnoreCase("time")) {
                    scan();
                    d.kind = Dim.TIME;
                    List<Expr> args = new ArrayList<>();
                    if (!acceptOp(")")) {
                        do {
                            args.add(parseExpr(0));
                        } while (acceptOp(","));
                        expectOp(")");
                    }
                    if (args.size() < 1 || args.size() > 2) {
                        throw new ParseError("\u0000time dimension expected 1 or 2 arguments");
                    }
                    if (!(args.get(0) instanceof DurLit dl)) {
                        throw new ParseError("\u0000time dimension must have duration argument");
                    }
                    d.interval = dl.nanos;
                    if (dl.nanos <= 0) {
                        // influxql treats a non-positive interval as "no time grouping"
                        d.kind = Dim.WILDCARD - 100;
                    }
                    if (args.size() == 2) {
                        Expr a = args.get(1);
                        if (a instanceof DurLit o) {
                            d.offset = o.nanos;
                        } else if (a instanceof TimeLit tl && tl.nanos == -1) {
                            d.offsetNow = true;
                        } else if (a instanceof IntLit il && il.v == 0) {
                            d.offset = 0;
                        } else if (a instanceof Paren p2 && p2.e instanceof DurLit o) {
                            d.offset = o.nanos;
                        } else if (a instanceof Bin b2 && b2.op.equals("-") && b2.l instanceof IntLit z && z.v == 0 && b2.r instanceof DurLit o) {
                            d.offset = -o.nanos;
                        } else {
                            throw new ParseError("\u0000time dimension offset must be duration or now()");
                        }
                    }
                } else if (t.t == T.IDENT || t.t == T.QIDENT) {
                    d.kind = Dim.TAG;
                    d.name = t.text;
                    parseCastType(false);
                } else {
                    throw err(t, "identifier, string, number, bool");
                }
                if (d.kind != Dim.WILDCARD - 100) {
                    out.add(d);
                }
            } while (acceptOp(","));
            return out;
        }

        boolean peekIsOp(String op) {
            Tok t = peek();
            return t.t == T.OP && t.text.equals(op);
        }

        // ---------------- SHOW / DDL
        Stmt parseShow() {
            Show sh = new Show();
            Tok t = scan();
            if (t.t != T.IDENT) {
                throw err(t, "CONTINUOUS", "DATABASES", "DIAGNOSTICS", "FIELD", "GRANTS", "MEASUREMENT", "MEASUREMENTS", "QUERIES",
                        "RETENTION", "SERIES", "SHARD", "SHARDS", "STATS", "SUBSCRIPTIONS", "TAG", "USERS");
            }
            String kw = t.text.toUpperCase(Locale.ROOT);
            switch (kw) {
                case "DATABASES":
                    sh.kind = "DATABASES";
                    return sh;
                case "MEASUREMENTS":
                    sh.kind = "MEASUREMENTS";
                    parseShowOn(sh);
                    if (acceptKw("WITH")) {
                        expectKw("MEASUREMENT");
                        Tok op = scan();
                        if (op.t == T.OP && (op.text.equals("=") || op.text.equals("!=") || op.text.equals("=~") || op.text.equals("!~"))) {
                            sh.withMeasOp = op.text;
                            if (op.text.endsWith("~")) {
                                Tok r = scanRegex();
                                if (r.t != T.REGEX) {
                                    throw err(r, "regex");
                                }
                                sh.withMeasRegex = compileRegex(r);
                            } else {
                                sh.withMeas = parseIdentOrString();
                            }
                        } else {
                            throw err(op, "=", "=~");
                        }
                    }
                    parseShowTail(sh, false);
                    return sh;
                case "MEASUREMENT":
                    expectKw("CARDINALITY");
                    sh.kind = "MEASUREMENT_CARDINALITY";
                    return sh;
                case "SERIES":
                    sh.kind = "SERIES";
                    if (acceptKw("CARDINALITY")) {
                        sh.cardinality = true;
                    }
                    parseShowOn(sh);
                    parseShowFrom(sh);
                    parseShowTail(sh, false);
                    return sh;
                case "TAG": {
                    Tok k = scan();
                    if (isKw(k, "KEYS")) {
                        sh.kind = "TAG_KEYS";
                    } else if (isKw(k, "VALUES")) {
                        sh.kind = "TAG_VALUES";
                    } else if (isKw(k, "KEY")) {
                        expectKw("CARDINALITY");
                        sh.kind = "TAG_KEY_CARDINALITY";
                        return sh;
                    } else {
                        throw err(k, "KEYS", "VALUES");
                    }
                    if (sh.kind.equals("TAG_VALUES") && acceptKw("CARDINALITY")) {
                        sh.cardinality = true;
                    }
                    parseShowOn(sh);
                    parseShowFrom(sh);
                    if (sh.kind.equals("TAG_VALUES")) {
                        expectKw("WITH");
                        expectKw("KEY");
                        Tok op = scan();
                        if (isKw(op, "IN")) {
                            sh.withOp = "IN";
                            expectOp("(");
                            do {
                                sh.withKeys.add(parseIdentOrString());
                            } while (acceptOp(","));
                            expectOp(")");
                        } else if (op.t == T.OP && (op.text.equals("=") || op.text.equals("!=") || op.text.equals("<>") || op.text.equals("=~")
                                || op.text.equals("!~"))) {
                            sh.withOp = op.text.equals("<>") ? "!=" : op.text;
                            if (op.text.endsWith("~")) {
                                Tok r = scanRegex();
                                if (r.t != T.REGEX) {
                                    throw err(r, "regex");
                                }
                                sh.withRegex = compileRegex(r);
                            } else {
                                sh.withKeys.add(parseIdentOrString());
                            }
                        } else {
                            throw err(op, "IN", "=", "!=", "=~", "!~");
                        }
                    }
                    parseShowTail(sh, true);
                    return sh;
                }
                case "FIELD": {
                    Tok k = scan();
                    if (isKw(k, "KEYS")) {
                        sh.kind = "FIELD_KEYS";
                    } else if (isKw(k, "KEY")) {
                        expectKw("CARDINALITY");
                        sh.kind = "FIELD_KEY_CARDINALITY";
                        return sh;
                    } else {
                        throw err(k, "KEYS", "KEY");
                    }
                    parseShowOn(sh);
                    parseShowFrom(sh);
                    parseShowTail(sh, true);
                    return sh;
                }
                case "RETENTION":
                    expectKw("POLICIES");
                    sh.kind = "RETENTION_POLICIES";
                    parseShowOn(sh);
                    return sh;
                case "USERS":
                    sh.kind = "USERS";
                    return sh;
                case "GRANTS":
                    expectKw("FOR");
                    sh.kind = "GRANTS";
                    sh.forUser = parseIdent();
                    return sh;
                case "QUERIES":
                    sh.kind = "QUERIES";
                    return sh;
                case "STATS":
                    sh.kind = "STATS";
                    if (acceptKw("FOR")) {
                        scan();
                    }
                    return sh;
                case "DIAGNOSTICS":
                    sh.kind = "DIAGNOSTICS";
                    if (acceptKw("FOR")) {
                        scan();
                    }
                    return sh;
                case "CONTINUOUS":
                    expectKw("QUERIES");
                    sh.kind = "CONTINUOUS_QUERIES";
                    return sh;
                case "SUBSCRIPTIONS":
                    sh.kind = "SUBSCRIPTIONS";
                    return sh;
                case "SHARDS":
                    sh.kind = "SHARDS";
                    return sh;
                case "SHARD":
                    expectKw("GROUPS");
                    sh.kind = "SHARD_GROUPS";
                    return sh;
                default:
                    throw err(t, "CONTINUOUS", "DATABASES", "DIAGNOSTICS", "FIELD", "GRANTS", "MEASUREMENT", "MEASUREMENTS", "QUERIES",
                            "RETENTION", "SERIES", "SHARD", "SHARDS", "STATS", "SUBSCRIPTIONS", "TAG", "USERS");
            }
        }

        void parseShowOn(Show sh) {
            if (acceptKw("ON")) {
                sh.on = parseIdent();
            }
        }

        void parseShowFrom(Show sh) {
            if (acceptKw("FROM")) {
                sh.from = parseSources();
            }
        }

        void parseShowTail(Show sh, boolean withSlimit) {
            if (acceptKw("WHERE")) {
                sh.cond = parseExpr(0);
            }
            for (boolean progress = true; progress;) {
                progress = false;
                if (acceptKw("LIMIT")) {
                    sh.limit = parseInteger();
                    progress = true;
                }
                if (acceptKw("OFFSET")) {
                    sh.offset = parseInteger();
                    progress = true;
                }
                if (acceptKw("SLIMIT")) {
                    sh.slimit = parseInteger();
                    progress = true;
                }
                if (acceptKw("SOFFSET")) {
                    sh.soffset = parseInteger();
                    progress = true;
                }
            }
        }

        long parseDurationLit() {
            Tok t = scan();
            if (t.t == T.DUR) {
                return parseDurationNanos(t.text);
            }
            if (t.t == T.IDENT && t.text.equalsIgnoreCase("INF")) {
                return 0;
            }
            if (t.t == T.INT && t.text.equals("0")) {
                return 0;
            }
            throw err(t, "duration");
        }

        void parseRpOptions(Ddl d, boolean alter) {
            while (true) {
                Tok t = peek();
                if (isKw(t, "DURATION")) {
                    scan();
                    d.duration = parseDurationLit();
                    d.hasDuration = true;
                } else if (isKw(t, "REPLICATION")) {
                    scan();
                    d.replication = parseInteger();
                } else if (isKw(t, "SHARD")) {
                    scan();
                    expectKw("DURATION");
                    d.shardDuration = parseDurationLit();
                } else if (isKw(t, "DEFAULT")) {
                    scan();
                    d.isDefault = true;
                } else if (isKw(t, "NAME") && !alter) {
                    scan();
                    d.rpName = parseIdent();
                } else {
                    return;
                }
            }
        }

        Stmt parseCreate() {
            Tok t = scan();
            Ddl d = new Ddl();
            if (isKw(t, "DATABASE")) {
                d.kind = "CREATE_DATABASE";
                d.name = parseIdent();
                if (acceptKw("WITH")) {
                    parseRpOptions(d, false);
                }
                return d;
            }
            if (isKw(t, "RETENTION")) {
                expectKw("POLICY");
                d.kind = "CREATE_RP";
                d.rpName = parseIdent();
                expectKw("ON");
                d.on = parseIdent();
                parseRpOptions(d, false);
                if (!d.hasDuration) {
                    throw err(peek(), "DURATION");
                }
                return d;
            }
            if (isKw(t, "USER")) {
                d.kind = "CREATE_USER";
                d.name = parseIdent();
                expectKw("WITH");
                expectKw("PASSWORD");
                scan();
                if (acceptKw("WITH")) {
                    expectKw("ALL");
                    expectKw("PRIVILEGES");
                    d.isDefault = true;
                }
                return d;
            }
            if (isKw(t, "CONTINUOUS") || isKw(t, "SUBSCRIPTION")) {
                d.kind = "USER";
                d.text = "CREATE " + t.text.toUpperCase(Locale.ROOT);
                drainStatement();
                return d;
            }
            throw err(t, "CONTINUOUS", "DATABASE", "USER", "RETENTION", "SUBSCRIPTION");
        }

        Stmt parseAlter() {
            expectKw("RETENTION");
            expectKw("POLICY");
            Ddl d = new Ddl();
            d.kind = "ALTER_RP";
            d.rpName = parseIdent();
            expectKw("ON");
            d.on = parseIdent();
            parseRpOptions(d, true);
            return d;
        }

        Stmt parseDrop() {
            Tok t = scan();
            Ddl d = new Ddl();
            if (isKw(t, "DATABASE")) {
                d.kind = "DROP_DATABASE";
                d.name = parseIdent();
                return d;
            }
            if (isKw(t, "MEASUREMENT")) {
                d.kind = "DROP_MEASUREMENT";
                Source s = new Source();
                parseQualifiedName(s);
                d.from.add(s);
                return d;
            }
            if (isKw(t, "SERIES")) {
                d.kind = "DROP_SERIES";
                boolean any = false;
                if (acceptKw("FROM")) {
                    d.from = parseSources();
                    any = true;
                }
                if (acceptKw("WHERE")) {
                    d.cond = parseExpr(0);
                    any = true;
                }
                if (!any) {
                    throw err(peek(), "FROM", "WHERE");
                }
                return d;
            }
            if (isKw(t, "RETENTION")) {
                expectKw("POLICY");
                d.kind = "DROP_RP";
                d.rpName = parseIdent();
                expectKw("ON");
                d.on = parseIdent();
                return d;
            }
            if (isKw(t, "USER")) {
                d.kind = "DROP_USER";
                d.name = parseIdent();
                return d;
            }
            if (isKw(t, "CONTINUOUS") || isKw(t, "SUBSCRIPTION") || isKw(t, "SHARD")) {
                d.kind = "USER";
                d.text = "DROP " + t.text.toUpperCase(Locale.ROOT);
                drainStatement();
                return d;
            }
            throw err(t, "CONTINUOUS", "DATABASE", "MEASUREMENT", "RETENTION", "SERIES", "SHARD", "SUBSCRIPTION", "USER");
        }

        Stmt parseDelete() {
            Ddl d = new Ddl();
            d.kind = "DELETE";
            boolean any = false;
            if (acceptKw("FROM")) {
                d.from = parseSources();
                any = true;
            }
            if (acceptKw("WHERE")) {
                d.cond = parseExpr(0);
                any = true;
            }
            if (!any) {
                throw err(peek(), "FROM", "WHERE");
            }
            return d;
        }

        // ---------------- expressions
        static int prec(String op) {
            return switch (op) {
                case "OR" -> 1;
                case "AND" -> 2;
                case "=", "!=", "<>", "<", "<=", ">", ">=", "=~", "!~" -> 3;
                case "+", "-", "|", "^" -> 4;
                case "*", "/", "%", "&" -> 5;
                default -> 0;
            };
        }

        String binOp(Tok t) {
            if (t.t == T.OP) {
                String o = t.text;
                if (o.equals("<>")) {
                    return "!=";
                }
                return prec(o) > 0 ? o : null;
            }
            if (t.t == T.IDENT) {
                String u = t.text.toUpperCase(Locale.ROOT);
                if (u.equals("AND") || u.equals("OR")) {
                    return u;
                }
            }
            return null;
        }

        Expr parseExpr(int minPrec) {
            Expr lhs = parseUnary();
            while (true) {
                Tok t = scan();
                String op = binOp(t);
                if (op == null || prec(op) < minPrec) {
                    unscan();
                    return lhs;
                }
                int p = prec(op);
                Expr rhs;
                if (op.equals("=~") || op.equals("!~")) {
                    Tok r = scanRegex();
                    if (r.t == T.REGEX) {
                        rhs = new RegexLit(compileRegex(r), r.text);
                    } else if (r.t == T.ILLEGAL && r.text.startsWith("/")) {
                        throw new ParseError("error parsing query: bad regex:  at line " + r.line + ", char " + r.col);
                    } else {
                        throw err(r, "regex");
                    }
                } else {
                    rhs = parseExpr(p + 1);
                }
                lhs = new Bin(op, lhs, rhs);
            }
        }

        Expr parseUnary() {
            Tok t = scan();
            switch (t.t) {
                case IDENT: {
                    String u = t.text.toUpperCase(Locale.ROOT);
                    if (u.equals("TRUE") || u.equals("FALSE")) {
                        return new BoolLit(u.equals("TRUE"));
                    }
                    if (isReserved(t.text) && !(u.equals("DISTINCT") && peekIsOp("("))) {
                        throw err(t, "identifier", "string", "number", "bool");
                    }
                    if (peekIsOp("(")) {
                        scan();
                        return parseCall(t.text);
                    }
                    return new VarRef(t.text, parseCastType(false));
                }
                case QIDENT: {
                    if (peekIsOp("(")) {
                        scan();
                        return parseCall(t.text);
                    }
                    return new VarRef(t.text, parseCastType(false));
                }
                case STRING: {
                    return new StrLit(t.text);
                }
                case INT: {
                    try {
                        return new IntLit(Long.parseLong(t.text));
                    } catch (NumberFormatException e) {
                        throw new ParseError("error parsing query: unable to parse integer at line " + t.line + ", char " + t.col);
                    }
                }
                case NUM:
                    return new NumLit(Double.parseDouble(t.text));
                case DUR:
                    return new DurLit(parseDurationNanos(t.text));
                case REGEX:
                    return new RegexLit(compileRegex(t), t.text);
                case PARAM:
                    return bind(t);
                case OP: {
                    if (t.text.equals("(")) {
                        Expr e = parseExpr(0);
                        expectOp(")");
                        return new Paren(e);
                    }
                    if (t.text.equals("-") || t.text.equals("+")) {
                        Tok n = scan();
                        if (n.t == T.INT) {
                            long v = Long.parseLong(n.text);
                            return new IntLit(t.text.equals("-") ? -v : v);
                        }
                        if (n.t == T.NUM) {
                            double v = Double.parseDouble(n.text);
                            return new NumLit(t.text.equals("-") ? -v : v);
                        }
                        if (n.t == T.DUR) {
                            long v = parseDurationNanos(n.text);
                            return new DurLit(t.text.equals("-") ? -v : v);
                        }
                        unscan();
                        Expr inner = parseUnary();
                        return t.text.equals("-") ? new Bin("*", new IntLit(-1), inner) : inner;
                    }
                    throw err(t, "identifier", "string", "number", "bool");
                }
                default:
                    throw err(t, "identifier", "string", "number", "bool");
            }
        }

        Expr bind(Tok t) {
            if (!params.containsKey(t.text)) {
                throw new ParseError("error parsing query: missing parameter: " + t.text);
            }
            Object v = params.get(t.text);
            if (v instanceof String s) {
                return new StrLit(s);
            }
            if (v instanceof Boolean b) {
                return new BoolLit(b);
            }
            if (v instanceof Long l) {
                return new IntLit(l);
            }
            if (v instanceof Number n) {
                double d = n.doubleValue();
                if (d == Math.rint(d) && Math.abs(d) < 9e18 && !(v instanceof Double)) {
                    return new IntLit((long) d);
                }
                return new NumLit(d);
            }
            throw new ParseError("error parsing query: missing parameter: unable to bind parameter with type " + (v == null ? "<nil>" : "[]interface {}"));
        }

        Expr parseCall(String name) {
            List<Expr> args = new ArrayList<>();
            String lname = name.toLowerCase(Locale.ROOT);
            if (!acceptOp(")")) {
                do {
                    Tok t = peek();
                    if (t.t == T.OP && t.text.equals("*")) {
                        scan();
                        args.add(new Wildcard(parseCastType(true)));
                    } else if (t.t == T.OP && t.text.equals("/")) {
                        Tok r = scanRegex();
                        args.add(new RegexLit(compileRegex(r), r.text));
                    } else if (atBareDistinct()) {
                        scan();
                        Expr inner = parseExpr(0);
                        args.add(new Call("distinct", new ArrayList<>(List.of(inner))));
                    } else {
                        args.add(parseExpr(0));
                    }
                } while (acceptOp(","));
                expectOp(")");
            }
            if (lname.equals("now")) {
                return new TimeLit(-1); // resolved by the engine (call node kept for fidelity)
            }
            return new Call(lname, args);
        }
    }

    // ------------------------------------------------------------------ misc

    /** Translates the few Go/RE2-only regexp constructs into java.util.regex. */
    static String goRegexToJava(String re) {
        return re.replace("(?P<", "(?<");
    }

    static String quoteIdent(String s) {
        boolean plain = !s.isEmpty() && (Character.isLetter(s.charAt(0)) || s.charAt(0) == '_');
        for (int i = 0; plain && i < s.length(); i++) {
            char c = s.charAt(i);
            plain = Character.isLetterOrDigit(c) || c == '_';
        }
        if (plain) {
            return s;
        }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
}
