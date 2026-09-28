package com.sayonora.warp.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** The Cosmos DB SQL query language: syntax tree, lexer and recursive-descent parser. Evaluation lives in {@link CosmosEval}. */
final class CosmosSql {

    private CosmosSql() {
    }

    // ---------------------------------------------------------------------------------------------- syntax tree

    sealed interface Expr permits Lit, Param, Ident, Prop, Index, Unary, Binary, Ternary, InList, Between, Like, Call, ArrayLit, ObjLit,
            Sub {
    }

    /** A literal; {@code value == null} is the {@code undefined} literal. */
    record Lit(JsonElement value) implements Expr {
    }

    record Param(String name) implements Expr {
    }

    record Ident(String name) implements Expr {
    }

    record Prop(Expr base, String name) implements Expr {
    }

    record Index(Expr base, Expr index) implements Expr {
    }

    record Unary(String op, Expr e) implements Expr {
    }

    record Binary(String op, Expr l, Expr r) implements Expr {
    }

    record Ternary(Expr cond, Expr a, Expr b) implements Expr {
    }

    record InList(Expr e, List<Expr> list, boolean negated) implements Expr {
    }

    record Between(Expr e, Expr lo, Expr hi, boolean negated) implements Expr {
    }

    record Like(Expr e, Expr pattern, Expr escape, boolean negated) implements Expr {
    }

    record Call(String name, List<Expr> args) implements Expr {
    }

    record ArrayLit(List<Expr> items) implements Expr {
    }

    record ObjLit(LinkedHashMap<String, Expr> fields) implements Expr {
    }

    enum SubKind { SCALAR, EXISTS, ARRAY }

    record Sub(Query query, SubKind kind) implements Expr {
    }

    /** One projection: {@code expr [AS alias]}. */
    record Item(Expr expr, String alias) {
    }

    /** One FROM / JOIN source: {@code alias IN expr}, {@code expr [AS] alias} or {@code (subquery) alias}. */
    record Source(String alias, Expr expr, boolean iterate, Query sub, boolean join) {
    }

    record Order(Expr expr, boolean desc) {
    }

    static final class Query {
        boolean distinct;
        Expr top;
        boolean star;
        boolean value;
        List<Item> items = new ArrayList<>();
        List<Source> from = new ArrayList<>();
        Expr where;
        List<Expr> groupBy = new ArrayList<>();
        List<Order> orderBy = new ArrayList<>();
        Expr offset;
        Expr limit;
        String text = "";
    }

    static final Set<String> AGGREGATES = Set.of("COUNT", "SUM", "AVG", "MIN", "MAX");

    // ---------------------------------------------------------------------------------------------- lexer

    private enum T { ID, KW, STR, NUM, PARAM, OP, EOF }

    private record Tok(T type, String text, int pos) {
    }

    private static final Set<String> KEYWORDS = Set.of("SELECT", "DISTINCT", "TOP", "VALUE", "FROM", "WHERE", "GROUP", "BY", "ORDER", "ASC",
            "DESC", "OFFSET", "LIMIT", "AS", "IN", "JOIN", "AND", "OR", "NOT", "BETWEEN", "LIKE", "ESCAPE", "EXISTS", "ARRAY", "UNDEFINED",
            "NULL", "TRUE", "FALSE", "ROOT");

    private static List<Tok> lex(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '\'' || c == '"') {
                StringBuilder sb = new StringBuilder();
                int start = i++;
                boolean closed = false;
                while (i < n) {
                    char d = s.charAt(i++);
                    if (d == c) {
                        closed = true;
                        break;
                    }
                    if (d == '\\' && i < n) {
                        char e = s.charAt(i++);
                        switch (e) {
                            case 'n' -> sb.append('\n');
                            case 't' -> sb.append('\t');
                            case 'r' -> sb.append('\r');
                            case 'b' -> sb.append('\b');
                            case 'f' -> sb.append('\f');
                            case '/' -> sb.append('/');
                            case 'u' -> {
                                if (i + 4 > n) {
                                    throw syntax("Syntax error, invalid unicode escape", start, n);
                                }
                                sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                                i += 4;
                            }
                            default -> sb.append(e);
                        }
                    } else {
                        sb.append(d);
                    }
                }
                if (!closed) {
                    throw syntax("Syntax error, unterminated string literal", start, n);
                }
                out.add(new Tok(T.STR, sb.toString(), start));
            } else if (Character.isDigit(c) || c == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1))) {
                int start = i;
                while (i < n && Character.isDigit(s.charAt(i))) {
                    i++;
                }
                if (i < n && s.charAt(i) == '.') {
                    i++;
                    while (i < n && Character.isDigit(s.charAt(i))) {
                        i++;
                    }
                }
                if (i < n && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                    int j = i + 1;
                    if (j < n && (s.charAt(j) == '+' || s.charAt(j) == '-')) {
                        j++;
                    }
                    if (j < n && Character.isDigit(s.charAt(j))) {
                        while (j < n && Character.isDigit(s.charAt(j))) {
                            j++;
                        }
                        i = j;
                    }
                }
                out.add(new Tok(T.NUM, s.substring(start, i), start));
            } else if (Character.isLetter(c) || c == '_' || c == '$') {
                int start = i;
                while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_' || s.charAt(i) == '$')) {
                    i++;
                }
                String w = s.substring(start, i);
                out.add(new Tok(KEYWORDS.contains(w.toUpperCase(Locale.ROOT)) ? T.KW : T.ID, w, start));
            } else if (c == '@') {
                int start = i++;
                while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
                    i++;
                }
                if (i == start + 1) {
                    throw syntax("Syntax error, incorrect syntax near '@'.", start, start + 1);
                }
                out.add(new Tok(T.PARAM, s.substring(start, i), start));
            } else {
                int start = i;
                String op = null;
                for (String cand : new String[] {">>>", "<<", ">>", "||", "<=", ">=", "!=", "<>", "??"}) {
                    if (s.startsWith(cand, i)) {
                        op = cand;
                        break;
                    }
                }
                if (op == null) {
                    if ("()[]{},.:?*/%+-&|^~<>=".indexOf(c) < 0) {
                        throw syntax("Syntax error, incorrect syntax near '" + c + "'.", i, i + 1);
                    }
                    op = String.valueOf(c);
                }
                i += op.length();
                out.add(new Tok(T.OP, op, start));
            }
        }
        out.add(new Tok(T.EOF, "", n));
        return out;
    }

    static CosmosException syntax(String msg, int start, int end) {
        String m = "{\"errors\":[{\"severity\":\"Error\",\"location\":{\"start\":" + start + ",\"end\":" + end
                + "},\"code\":\"SC1001\",\"message\":" + new JsonPrimitive(msg) + "}]}";
        return CosmosException.badRequest(m);
    }

    // ---------------------------------------------------------------------------------------------- parser

    static Query parse(String sql) {
        Parser p = new Parser(sql);
        Query q = p.query();
        if (p.peek().type != T.EOF) {
            throw p.err();
        }
        q.text = sql;
        return q;
    }

    private static final class Parser {
        private final List<Tok> toks;
        private int pos;
        private final String sql;

        Parser(String sql) {
            this.sql = sql;
            this.toks = lex(sql);
        }

        Tok peek() {
            return toks.get(pos);
        }

        Tok peek(int k) {
            return toks.get(Math.min(pos + k, toks.size() - 1));
        }

        Tok next() {
            return toks.get(pos++);
        }

        CosmosException err() {
            Tok t = peek();
            String near = t.type == T.EOF ? "<EOF>" : t.text;
            return syntax("Syntax error, incorrect syntax near '" + near + "'.", t.pos, t.pos + Math.max(1, t.text.length()));
        }

        boolean kw(String k) {
            Tok t = peek();
            return t.type == T.KW && t.text.equalsIgnoreCase(k);
        }

        boolean kwAt(int off, String k) {
            Tok t = peek(off);
            return t.type == T.KW && t.text.equalsIgnoreCase(k);
        }

        boolean acceptKw(String k) {
            if (kw(k)) {
                pos++;
                return true;
            }
            return false;
        }

        void expectKw(String k) {
            if (!acceptKw(k)) {
                throw err();
            }
        }

        boolean op(String o) {
            Tok t = peek();
            return t.type == T.OP && t.text.equals(o);
        }

        boolean acceptOp(String o) {
            if (op(o)) {
                pos++;
                return true;
            }
            return false;
        }

        void expectOp(String o) {
            if (!acceptOp(o)) {
                throw err();
            }
        }

        Query query() {
            Query q = new Query();
            expectKw("SELECT");
            if (acceptKw("DISTINCT")) {
                q.distinct = true;
            }
            if (acceptKw("TOP")) {
                q.top = primary();
            }
            if (op("*")) {
                pos++;
                q.star = true;
            } else if (acceptKw("VALUE")) {
                q.value = true;
                q.items.add(new Item(expr(), null));
            } else {
                do {
                    Expr e = expr();
                    String alias = null;
                    if (acceptKw("AS")) {
                        alias = identName();
                    } else if (peek().type == T.ID) {
                        alias = next().text;
                    }
                    q.items.add(new Item(e, alias));
                } while (acceptOp(","));
            }
            if (acceptKw("FROM")) {
                q.from.add(source(false));
                while (kw("JOIN")) {
                    pos++;
                    q.from.add(source(true));
                }
            } else if (q.star) {
                throw err();
            }
            if (acceptKw("WHERE")) {
                q.where = expr();
            }
            if (kw("GROUP")) {
                pos++;
                expectKw("BY");
                do {
                    q.groupBy.add(expr());
                } while (acceptOp(","));
            }
            if (kw("ORDER")) {
                pos++;
                expectKw("BY");
                do {
                    Expr e = expr();
                    boolean desc = false;
                    if (acceptKw("DESC")) {
                        desc = true;
                    } else {
                        acceptKw("ASC");
                    }
                    q.orderBy.add(new Order(e, desc));
                } while (acceptOp(","));
            }
            if (acceptKw("OFFSET")) {
                q.offset = primary();
                expectKw("LIMIT");
                q.limit = primary();
            }
            return q;
        }

        String identName() {
            Tok t = next();
            if (t.type == T.ID || t.type == T.KW && t.text.equalsIgnoreCase("ROOT")) {
                return t.text;
            }
            pos--;
            throw err();
        }

        Source source(boolean join) {
            if (op("(") && kwAt(1, "SELECT")) {
                pos++;
                Query sub = query();
                expectOp(")");
                acceptKw("AS");
                String alias = peek().type == T.ID ? next().text : "$sub";
                return new Source(alias, null, false, sub, join);
            }
            if ((peek().type == T.ID || kw("ROOT")) && kwAt(1, "IN")) {
                String alias = next().text;
                pos++;
                Expr e = op("[") ? primary() : pathExpr();
                return new Source(alias, e, true, null, join);
            }
            Expr e = pathExpr();
            String alias = null;
            if (acceptKw("AS")) {
                alias = identName();
            } else if (peek().type == T.ID) {
                alias = next().text;
            }
            if (alias == null) {
                alias = implicitAlias(e);
            }
            return new Source(alias, e, false, null, join);
        }

        private String implicitAlias(Expr e) {
            if (e instanceof Ident i) {
                return i.name();
            }
            if (e instanceof Prop p) {
                return p.name();
            }
            return "$src";
        }

        /** A container expression: identifier followed by {@code .prop}, {@code ["prop"]} and {@code [n]}. */
        Expr pathExpr() {
            Expr e = new Ident(identName());
            while (true) {
                if (op(".") && (peek(1).type == T.ID || peek(1).type == T.KW)) {
                    pos++;
                    e = new Prop(e, next().text);
                } else if (op("[")) {
                    pos++;
                    Expr idx = expr();
                    expectOp("]");
                    e = idx instanceof Lit l && l.value() != null && l.value().isJsonPrimitive() && l.value().getAsJsonPrimitive().isString()
                            ? new Prop(e, l.value().getAsString()) : new Index(e, idx);
                } else {
                    return e;
                }
            }
        }

        // ---- expressions, lowest precedence first: ?: , ?? , OR, AND, NOT, IN/BETWEEN/LIKE, = != <>, < <= > >=, ||, |, ^, &, shifts, + -, * / %, unary
        Expr expr() {
            Expr c = coalesce();
            if (acceptOp("?")) {
                Expr a = expr();
                expectOp(":");
                Expr b = expr();
                return new Ternary(c, a, b);
            }
            return c;
        }

        Expr coalesce() {
            Expr l = or();
            while (acceptOp("??")) {
                l = new Binary("??", l, or());
            }
            return l;
        }

        Expr or() {
            Expr l = and();
            while (acceptKw("OR")) {
                l = new Binary("OR", l, and());
            }
            return l;
        }

        Expr and() {
            Expr l = not();
            while (acceptKw("AND")) {
                l = new Binary("AND", l, not());
            }
            return l;
        }

        Expr not() {
            if (kw("NOT") && !kwAt(1, "IN") && !kwAt(1, "BETWEEN") && !kwAt(1, "LIKE")) {
                pos++;
                return new Unary("NOT", not());
            }
            return inBetweenLike();
        }

        Expr inBetweenLike() {
            Expr l = equality();
            while (true) {
                boolean neg = false;
                int save = pos;
                if (kw("NOT")) {
                    neg = true;
                    pos++;
                }
                if (acceptKw("IN")) {
                    expectOp("(");
                    List<Expr> list = new ArrayList<>();
                    if (!op(")")) {
                        do {
                            list.add(expr());
                        } while (acceptOp(","));
                    }
                    expectOp(")");
                    l = new InList(l, list, neg);
                } else if (acceptKw("BETWEEN")) {
                    Expr lo = equality();
                    expectKw("AND");
                    Expr hi = equality();
                    l = new Between(l, lo, hi, neg);
                } else if (acceptKw("LIKE")) {
                    Expr pat = equality();
                    Expr esc = null;
                    if (acceptKw("ESCAPE")) {
                        esc = equality();
                    }
                    l = new Like(l, pat, esc, neg);
                } else {
                    pos = save;
                    return l;
                }
            }
        }

        Expr equality() {
            Expr l = relational();
            while (op("=") || op("!=") || op("<>")) {
                String o = next().text;
                l = new Binary(o.equals("<>") ? "!=" : o, l, relational());
            }
            return l;
        }

        Expr relational() {
            Expr l = concat();
            while (op("<") || op("<=") || op(">") || op(">=")) {
                l = new Binary(next().text, l, concat());
            }
            return l;
        }

        Expr concat() {
            Expr l = bitOr();
            while (op("||")) {
                pos++;
                l = new Binary("||", l, bitOr());
            }
            return l;
        }

        Expr bitOr() {
            Expr l = bitXor();
            while (op("|")) {
                pos++;
                l = new Binary("|", l, bitXor());
            }
            return l;
        }

        Expr bitXor() {
            Expr l = bitAnd();
            while (op("^")) {
                pos++;
                l = new Binary("^", l, bitAnd());
            }
            return l;
        }

        Expr bitAnd() {
            Expr l = shift();
            while (op("&")) {
                pos++;
                l = new Binary("&", l, shift());
            }
            return l;
        }

        Expr shift() {
            Expr l = additive();
            while (op("<<") || op(">>") || op(">>>")) {
                l = new Binary(next().text, l, additive());
            }
            return l;
        }

        Expr additive() {
            Expr l = multiplicative();
            while (op("+") || op("-")) {
                l = new Binary(next().text, l, multiplicative());
            }
            return l;
        }

        Expr multiplicative() {
            Expr l = unary();
            while (op("*") || op("/") || op("%")) {
                l = new Binary(next().text, l, unary());
            }
            return l;
        }

        Expr unary() {
            if (op("-") || op("+") || op("~")) {
                String o = next().text;
                Expr e = unary();
                if (o.equals("-") && e instanceof Lit l && CosmosJson.isNum(l.value())) {
                    return new Lit(CosmosJson.num(-CosmosJson.dbl(l.value())));
                }
                return new Unary(o, e);
            }
            return postfix();
        }

        Expr postfix() {
            Expr e = primary();
            while (true) {
                if (op(".") && (peek(1).type == T.ID || peek(1).type == T.KW)) {
                    pos++;
                    e = new Prop(e, next().text);
                } else if (op("[")) {
                    pos++;
                    Expr idx = expr();
                    expectOp("]");
                    e = idx instanceof Lit l && CosmosJson.isStr(l.value()) ? new Prop(e, l.value().getAsString()) : new Index(e, idx);
                } else {
                    return e;
                }
            }
        }

        Expr primary() {
            Tok t = peek();
            switch (t.type) {
                case NUM: {
                    pos++;
                    boolean integral = t.text.indexOf('.') < 0 && t.text.indexOf('e') < 0 && t.text.indexOf('E') < 0 && t.text.length() < 18;
                    JsonElement v = new JsonPrimitive(integral ? (Number) Long.valueOf(t.text) : (Number) Double.valueOf(t.text));
                    return new Lit(v);
                }
                case STR:
                    pos++;
                    return new Lit(new JsonPrimitive(t.text));
                case PARAM:
                    pos++;
                    return new Param(t.text);
                case OP: {
                    if (t.text.equals("(")) {
                        pos++;
                        if (kw("SELECT")) {
                            Query q = query();
                            expectOp(")");
                            return new Sub(q, SubKind.SCALAR);
                        }
                        Expr e = expr();
                        expectOp(")");
                        return e;
                    }
                    if (t.text.equals("[")) {
                        pos++;
                        List<Expr> items = new ArrayList<>();
                        if (!op("]")) {
                            do {
                                items.add(expr());
                            } while (acceptOp(","));
                        }
                        expectOp("]");
                        return new ArrayLit(items);
                    }
                    if (t.text.equals("{")) {
                        pos++;
                        LinkedHashMap<String, Expr> f = new LinkedHashMap<>();
                        if (!op("}")) {
                            do {
                                Tok k = next();
                                if (k.type != T.STR && k.type != T.ID) {
                                    pos--;
                                    throw err();
                                }
                                expectOp(":");
                                f.put(k.text, expr());
                            } while (acceptOp(","));
                        }
                        expectOp("}");
                        return new ObjLit(f);
                    }
                    throw err();
                }
                case KW: {
                    String k = t.text.toUpperCase(Locale.ROOT);
                    switch (k) {
                        case "NULL":
                            pos++;
                            return new Lit(CosmosJson.NULL);
                        case "TRUE":
                            pos++;
                            return new Lit(CosmosJson.TRUE);
                        case "FALSE":
                            pos++;
                            return new Lit(CosmosJson.FALSE);
                        case "UNDEFINED":
                            pos++;
                            return new Lit(null);
                        case "EXISTS":
                        case "ARRAY": {
                            pos++;
                            expectOp("(");
                            Query q = query();
                            expectOp(")");
                            return new Sub(q, k.equals("EXISTS") ? SubKind.EXISTS : SubKind.ARRAY);
                        }
                        case "ROOT":
                            pos++;
                            return new Ident(t.text);
                        default:
                            throw err();
                    }
                }
                case ID: {
                    pos++;
                    if (op("(")) {
                        pos++;
                        List<Expr> args = new ArrayList<>();
                        if (!op(")")) {
                            do {
                                args.add(expr());
                            } while (acceptOp(","));
                        }
                        expectOp(")");
                        return new Call(t.text, args);
                    }
                    // udf.name(...) : a user defined function call
                    if (t.text.equalsIgnoreCase("udf") && op(".") && peek(1).type == T.ID && peek(2).type == T.OP && peek(2).text.equals("(")) {
                        throw CosmosException.badRequest("User defined functions are not supported by Warp cosmoswire: JavaScript is not executed ("
                                + "udf." + peek(1).text + ")");
                    }
                    return new Ident(t.text);
                }
                default:
                    throw err();
            }
        }
    }

    /** Names of every parameter used by the query, in order of appearance (for validation). */
    static boolean isNumberLiteral(Expr e) {
        return e instanceof Lit l && CosmosJson.isNum(l.value());
    }

    static JsonArray emptyArray() {
        return new JsonArray();
    }

    static JsonObject emptyObject() {
        return new JsonObject();
    }

    static Map<String, Expr> noFields() {
        return Map.of();
    }
}
