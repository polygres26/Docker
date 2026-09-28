package com.sayonora.warp.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DynamoDB's PartiQL surface (ExecuteStatement, BatchExecuteStatement, ExecuteTransaction):
 * SELECT (any WHERE -- a partition-key equality drives a Query, anything else a filtered Scan --
 * projections, ORDER BY, {@code "table"."index"}, Limit/NextToken paging), INSERT, UPDATE (SET /
 * REMOVE), DELETE, with {@code ?} parameters and RETURNING. WHERE clauses become the same
 * {@link Expr} conditions the expression API uses, so evaluation semantics are shared.
 */
final class PartiQl {

    private final OperationHandlers handlers;
    private final PgItemStore store;

    PartiQl(OperationHandlers handlers, PgItemStore store) {
        this.handlers = handlers;
        this.store = store;
    }

    // ================================================================================== AST

    private sealed interface Stmt permits Select, Insert, Update, Delete {}

    private record Select(String table, String index, List<Expr.Path> projection, boolean countOnly, Expr.Cond where,
            String orderAttr, boolean desc) implements Stmt {}

    private record Insert(String table, Map<String, AttributeValue> item) implements Stmt {}

    private record Update(String table, List<Expr.UpdateAction> actions, Expr.Cond where, String returning) implements Stmt {}

    private record Delete(String table, Expr.Cond where, String returning) implements Stmt {}

    // ================================================================================== lexer

    private enum T { IDENT, QIDENT, STRING, NUMBER, PARAM, LP, RP, COMMA, DOT, LB, RB, LC, RC, COLON, EQ, NE, LT, LE, GT, GE,
        PLUS, MINUS, STAR, LSET, RSET, SEMI, EOF }

    private record Tok(T kind, String text, int pos) {}

    private static List<Tok> lex(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0, n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            int start = i;
            if (c == '\'' || c == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                boolean closed = false;
                while (i < n) {
                    char d = s.charAt(i);
                    if (d == c) {
                        if (i + 1 < n && s.charAt(i + 1) == c) {
                            sb.append(c);
                            i += 2;
                            continue;
                        }
                        closed = true;
                        i++;
                        break;
                    }
                    sb.append(d);
                    i++;
                }
                if (!closed) throw malformed(s);
                out.add(new Tok(c == '\'' ? T.STRING : T.QIDENT, sb.toString(), start));
            } else if (Character.isLetter(c) || c == '_') {
                while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) i++;
                out.add(new Tok(T.IDENT, s.substring(start, i), start));
            } else if (Character.isDigit(c) || (c == '-' && i + 1 < n && Character.isDigit(s.charAt(i + 1))
                    && (out.isEmpty() || isValueStart(out.get(out.size() - 1))))) {
                i++;
                while (i < n && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) i++;
                if (i < n && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                    i++;
                    if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
                    while (i < n && Character.isDigit(s.charAt(i))) i++;
                }
                out.add(new Tok(T.NUMBER, s.substring(start, i), start));
            } else {
                i++;
                switch (c) {
                    case '?' -> out.add(new Tok(T.PARAM, "?", start));
                    case '(' -> out.add(new Tok(T.LP, "(", start));
                    case ')' -> out.add(new Tok(T.RP, ")", start));
                    case ',' -> out.add(new Tok(T.COMMA, ",", start));
                    case '.' -> out.add(new Tok(T.DOT, ".", start));
                    case '[' -> out.add(new Tok(T.LB, "[", start));
                    case ']' -> out.add(new Tok(T.RB, "]", start));
                    case '{' -> out.add(new Tok(T.LC, "{", start));
                    case '}' -> out.add(new Tok(T.RC, "}", start));
                    case ':' -> out.add(new Tok(T.COLON, ":", start));
                    case ';' -> out.add(new Tok(T.SEMI, ";", start));
                    case '*' -> out.add(new Tok(T.STAR, "*", start));
                    case '+' -> out.add(new Tok(T.PLUS, "+", start));
                    case '-' -> out.add(new Tok(T.MINUS, "-", start));
                    case '=' -> out.add(new Tok(T.EQ, "=", start));
                    case '!' -> {
                        if (i < n && s.charAt(i) == '=') { i++; out.add(new Tok(T.NE, "<>", start)); }
                        else throw malformed(s);
                    }
                    case '<' -> {
                        if (i < n && s.charAt(i) == '>') { i++; out.add(new Tok(T.NE, "<>", start)); }
                        else if (i < n && s.charAt(i) == '=') { i++; out.add(new Tok(T.LE, "<=", start)); }
                        else if (i < n && s.charAt(i) == '<') { i++; out.add(new Tok(T.LSET, "<<", start)); }
                        else out.add(new Tok(T.LT, "<", start));
                    }
                    case '>' -> {
                        if (i < n && s.charAt(i) == '=') { i++; out.add(new Tok(T.GE, ">=", start)); }
                        else if (i < n && s.charAt(i) == '>') { i++; out.add(new Tok(T.RSET, ">>", start)); }
                        else out.add(new Tok(T.GT, ">", start));
                    }
                    default -> throw malformed(s);
                }
            }
        }
        out.add(new Tok(T.EOF, "<EOF>", n));
        return out;
    }

    private static boolean isValueStart(Tok prev) {
        return switch (prev.kind()) {
            case EQ, NE, LT, LE, GT, GE, LP, COMMA, LB, LC, COLON, LSET, PLUS, MINUS -> true;
            case IDENT -> Set.of("AND", "OR", "NOT", "BETWEEN", "IN", "SET", "VALUE", "VALUES").contains(prev.text().toUpperCase());
            default -> false;
        };
    }

    private static DynamoException malformed(String stmt) {
        return DynamoException.validation("Statement wasn't well formed, can't be processed: " + stmt);
    }

    // ================================================================================== parser

    private static final class Parser {
        private final String src;
        private final List<Tok> toks;
        private final List<AttributeValue> params;
        private int pos;
        private int paramIndex;

        Parser(String src, List<AttributeValue> params) {
            this.src = src;
            this.toks = lex(src);
            this.params = params;
        }

        private Tok peek() { return toks.get(pos); }
        private Tok peekAt(int o) { return toks.get(Math.min(pos + o, toks.size() - 1)); }
        private Tok next() { Tok t = toks.get(pos); if (t.kind() != T.EOF) pos++; return t; }
        private boolean accept(T k) { if (peek().kind() == k) { pos++; return true; } return false; }
        private void expect(T k) { if (!accept(k)) throw malformed(src); }

        private boolean kw(String k) {
            Tok t = peek();
            return t.kind() == T.IDENT && t.text().equalsIgnoreCase(k);
        }

        private boolean acceptKw(String k) {
            if (kw(k)) { pos++; return true; }
            return false;
        }

        private void expectKw(String k) {
            if (!acceptKw(k)) throw malformed(src);
        }

        Stmt statement() {
            Stmt s;
            if (acceptKw("SELECT")) s = select();
            else if (acceptKw("INSERT")) s = insert();
            else if (acceptKw("UPDATE")) s = update();
            else if (acceptKw("DELETE")) s = delete();
            else throw malformed(src);
            accept(T.SEMI);
            if (peek().kind() != T.EOF) throw malformed(src);
            if (paramIndex != params.size()) {
                throw DynamoException.validation("Number of parameters in request and statement don't match.");
            }
            return s;
        }

        // ---- names
        private String identifier() {
            Tok t = next();
            if (t.kind() != T.IDENT && t.kind() != T.QIDENT) throw malformed(src);
            return t.text();
        }

        private String[] tableRef() {
            String table = identifier();
            String index = null;
            if (accept(T.DOT)) index = identifier();
            return new String[] {table, index};
        }

        Expr.Path path() {
            List<Object> segs = new ArrayList<>();
            segs.add(identifier());
            while (true) {
                if (accept(T.DOT)) {
                    segs.add(identifier());
                } else if (peek().kind() == T.LB && peekAt(1).kind() == T.NUMBER) {
                    pos++;
                    segs.add(Integer.parseInt(next().text()));
                    expect(T.RB);
                } else {
                    break;
                }
            }
            return new Expr.Path(segs);
        }

        // ---- SELECT
        private Select select() {
            List<Expr.Path> proj = null;
            boolean count = false;
            if (accept(T.STAR)) {
                // all attributes
            } else if (kw("COUNT") && peekAt(1).kind() == T.LP) {
                pos++;
                expect(T.LP);
                if (!accept(T.STAR)) path();
                expect(T.RP);
                count = true;
            } else {
                proj = new ArrayList<>();
                do {
                    proj.add(path());
                } while (accept(T.COMMA));
            }
            expectKw("FROM");
            String[] ref = tableRef();
            Expr.Cond where = null;
            if (acceptKw("WHERE")) where = or();
            String order = null;
            boolean desc = false;
            if (acceptKw("ORDER")) {
                expectKw("BY");
                order = path().top();
                if (acceptKw("DESC")) desc = true;
                else acceptKw("ASC");
            }
            return new Select(ref[0], ref[1], proj, count, where, order, desc);
        }

        // ---- INSERT
        private Insert insert() {
            expectKw("INTO");
            String[] ref = tableRef();
            if (!acceptKw("VALUE") && !acceptKw("VALUES")) throw malformed(src);
            AttributeValue v = value();
            if (v.type != AttributeValue.Type.M) throw malformed(src);
            return new Insert(ref[0], v.map);
        }

        // ---- UPDATE
        private Update update() {
            String[] ref = tableRef();
            List<Expr.UpdateAction> actions = new ArrayList<>();
            boolean any = false;
            while (true) {
                if (acceptKw("SET")) {
                    any = true;
                    do {
                        Expr.Path p = path();
                        expect(T.EQ);
                        setAction(p, actions);
                    } while (accept(T.COMMA));
                } else if (acceptKw("REMOVE")) {
                    any = true;
                    do {
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.REMOVE, path(), null));
                    } while (accept(T.COMMA));
                } else {
                    break;
                }
            }
            if (!any) throw malformed(src);
            expectKw("WHERE");
            Expr.Cond where = or();
            return new Update(ref[0], actions, where, returning());
        }

        private void setAction(Expr.Path p, List<Expr.UpdateAction> out) {
            if ((kw("set_add") || kw("set_delete")) && peekAt(1).kind() == T.LP) {
                boolean add = next().text().equalsIgnoreCase("set_add");
                expect(T.LP);
                path();
                expect(T.COMMA);
                Expr.Operand v = operand();
                expect(T.RP);
                out.add(new Expr.UpdateAction(add ? Expr.ActionKind.ADD : Expr.ActionKind.DELETE, p, v));
                return;
            }
            Expr.Operand left = setOperand();
            if (peek().kind() == T.PLUS || peek().kind() == T.MINUS) {
                char op = next().kind() == T.PLUS ? '+' : '-';
                left = new Expr.Arith(op, left, setOperand());
            }
            out.add(new Expr.UpdateAction(Expr.ActionKind.SET, p, left));
        }

        private Expr.Operand setOperand() {
            if (kw("list_append") && peekAt(1).kind() == T.LP) {
                pos += 2;
                Expr.Operand a = setOperand();
                expect(T.COMMA);
                Expr.Operand b = setOperand();
                expect(T.RP);
                return new Expr.ListAppend(a, b);
            }
            if (kw("if_not_exists") && peekAt(1).kind() == T.LP) {
                pos += 2;
                Expr.Path p = path();
                expect(T.COMMA);
                Expr.Operand fb = setOperand();
                expect(T.RP);
                return new Expr.IfNotExists(p, fb);
            }
            return operand();
        }

        // ---- DELETE
        private Delete delete() {
            expectKw("FROM");
            String[] ref = tableRef();
            expectKw("WHERE");
            Expr.Cond where = or();
            return new Delete(ref[0], where, returning());
        }

        private String returning() {
            if (!acceptKw("RETURNING")) return null;
            String a = identifier().toUpperCase(), b = identifier().toUpperCase();
            if (!accept(T.STAR)) {
                do {
                    path();
                } while (accept(T.COMMA));
            }
            if (!(a.equals("ALL") || a.equals("MODIFIED")) || !(b.equals("OLD") || b.equals("NEW"))) throw malformed(src);
            return a + "_" + b;
        }

        // ---- conditions
        Expr.Cond or() {
            Expr.Cond l = and();
            if (!kw("OR")) return l;
            List<Expr.Cond> parts = new ArrayList<>(List.of(l));
            while (acceptKw("OR")) parts.add(and());
            return new Expr.Or(parts);
        }

        private Expr.Cond and() {
            Expr.Cond l = not();
            if (!kw("AND")) return l;
            List<Expr.Cond> parts = new ArrayList<>(List.of(l));
            while (acceptKw("AND")) parts.add(not());
            return new Expr.And(parts);
        }

        private Expr.Cond not() {
            if (acceptKw("NOT")) return new Expr.Not(not());
            return predicate();
        }

        private Expr.Cond predicate() {
            if (peek().kind() == T.LP) {
                pos++;
                Expr.Cond c = or();
                expect(T.RP);
                return c;
            }
            if (peek().kind() == T.IDENT && peekAt(1).kind() == T.LP) {
                String f = peek().text();
                if (f.equals("begins_with") || f.equals("contains") || f.equals("attribute_type")) {
                    pos += 2;
                    List<Expr.Operand> args = new ArrayList<>();
                    do {
                        args.add(operand());
                    } while (accept(T.COMMA));
                    expect(T.RP);
                    if (args.size() != 2) throw malformed(src);
                    return new Expr.Fn(f, args);
                }
                if (f.equals("attribute_exists") || f.equals("attribute_not_exists")) {
                    pos += 2;
                    Expr.Operand a = operand();
                    expect(T.RP);
                    return new Expr.Fn(f, List.of(a));
                }
            }
            Expr.Operand left = operand();
            Tok t = peek();
            switch (t.kind()) {
                case EQ, NE, LT, LE, GT, GE -> {
                    pos++;
                    return new Expr.Cmp(t.text(), left, operand());
                }
                default -> { }
            }
            if (acceptKw("BETWEEN")) {
                Expr.Operand lo = operand();
                expectKw("AND");
                return new Expr.Between(left, lo, operand());
            }
            if (acceptKw("IN")) {
                T close;
                if (accept(T.LP)) close = T.RP;
                else if (accept(T.LB)) close = T.RB;
                else if (accept(T.LSET)) close = T.RSET;
                else throw malformed(src);
                List<Expr.Operand> c = new ArrayList<>();
                do {
                    c.add(operand());
                } while (accept(T.COMMA));
                expect(close);
                return new Expr.In(left, c);
            }
            if (acceptKw("IS")) {
                boolean neg = acceptKw("NOT");
                if (acceptKw("MISSING")) {
                    return new Expr.Fn(neg ? "attribute_exists" : "attribute_not_exists", List.of(left));
                }
                if (acceptKw("NULL")) {
                    return new Expr.Cmp(neg ? "<>" : "=", left, new Expr.ValueOp(AttributeValue.ofNull()));
                }
                throw malformed(src);
            }
            throw malformed(src);
        }

        Expr.Operand operand() {
            Tok t = peek();
            if (t.kind() == T.IDENT && t.text().equals("size") && peekAt(1).kind() == T.LP) {
                pos += 2;
                Expr.Path p = path();
                expect(T.RP);
                return new Expr.SizeOp(p);
            }
            if (t.kind() == T.IDENT || t.kind() == T.QIDENT) {
                if (t.kind() == T.IDENT) {
                    String u = t.text().toUpperCase();
                    if (u.equals("TRUE") || u.equals("FALSE") || u.equals("NULL") || u.equals("MISSING")) {
                        return new Expr.ValueOp(value());
                    }
                }
                return new Expr.PathOp(path());
            }
            return new Expr.ValueOp(value());
        }

        // ---- values
        AttributeValue value() {
            Tok t = next();
            switch (t.kind()) {
                case STRING:
                    return AttributeValue.ofS(t.text());
                case NUMBER:
                    return AttributeValue.ofN(t.text());
                case PARAM:
                    if (paramIndex >= params.size()) {
                        throw DynamoException.validation("Number of parameters in request and statement don't match.");
                    }
                    return params.get(paramIndex++);
                case IDENT: {
                    String u = t.text().toUpperCase();
                    if (u.equals("TRUE")) return AttributeValue.ofBool(true);
                    if (u.equals("FALSE")) return AttributeValue.ofBool(false);
                    if (u.equals("NULL")) return AttributeValue.ofNull();
                    throw malformed(src);
                }
                case LB: {
                    List<AttributeValue> l = new ArrayList<>();
                    if (!accept(T.RB)) {
                        do {
                            l.add(value());
                        } while (accept(T.COMMA));
                        expect(T.RB);
                    }
                    return AttributeValue.ofL(l);
                }
                case LC: {
                    Map<String, AttributeValue> m = new LinkedHashMap<>();
                    if (!accept(T.RC)) {
                        do {
                            Tok k = next();
                            if (k.kind() != T.STRING && k.kind() != T.QIDENT && k.kind() != T.IDENT) throw malformed(src);
                            expect(T.COLON);
                            m.put(k.text(), value());
                        } while (accept(T.COMMA));
                        expect(T.RC);
                    }
                    return AttributeValue.ofM(m);
                }
                case LSET: {
                    List<AttributeValue> members = new ArrayList<>();
                    do {
                        members.add(value());
                    } while (accept(T.COMMA));
                    expect(T.RSET);
                    return setOf(members);
                }
                case MINUS:
                    return negate(value());
                default:
                    throw malformed(src);
            }
        }

        private AttributeValue negate(AttributeValue v) {
            if (v.type != AttributeValue.Type.N) throw malformed(src);
            return AttributeValue.ofNumber(v.number().negate());
        }

        private AttributeValue setOf(List<AttributeValue> members) {
            AttributeValue.Type first = members.get(0).type;
            AttributeValue.Type st = switch (first) {
                case S -> AttributeValue.Type.SS;
                case N -> AttributeValue.Type.NS;
                case B -> AttributeValue.Type.BS;
                default -> throw DynamoException.validation("Set members must be strings, numbers or binary values");
            };
            Set<String> set = new LinkedHashSet<>();
            for (AttributeValue m : members) {
                if (m.type != first) throw DynamoException.validation("Set members must all have the same type");
                if (!set.add(m.scalar)) throw DynamoException.validation("Input collection contains duplicates");
            }
            return AttributeValue.ofSet(st, set);
        }
    }

    private Stmt parse(String statement, JsonArray params) {
        List<AttributeValue> p = new ArrayList<>();
        if (params != null) for (JsonElement e : params) p.add(AttributeValue.fromJson(e));
        return new Parser(statement, p).statement();
    }

    private static JsonArray paramsOf(JsonObject o) {
        return LegacyParams.has(o, "Parameters") ? o.getAsJsonArray("Parameters") : null;
    }

    // ================================================================================== analysis helpers

    private static List<Expr.Cond> conjuncts(Expr.Cond c) {
        List<Expr.Cond> out = new ArrayList<>();
        if (c == null) return out;
        if (c instanceof Expr.And a) {
            for (Expr.Cond p : a.parts()) out.addAll(conjuncts(p));
        } else {
            out.add(c);
        }
        return out;
    }

    private static Expr.Cond andOf(List<Expr.Cond> parts) {
        if (parts.isEmpty()) return null;
        return parts.size() == 1 ? parts.get(0) : new Expr.And(parts);
    }

    /** {attr, op, values...} of a conjunct that has the shape "top-level attribute vs literal", else null. */
    private static Expr.KeyCond asKeyTerm(Expr.Cond c) {
        try {
            List<Expr.KeyCond> t = KeyPlanner.termsFromCondition(c);
            return t.size() == 1 ? t.get(0) : null;
        } catch (DynamoException e) {
            return null;
        }
    }

    private record KeyExtraction(KeyPlanner.Plan plan, Expr.Cond remaining) {}

    /** Splits a WHERE clause into a Query key plan for the access path and a residual filter. */
    private KeyExtraction extractKey(TableSchema s, TableSchema.IndexDef idx, Expr.Cond where) {
        List<Expr.Cond> parts = conjuncts(where);
        List<TableSchema.KeyAttr> hash = KeyPlanner.hashAttrs(s, idx), range = KeyPlanner.rangeAttrs(s, idx);
        Map<Expr.Cond, Expr.KeyCond> terms = new LinkedHashMap<>();
        for (Expr.Cond c : parts) {
            Expr.KeyCond t = asKeyTerm(c);
            if (t != null) terms.put(c, t);
        }
        List<Expr.KeyCond> chosen = new ArrayList<>();
        Set<Expr.Cond> used = new LinkedHashSet<>();
        for (TableSchema.KeyAttr h : hash) {
            Expr.Cond found = null;
            for (var e : terms.entrySet()) {
                if (e.getValue().attr().equals(h.name()) && e.getValue().op().equals("EQ") && !used.contains(e.getKey())) {
                    found = e.getKey();
                    break;
                }
            }
            if (found == null) return new KeyExtraction(null, where);
            chosen.add(terms.get(found));
            used.add(found);
        }
        for (TableSchema.KeyAttr r : range) {
            Expr.Cond found = null;
            for (var e : terms.entrySet()) {
                if (e.getValue().attr().equals(r.name()) && !used.contains(e.getKey())) {
                    found = e.getKey();
                    break;
                }
            }
            if (found == null) break;
            Expr.KeyCond t = terms.get(found);
            chosen.add(t);
            used.add(found);
            if (!t.op().equals("EQ")) break;
        }
        List<Expr.Cond> rest = new ArrayList<>();
        for (Expr.Cond c : parts) if (!used.contains(c)) rest.add(c);
        try {
            return new KeyExtraction(KeyPlanner.plan(s, idx, chosen), andOf(rest));
        } catch (DynamoException e) {
            // e.g. a value of the wrong type: DynamoDB reports it, so do we
            throw e;
        }
    }

    /** UPDATE / DELETE: WHERE must pin the whole primary key; anything else becomes a condition. */
    private record KeyAndCond(Map<String, AttributeValue> key, Expr.Cond cond) {}

    private KeyAndCond primaryKeyOf(TableSchema s, Expr.Cond where) {
        List<Expr.Cond> parts = conjuncts(where);
        Map<String, AttributeValue> key = new LinkedHashMap<>();
        List<Expr.Cond> rest = new ArrayList<>();
        List<String> need = new ArrayList<>(List.of(s.partitionKeyName()));
        if (s.hasSortKey()) need.add(s.sortKeyName());
        for (Expr.Cond c : parts) {
            Expr.KeyCond t = asKeyTerm(c);
            if (t != null && t.op().equals("EQ") && need.contains(t.attr()) && !key.containsKey(t.attr())) {
                key.put(t.attr(), t.v1());
            } else {
                rest.add(c);
            }
        }
        if (key.size() != need.size()) {
            throw DynamoException.validation("Where clause does not contain a mandatory equality on all key attributes");
        }
        Map<String, AttributeValue> ordered = new LinkedHashMap<>();
        for (String n : need) ordered.put(n, key.get(n));
        ItemValidator.validateKey(s, ordered);
        return new KeyAndCond(ordered, andOf(rest));
    }

    private static String hashOf(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) sb.append(String.format("%02x", d[i]));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ================================================================================== ExecuteStatement

    JsonObject executeStatement(JsonObject req) {
        String statement = ensureStatement(req);
        String consumed = enumOrDefault(req, "ReturnConsumedCapacity", "NONE", "INDEXES", "TOTAL", "NONE");
        String onFailure = enumOrDefault(req, "ReturnValuesOnConditionCheckFailure", "NONE", "ALL_OLD", "NONE");
        Stmt stmt = parse(statement, paramsOf(req));
        boolean consistent = LegacyParams.has(req, "ConsistentRead") && req.get("ConsistentRead").getAsBoolean();
        Integer limit = LegacyParams.has(req, "Limit") ? req.get("Limit").getAsInt() : null;
        if (limit != null && limit < 1) {
            throw DynamoException.validation("1 validation error detected: Value '" + limit + "' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1");
        }
        String nextToken = OperationHandlers.optString(req, "NextToken");
        return run(stmt, statement, consistent, limit, nextToken, consumed, onFailure, false, null);
    }

    private static String ensureStatement(JsonObject req) {
        String s = OperationHandlers.optString(req, "Statement");
        if (s == null || s.isBlank()) {
            throw DynamoException.validation("1 validation error detected: Value null at 'statement' failed to satisfy constraint: Member must not be null");
        }
        return s;
    }

    private static String enumOrDefault(JsonObject req, String field, String def, String... allowed) {
        String v = OperationHandlers.optString(req, field);
        if (v == null) return def;
        for (String a : allowed) if (a.equals(v)) return v;
        throw DynamoException.validation("1 validation error detected: Value '" + v + "' at '" + Character.toLowerCase(field.charAt(0)) + field.substring(1)
                + "' failed to satisfy constraint: Member must satisfy enum value set: [" + String.join(", ", allowed) + "]");
    }

    /** Runs one parsed statement; {@code batchSlot} restricts SELECT to a full-primary-key lookup. */
    private JsonObject run(Stmt stmt, String statement, boolean consistent, Integer limit, String nextToken, String consumed,
            String onFailure, boolean batchSlot, List<Map<String, AttributeValue>> singleOut) {
        JsonObject resp = new JsonObject();
        JsonArray items = new JsonArray();
        switch (stmt) {
            case Select sel -> {
                TableSchema s = store.describeTable(sel.table());
                TableSchema.IndexDef idx = null;
                if (sel.index() != null) {
                    idx = s.index(sel.index());
                    if (idx == null) {
                        throw DynamoException.validation("The table does not have the specified index");
                    }
                    if (batchSlot) {
                        throw DynamoException.validation("Select statements within BatchExecuteStatement must specify the primary key in the where clause.");
                    }
                    if (consistent && !idx.local()) {
                        throw DynamoException.validation("Strongly consistent read is not supported on Global Secondary Indexes");
                    }
                }
                KeyExtraction ke = extractKey(s, idx, sel.where());
                if (batchSlot) {
                    KeyAndCond pk = primaryKeyOf(s, sel.where()); // throws unless the whole primary key is pinned
                    Map<String, AttributeValue> item = store.getItem(s, pk.key());
                    if (item != null && (pk.cond() == null || ExprEval.test(pk.cond(), item))) {
                        if (singleOut != null) singleOut.add(ExprEval.project(item, sel.projection()));
                    }
                    return resp;
                }
                Map<String, AttributeValue> startKey = null;
                String tokenTag = hashOf(statement);
                if (nextToken != null) {
                    startKey = decodeToken(nextToken, tokenTag, s, idx);
                }
                if (sel.orderAttr() != null && ke.plan() != null) {
                    List<TableSchema.KeyAttr> range = KeyPlanner.rangeAttrs(s, idx);
                    if (range.isEmpty() || !range.get(0).name().equals(sel.orderAttr())) {
                        throw DynamoException.validation("Unsupported ORDER BY attribute: " + sel.orderAttr());
                    }
                }
                QueryEngine.Result r = handlers.engine.run(new QueryEngine.Spec(s, idx, ke.plan(), ke.remaining(), limit, startKey,
                        !sel.desc(), 0, 0, !sel.countOnly()));
                for (Map<String, AttributeValue> item : r.items()) {
                    Map<String, AttributeValue> view = item;
                    if (idx != null && idx.local()) view = QueryEngine.projectForIndex(s, idx, item);
                    items.add(PgItemStore.itemToJson(ExprEval.project(view, sel.projection())));
                }
                if (sel.countOnly()) {
                    JsonObject c = new JsonObject();
                    c.add("count", new JsonObject());
                    c.getAsJsonObject("count").addProperty("N", String.valueOf(r.matched()));
                    items.add(c.get("count"));
                }
                if (r.lastEvaluatedKey() != null) {
                    resp.addProperty("NextToken", encodeToken(tokenTag, r.lastEvaluatedKey()));
                }
                if (Capacity.requested(consumed)) {
                    Capacity.Usage u = new Capacity.Usage(s.tableName());
                    double units = Capacity.readUnits(r.scannedBytes(), consistent, false);
                    if (idx != null) u.addIndex(idx, units);
                    else u.tableUnits = units;
                    resp.add("ConsumedCapacity", Capacity.toJson(consumed, u));
                }
            }
            case Insert ins -> {
                TableSchema s = store.describeTable(ins.table());
                Expr.Cond notExists = new Expr.Fn("attribute_not_exists", List.of(new Expr.PathOp(Expr.Path.of(s.partitionKeyName()))));
                try {
                    handlers.putForPartiQl(s, ins.item(), notExists, false);
                } catch (PgItemStore.ConditionalCheckFailed e) {
                    throw new DynamoException("DuplicateItemException", "Duplicate primary key exists in table");
                }
            }
            case Update up -> {
                TableSchema s = store.describeTable(up.table());
                KeyAndCond kc = primaryKeyOf(s, up.where());
                Expr.Cond exists = new Expr.Fn("attribute_exists", List.of(new Expr.PathOp(Expr.Path.of(s.partitionKeyName()))));
                Expr.Cond cond = kc.cond() == null ? exists : new Expr.And(List.of(exists, kc.cond()));
                PgItemStore.WriteResult r;
                try {
                    r = handlers.updateForPartiQl(s, kc.key(), new Expr.UpdatePlan(up.actions()), cond);
                } catch (PgItemStore.ConditionalCheckFailed e) {
                    throw failure(e, onFailure);
                }
                Map<String, AttributeValue> ret = returningItem(up.returning(), r);
                if (ret != null) items.add(PgItemStore.itemToJson(ret));
            }
            case Delete del -> {
                TableSchema s = store.describeTable(del.table());
                KeyAndCond kc = primaryKeyOf(s, del.where());
                PgItemStore.WriteResult r;
                try {
                    r = handlers.deleteForPartiQl(s, kc.key(), kc.cond(), del.returning() != null);
                } catch (PgItemStore.ConditionalCheckFailed e) {
                    throw failure(e, onFailure);
                }
                if (del.returning() != null && r.old() != null) items.add(PgItemStore.itemToJson(r.old()));
            }
        }
        resp.add("Items", items);
        return resp;
    }

    private static DynamoException failure(PgItemStore.ConditionalCheckFailed e, String onFailure) {
        if ("ALL_OLD".equals(onFailure) && e.item != null) e.withExtra("Item", PgItemStore.itemToJson(e.item));
        return e;
    }

    private static Map<String, AttributeValue> returningItem(String returning, PgItemStore.WriteResult r) {
        if (returning == null) return null;
        return switch (returning) {
            case "ALL_NEW" -> r.neu();
            case "ALL_OLD" -> r.old();
            case "MODIFIED_NEW" -> ExprEval.valuesAt(r.neu(), r.touched());
            case "MODIFIED_OLD" -> ExprEval.valuesAt(r.old(), r.touched());
            default -> null;
        };
    }

    // ---- NextToken: bound to the statement that issued it
    private static String encodeToken(String tag, Map<String, AttributeValue> lek) {
        JsonObject o = new JsonObject();
        o.addProperty("h", tag);
        o.add("k", PgItemStore.itemToJson(lek));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(o.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static Map<String, AttributeValue> decodeToken(String token, String tag, TableSchema s, TableSchema.IndexDef idx) {
        JsonObject o;
        try {
            o = JsonParser.parseString(new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8)).getAsJsonObject();
        } catch (RuntimeException e) {
            throw DynamoException.validation("Invalid NextToken");
        }
        if (!o.has("h") || !o.get("h").getAsString().equals(tag)) {
            throw DynamoException.validation("NextToken does not match request");
        }
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(o.getAsJsonObject("k"));
        KeyPlanner.checkStartKey(s, idx, key);
        return key;
    }

    // ================================================================================== BatchExecuteStatement

    JsonObject batchExecuteStatement(JsonObject req) {
        String consumed = enumOrDefault(req, "ReturnConsumedCapacity", "NONE", "INDEXES", "TOTAL", "NONE");
        if (!LegacyParams.has(req, "Statements") || req.getAsJsonArray("Statements").isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '[]' at 'statements' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonArray statements = req.getAsJsonArray("Statements");
        if (statements.size() > 25) {
            throw DynamoException.validation("1 validation error detected: Value at 'statements' failed to satisfy constraint: Member must have length less than or equal to 25");
        }
        List<Stmt> parsed = new ArrayList<>();
        boolean reads = false, writes = false;
        for (JsonElement e : statements) {
            JsonObject s = e.getAsJsonObject();
            Stmt st = parse(ensureStatement(s), paramsOf(s));
            parsed.add(st);
            if (st instanceof Select) reads = true;
            else writes = true;
        }
        if (reads && writes) {
            throw DynamoException.validation("Read and write requests together in the same batch is not supported.");
        }
        JsonArray responses = new JsonArray();
        for (int i = 0; i < parsed.size(); i++) {
            Stmt st = parsed.get(i);
            JsonObject entry = new JsonObject();
            String table = st instanceof Select s ? s.table() : st instanceof Insert s ? s.table()
                    : st instanceof Update s ? s.table() : ((Delete) st).table();
            entry.addProperty("TableName", table);
            try {
                List<Map<String, AttributeValue>> out = new ArrayList<>();
                JsonObject r = run(st, ensureStatement(statements.get(i).getAsJsonObject()), false, null, null, consumed,
                        OperationHandlers.optString(statements.get(i).getAsJsonObject(), "ReturnValuesOnConditionCheckFailure") == null
                                ? "NONE" : "ALL_OLD", true, out);
                if (!out.isEmpty()) {
                    entry.add("Item", PgItemStore.itemToJson(out.get(0)));
                } else if (!(st instanceof Select) && r.has("Items") && !r.getAsJsonArray("Items").isEmpty()) {
                    entry.add("Item", r.getAsJsonArray("Items").get(0));
                }
            } catch (DynamoException de) {
                JsonObject err = new JsonObject();
                err.addProperty("Code", batchErrorCode(de.dynamoErrorType));
                err.addProperty("Message", de.getMessage());
                if (de.extraBody != null && de.extraBody.has("Item")) err.add("Item", de.extraBody.get("Item"));
                entry.add("Error", err);
                entry.remove("TableName");
                entry.addProperty("TableName", table);
            }
            responses.add(entry);
        }
        JsonObject resp = new JsonObject();
        resp.add("Responses", responses);
        return resp;
    }

    private static String batchErrorCode(String type) {
        return switch (type) {
            case "ValidationException" -> "ValidationError";
            case "ConditionalCheckFailedException" -> "ConditionalCheckFailed";
            case "DuplicateItemException" -> "DuplicateItem";
            case "ResourceNotFoundException" -> "ResourceNotFound";
            case "AccessDeniedException" -> "AccessDenied";
            case "ProvisionedThroughputExceededException" -> "ProvisionedThroughputExceeded";
            case "TransactionConflictException" -> "TransactionConflict";
            default -> "InternalServerError";
        };
    }

    // ================================================================================== ExecuteTransaction

    JsonObject executeTransaction(JsonObject req) {
        if (!LegacyParams.has(req, "TransactStatements") || req.getAsJsonArray("TransactStatements").isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '[]' at 'transactStatements' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonArray statements = req.getAsJsonArray("TransactStatements");
        if (statements.size() > 100) {
            throw DynamoException.validation("1 validation error detected: Value at 'transactStatements' failed to satisfy constraint: Member must have length less than or equal to 100");
        }
        String token = OperationHandlers.optString(req, "ClientRequestToken");
        List<Stmt> parsed = new ArrayList<>();
        boolean reads = false, writes = false;
        for (JsonElement e : statements) {
            JsonObject s = e.getAsJsonObject();
            Stmt st = parse(ensureStatement(s), paramsOf(s));
            parsed.add(st);
            if (st instanceof Select) reads = true;
            else writes = true;
        }
        if (reads && writes) {
            throw DynamoException.validation("ExecuteTransaction API does not support both read and write operations in the same request.");
        }
        JsonObject resp = new JsonObject();
        JsonArray responses = new JsonArray();
        if (reads) {
            List<OperationHandlers.TxGet> gets = new ArrayList<>();
            for (Stmt st : parsed) {
                Select sel = (Select) st;
                if (sel.index() != null) {
                    throw DynamoException.validation("Select statements within ExecuteTransaction must specify the primary key in the where clause.");
                }
                TableSchema s = store.describeTable(sel.table());
                KeyAndCond kc = primaryKeyOf(s, sel.where());
                gets.add(new OperationHandlers.TxGet(s, kc.key(), sel.projection()));
            }
            List<Map<String, AttributeValue>> items = handlers.runTransactGets(gets);
            for (int i = 0; i < gets.size(); i++) {
                JsonObject r = new JsonObject();
                r.addProperty("TableName", gets.get(i).schema().tableName());
                if (items.get(i) != null) r.add("Item", PgItemStore.itemToJson(ExprEval.project(items.get(i), gets.get(i).projection())));
                responses.add(r);
            }
        } else {
            List<OperationHandlers.TxOp> ops = new ArrayList<>();
            for (int i = 0; i < parsed.size(); i++) {
                boolean returnOld = "ALL_OLD".equals(OperationHandlers.optString(statements.get(i).getAsJsonObject(),
                        "ReturnValuesOnConditionCheckFailure"));
                switch (parsed.get(i)) {
                    case Insert ins -> {
                        TableSchema s = store.describeTable(ins.table());
                        ItemValidator.validateItem(s, ins.item(), false);
                        Expr.Cond notExists = new Expr.Fn("attribute_not_exists", List.of(new Expr.PathOp(Expr.Path.of(s.partitionKeyName()))));
                        ops.add(new OperationHandlers.TxOp("Put", s, ins.item(), notExists, null, returnOld, "DuplicateItem"));
                    }
                    case Update up -> {
                        TableSchema s = store.describeTable(up.table());
                        KeyAndCond kc = primaryKeyOf(s, up.where());
                        Expr.Cond exists = new Expr.Fn("attribute_exists", List.of(new Expr.PathOp(Expr.Path.of(s.partitionKeyName()))));
                        Expr.Cond cond = kc.cond() == null ? exists : new Expr.And(List.of(exists, kc.cond()));
                        ops.add(new OperationHandlers.TxOp("Update", s, kc.key(), cond, new Expr.UpdatePlan(up.actions()), returnOld,
                                "ConditionalCheckFailed"));
                    }
                    case Delete del -> {
                        TableSchema s = store.describeTable(del.table());
                        KeyAndCond kc = primaryKeyOf(s, del.where());
                        ops.add(new OperationHandlers.TxOp("Delete", s, kc.key(), kc.cond(), null, returnOld, "ConditionalCheckFailed"));
                    }
                    default -> throw new IllegalStateException();
                }
            }
            handlers.executeTxWrites(ops, token, token == null ? null : hashOf(statements.toString()));
        }
        resp.add("Responses", responses);
        return resp;
    }

    // used by the unit tests
    static Object parseForTest(String statement, List<AttributeValue> params) {
        return new Parser(statement, params).statement();
    }
}
