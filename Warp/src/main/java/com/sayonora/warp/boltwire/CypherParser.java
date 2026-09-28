package com.sayonora.warp.boltwire;

import com.sayonora.warp.boltwire.Cy.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A hand-written Cypher lexer and recursive-descent parser for openCypher 9 with the Neo4j 5 additions Warp supports
 * (EXISTS/COUNT/COLLECT subqueries, CALL subqueries, IS NOT NULL, `n:A:B` label predicates, constraint / index DDL and SHOW
 * commands). It produces the tree in {@link Cy}; semantic checks live in {@link Analyzer}.
 */
final class CypherParser {

    private enum T { WORD, QUOTED, INT, FLOAT, STRING, PARAM, SYM, EOF }

    private record Tok(T type, String text, int start, int end) {
        boolean sym(String s) {
            return type == T.SYM && text.equals(s);
        }

        boolean kw(String k) {
            return type == T.WORD && text.equalsIgnoreCase(k);
        }
    }

    private final String src;
    private final List<Tok> toks = new ArrayList<>();
    private int p;

    private CypherParser(String src) {
        this.src = src;
        lex();
    }

    static Query parse(String text) {
        CypherParser ps = new CypherParser(text);
        return ps.parseStatement();
    }

    // ------------------------------------------------------------------------------------------ lexer

    private CypherException err(String msg, int pos) {
        int line = 1, col = 1;
        for (int i = 0; i < Math.min(pos, src.length()); i++) {
            if (src.charAt(i) == '\n') {
                line++;
                col = 1;
            } else {
                col++;
            }
        }
        return CypherException.syntax(msg + " (line " + line + ", column " + col + " (offset: " + pos + "))");
    }

    private CypherException unexpected(String expected) {
        Tok t = peek();
        String shown = t.type == T.EOF ? "end of input" : "'" + t.text + "'";
        return err("Invalid input " + shown + ": expected " + expected, t.start);
    }

    private void lex() {
        int i = 0, n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (Character.isWhitespace(c) || c == ' ' || c == '﻿') {
                i++;
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n' && src.charAt(i) != '\r') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int e = src.indexOf("*/", i + 2);
                if (e < 0) {
                    throw err("Invalid input: unterminated comment", i);
                }
                i = e + 2;
            } else if (Character.isLetter(c) || c == '_') {
                int s = i;
                while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                    i++;
                }
                toks.add(new Tok(T.WORD, src.substring(s, i), s, i));
            } else if (c == '`') {
                int s = i;
                StringBuilder sb = new StringBuilder();
                i++;
                while (true) {
                    if (i >= n) {
                        throw err("Invalid input: unterminated escaped identifier", s);
                    }
                    char d = src.charAt(i);
                    if (d == '`') {
                        if (i + 1 < n && src.charAt(i + 1) == '`') {
                            sb.append('`');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    sb.append(d);
                    i++;
                }
                toks.add(new Tok(T.QUOTED, sb.toString(), s, i));
            } else if (Character.isDigit(c) || (c == '.' && i + 1 < n && Character.isDigit(src.charAt(i + 1)))) {
                i = lexNumber(i);
            } else if (c == '\'' || c == '"') {
                i = lexString(i, c);
            } else if (c == '$') {
                int s = i;
                i++;
                if (i < n && src.charAt(i) == '`') {
                    int e = src.indexOf('`', i + 1);
                    toks.add(new Tok(T.PARAM, src.substring(i + 1, e), s, e + 1));
                    i = e + 1;
                } else {
                    int b = i;
                    while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                        i++;
                    }
                    if (b == i) {
                        throw err("Invalid input '$': expected a parameter name", s);
                    }
                    toks.add(new Tok(T.PARAM, src.substring(b, i), s, i));
                }
            } else {
                String two = i + 1 < n ? src.substring(i, i + 2) : "";
                if (two.equals("<>") || two.equals("<=") || two.equals(">=") || two.equals("=~") || two.equals("+=")
                        || two.equals("..")) {
                    toks.add(new Tok(T.SYM, two, i, i + 2));
                    i += 2;
                } else if ("()[]{},.:;|+-*/%^=<>&!".indexOf(c) >= 0) {
                    toks.add(new Tok(T.SYM, String.valueOf(c), i, i + 1));
                    i++;
                } else {
                    throw err("Invalid input '" + c + "'", i);
                }
            }
        }
        toks.add(new Tok(T.EOF, "", n, n));
    }

    private int lexNumber(int i) {
        int n = src.length(), s = i;
        if (src.charAt(i) == '0' && i + 1 < n && (src.charAt(i + 1) == 'x' || src.charAt(i + 1) == 'X')) {
            i += 2;
            int b = i;
            while (i < n && Character.digit(src.charAt(i), 16) >= 0) {
                i++;
            }
            if (b == i || (i < n && Character.isLetter(src.charAt(i)))) {
                throw err("Invalid input: invalid hexadecimal literal", s);
            }
            toks.add(new Tok(T.INT, src.substring(s, i), s, i));
            return i;
        }
        if (src.charAt(i) == '0' && i + 1 < n && (src.charAt(i + 1) == 'o' || src.charAt(i + 1) == 'O')) {
            i += 2;
            int b = i;
            while (i < n && src.charAt(i) >= '0' && src.charAt(i) <= '7') {
                i++;
            }
            if (b == i || (i < n && (Character.isLetterOrDigit(src.charAt(i))))) {
                throw err("Invalid input: invalid octal literal", s);
            }
            toks.add(new Tok(T.INT, src.substring(s, i), s, i));
            return i;
        }
        boolean isFloat = false;
        while (i < n && Character.isDigit(src.charAt(i))) {
            i++;
        }
        if (i < n && src.charAt(i) == '.' && i + 1 < n && Character.isDigit(src.charAt(i + 1))) {
            isFloat = true;
            i++;
            while (i < n && Character.isDigit(src.charAt(i))) {
                i++;
            }
        } else if (i < n && src.charAt(i) == '.' && !(i + 1 < n && (src.charAt(i + 1) == '.' || Character.isLetter(src.charAt(i + 1))
                || src.charAt(i + 1) == '_'))) {
            // "1." is a float literal
            isFloat = true;
            i++;
        }
        if (i < n && (src.charAt(i) == 'e' || src.charAt(i) == 'E')) {
            int j = i + 1;
            if (j < n && (src.charAt(j) == '+' || src.charAt(j) == '-')) {
                j++;
            }
            if (j < n && Character.isDigit(src.charAt(j))) {
                while (j < n && Character.isDigit(src.charAt(j))) {
                    j++;
                }
                i = j;
                isFloat = true;
            } else {
                throw err("Invalid input: invalid number literal", s);
            }
        }
        if (i < n && (Character.isLetter(src.charAt(i)) || src.charAt(i) == '_')) {
            throw err("Invalid input: invalid number literal", s);
        }
        toks.add(new Tok(isFloat ? T.FLOAT : T.INT, src.substring(s, i), s, i));
        return i;
    }

    private int lexString(int i, char q) {
        int n = src.length(), s = i;
        StringBuilder sb = new StringBuilder();
        i++;
        while (true) {
            if (i >= n) {
                throw err("Invalid input: unterminated string literal", s);
            }
            char c = src.charAt(i);
            if (c == q) {
                i++;
                break;
            }
            if (c == '\\') {
                if (i + 1 >= n) {
                    throw err("Invalid input: unterminated string literal", s);
                }
                char d = src.charAt(i + 1);
                i += 2;
                switch (d) {
                    case 'n' -> sb.append('\n');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case '\\' -> sb.append('\\');
                    case '\'' -> sb.append('\'');
                    case '"' -> sb.append('"');
                    case 'u', 'U' -> {
                        int len = d == 'u' ? 4 : 8;
                        if (i + len > n) {
                            throw err("Invalid input: invalid unicode escape", i - 2);
                        }
                        String hex = src.substring(i, i + len);
                        try {
                            sb.appendCodePoint(Integer.parseInt(hex, 16));
                        } catch (IllegalArgumentException e) {
                            throw err("Invalid input: invalid unicode escape", i - 2);
                        }
                        i += len;
                    }
                    default -> throw err("Invalid input '\\" + d + "': invalid escape sequence", i - 2);
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        toks.add(new Tok(T.STRING, sb.toString(), s, i));
        return i;
    }

    // ------------------------------------------------------------------------------------------ token helpers

    private Tok peek() {
        return toks.get(p);
    }

    private Tok peekAt(int k) {
        return toks.get(Math.min(p + k, toks.size() - 1));
    }

    private Tok next() {
        Tok t = toks.get(p);
        if (p < toks.size() - 1) {
            p++;
        }
        return t;
    }

    private boolean isSym(String s) {
        return peek().sym(s);
    }

    private boolean isKw(String k) {
        return peek().kw(k);
    }

    private boolean isKwAt(int k, String kw) {
        return peekAt(k).kw(kw);
    }

    private boolean acceptSym(String s) {
        if (isSym(s)) {
            next();
            return true;
        }
        return false;
    }

    private boolean acceptKw(String k) {
        if (isKw(k)) {
            next();
            return true;
        }
        return false;
    }

    private void expectSym(String s) {
        if (!acceptSym(s)) {
            throw unexpected("'" + s + "'");
        }
    }

    private void expectKw(String k) {
        if (!acceptKw(k)) {
            throw unexpected("'" + k + "'");
        }
    }

    /** A symbolic name: identifier, escaped identifier or (as a name) any keyword. */
    private String name() {
        Tok t = peek();
        if (t.type == T.WORD || t.type == T.QUOTED) {
            next();
            return t.text;
        }
        throw unexpected("an identifier");
    }

    private boolean isName() {
        return peek().type == T.WORD || peek().type == T.QUOTED;
    }

    private static final Set<String> RESERVED_AS_VAR = Set.of("MATCH", "RETURN", "WITH", "WHERE", "UNWIND", "CREATE",
            "MERGE", "DELETE", "SET", "REMOVE", "ORDER", "SKIP", "LIMIT", "UNION", "CALL", "YIELD", "AS", "DISTINCT",
            "AND", "OR", "XOR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "STARTS", "ENDS", "CONTAINS", "OPTIONAL", "DETACH", "ON", "FOREACH");

    private String variableName() {
        Tok t = peek();
        if (t.type == T.QUOTED) {
            next();
            return t.text;
        }
        if (t.type == T.WORD && !RESERVED_AS_VAR.contains(t.text.toUpperCase())) {
            next();
            return t.text;
        }
        throw unexpected("an identifier");
    }

    // ------------------------------------------------------------------------------------------ statements

    private Query parseStatement() {
        while (isKw("EXPLAIN") || isKw("PROFILE")) {
            next();
        }
        if (isKw("USE") && peekAt(1).type != T.SYM) {
            next();
            name();
            while (acceptSym(".")) {
                name();
            }
        }
        Clause schema = tryParseSchema();
        if (schema != null) {
            acceptSym(";");
            if (peek().type != T.EOF) {
                throw unexpected("end of input");
            }
            return new Query(List.of(List.of(schema)), List.of(), src);
        }
        List<List<Clause>> parts = new ArrayList<>();
        List<Boolean> all = new ArrayList<>();
        parts.add(parseSingleQuery());
        while (isKw("UNION")) {
            next();
            all.add(acceptKw("ALL"));
            if (!all.get(all.size() - 1)) {
                acceptKw("DISTINCT");
            }
            parts.add(parseSingleQuery());
        }
        acceptSym(";");
        if (peek().type != T.EOF) {
            throw unexpected("a clause, 'UNION' or end of input");
        }
        return new Query(parts, all, src);
    }

    private List<Clause> parseSingleQuery() {
        List<Clause> clauses = new ArrayList<>();
        while (true) {
            Tok t = peek();
            if (t.type == T.EOF || t.kw("UNION") || t.sym(";") || t.sym("}")) {
                break;
            }
            clauses.add(parseClause());
        }
        if (clauses.isEmpty()) {
            throw unexpected("a clause");
        }
        return clauses;
    }

    private Clause parseClause() {
        Tok t = peek();
        if (t.type != T.WORD) {
            throw unexpected("a clause");
        }
        String k = t.text.toUpperCase();
        switch (k) {
            case "OPTIONAL" -> {
                next();
                if (!isKw("MATCH")) {
                    throw unexpected("'MATCH'");
                }
                return parseMatch(true);
            }
            case "MATCH" -> {
                return parseMatch(false);
            }
            case "UNWIND" -> {
                next();
                Expr e = expr();
                expectKw("AS");
                return new Unwind(e, variableName());
            }
            case "WITH" -> {
                next();
                Projection pr = parseProjection(false);
                Expr where = acceptKw("WHERE") ? expr() : null;
                return new With(pr, where);
            }
            case "RETURN" -> {
                next();
                return new Return(parseProjection(true));
            }
            case "CREATE" -> {
                next();
                return new Create(parsePatternList());
            }
            case "MERGE" -> {
                next();
                PatternPart part = parsePatternPart(true);
                List<SetItem> onCreate = new ArrayList<>(), onMatch = new ArrayList<>();
                while (isKw("ON")) {
                    next();
                    boolean create;
                    if (acceptKw("CREATE")) {
                        create = true;
                    } else if (acceptKw("MATCH")) {
                        create = false;
                    } else {
                        throw unexpected("'MATCH' or 'CREATE'");
                    }
                    expectKw("SET");
                    (create ? onCreate : onMatch).addAll(parseSetItems());
                }
                return new Merge(part, onCreate, onMatch);
            }
            case "SET" -> {
                next();
                return new SetClause(parseSetItems());
            }
            case "REMOVE" -> {
                next();
                List<RemoveItem> items = new ArrayList<>();
                do {
                    Expr target = parsePostfix(false);
                    if (isSym(":")) {
                        items.add(new RemoveItem("LABELS", target, parseLabels()));
                    } else if (target instanceof Prop) {
                        items.add(new RemoveItem("PROP", target, null));
                    } else {
                        throw unexpected("a property or label to remove");
                    }
                } while (acceptSym(","));
                return new Remove(items);
            }
            case "DETACH", "DELETE" -> {
                boolean detach = acceptKw("DETACH");
                expectKw("DELETE");
                List<Expr> targets = new ArrayList<>();
                do {
                    targets.add(expr());
                } while (acceptSym(","));
                return new Delete(detach, targets);
            }
            case "CALL" -> {
                return parseCall();
            }
            case "FOREACH" -> {
                next();
                expectSym("(");
                String var = variableName();
                expectKw("IN");
                Expr list = expr();
                expectSym("|");
                List<Clause> body = new ArrayList<>();
                while (!isSym(")")) {
                    body.add(parseClause());
                }
                expectSym(")");
                return new Foreach(var, list, body);
            }
            default -> throw unexpected("a clause");
        }
    }

    private Match parseMatch(boolean optional) {
        int pos = peek().start;
        expectKw("MATCH");
        List<PatternPart> parts = parsePatternList();
        while (isKw("USING")) {
            next();
            // planner hints: USING INDEX [SEEK] v:Label(prop) | USING SCAN v:Label | USING JOIN ON v[, w]
            int depth = 0;
            while (peek().type != T.EOF && !(depth == 0 && (isKw("WHERE") || isKw("USING") || isKw("MATCH")
                    || isKw("RETURN") || isKw("WITH") || isKw("OPTIONAL") || isKw("UNWIND") || isKw("CREATE")
                    || isKw("MERGE") || isKw("SET") || isKw("DELETE") || isKw("REMOVE") || isKw("CALL")))) {
                if (isSym("(")) {
                    depth++;
                } else if (isSym(")")) {
                    depth--;
                }
                next();
            }
        }
        Expr where = acceptKw("WHERE") ? expr() : null;
        return new Match(optional, parts, where, pos);
    }

    private List<SetItem> parseSetItems() {
        List<SetItem> items = new ArrayList<>();
        do {
            Expr target = parsePostfix(false);
            if (isSym(":") && target instanceof Var) {
                items.add(new SetItem("LABELS", target, null, parseLabels()));
            } else if (acceptSym("=")) {
                Expr v = expr();
                items.add(target instanceof Prop ? new SetItem("PROP", target, v, null) : new SetItem("REPLACE", target, v, null));
            } else if (acceptSym("+=")) {
                items.add(new SetItem("MERGE", target, expr(), null));
            } else {
                throw unexpected("'=', '+=' or a label");
            }
        } while (acceptSym(","));
        return items;
    }

    private Clause parseCall() {
        expectKw("CALL");
        if (isSym("{")) {
            next();
            Query q = parseSubquery();
            return new CallSubquery(q);
        }
        StringBuilder nm = new StringBuilder(name());
        while (acceptSym(".")) {
            nm.append('.').append(name());
        }
        List<Expr> args = null;
        if (acceptSym("(")) {
            args = new ArrayList<>();
            if (!isSym(")")) {
                do {
                    args.add(expr());
                } while (acceptSym(","));
            }
            expectSym(")");
        }
        List<YieldItem> yields = null;
        boolean star = false;
        Expr where = null;
        if (acceptKw("YIELD")) {
            yields = new ArrayList<>();
            if (acceptSym("*")) {
                star = true;
            } else {
                do {
                    String n = name();
                    String alias = acceptKw("AS") ? variableName() : null;
                    yields.add(new YieldItem(n, alias));
                } while (acceptSym(","));
            }
            if (acceptKw("WHERE")) {
                where = expr();
            }
        }
        boolean standalone = false;
        Tok t = peek();
        if (t.type == T.EOF || t.kw("UNION") || t.sym(";") || t.sym("}")) {
            standalone = toks.indexOf(toks.get(0)) == 0 && isFirstClause(nm.toString());
        }
        return new CallClause(nm.toString(), args, yields, star, where, standalone);
    }

    private boolean isFirstClause(String name) {
        // a CALL is "standalone" when it is the only clause of the statement; the Analyzer re-derives this exactly,
        // this flag is only a hint for the parser's own implicit-argument acceptance
        return true;
    }

    private Query parseSubquery() {
        int start = peek().start;
        List<List<Clause>> parts = new ArrayList<>();
        List<Boolean> all = new ArrayList<>();
        parts.add(parseSingleQuery());
        while (isKw("UNION")) {
            next();
            all.add(acceptKw("ALL"));
            parts.add(parseSingleQuery());
        }
        int end = peek().start;
        expectSym("}");
        return new Query(parts, all, src.substring(start, end));
    }

    // ------------------------------------------------------------------------------------------ schema commands

    private Clause tryParseSchema() {
        int save = p;
        String startText = src.substring(peek().start);
        try {
            if (isKw("SHOW")) {
                next();
                StringBuilder what = new StringBuilder();
                while (peek().type == T.WORD && !isKw("YIELD") && !isKw("WHERE") && !isKw("RETURN")) {
                    what.append(what.length() > 0 ? " " : "").append(next().text.toUpperCase());
                }
                List<String> yields = null;
                List<String> options = new ArrayList<>();
                String where = null;
                Expr whereExpr = null;
                if (acceptKw("YIELD")) {
                    yields = new ArrayList<>();
                    if (acceptSym("*")) {
                        yields = null;
                    } else {
                        do {
                            String n = name();
                            if (acceptKw("AS")) {
                                n = n + " AS " + name();
                            }
                            yields.add(n);
                        } while (acceptSym(","));
                    }
                }
                if (isKw("WHERE") || isKw("RETURN") || isKw("ORDER") || isKw("SKIP") || isKw("LIMIT")) {
                    // SHOW ... [YIELD ...] WHERE/ORDER/RETURN: keep the tail as text; the executor re-runs it
                    // through the row pipeline on top of the SHOW result.
                    options.add(src.substring(peek().start).replaceAll(";\\s*$", ""));
                    p = toks.size() - 1;
                }
                return new SchemaCmd("SHOW", null, false, false, null, null, yields, null, null, what.toString(), options,
                        startText);
            }
            if (isKw("DROP") && (isKwAt(1, "CONSTRAINT") || isKwAt(1, "INDEX"))) {
                next();
                String kind = next().text.toUpperCase();
                String nm = name();
                boolean ifEx = false;
                if (acceptKw("IF")) {
                    expectKw("EXISTS");
                    ifEx = true;
                }
                return new SchemaCmd("DROP_" + kind, nm, false, ifEx, null, null, null, null, null, null, null, startText);
            }
            if (isKw("CREATE") || isKw("DROP")) {
                int q = p + 1;
                boolean orReplace = false;
                if (toks.get(q).kw("OR")) {
                    q += 2;
                    orReplace = true;
                }
                Tok t = toks.get(q);
                Tok t2 = toks.get(Math.min(q + 1, toks.size() - 1));
                boolean ddl = t.kw("CONSTRAINT") || t.kw("INDEX") || ((t.kw("RANGE") || t.kw("TEXT") || t.kw("POINT")
                        || t.kw("LOOKUP") || t.kw("FULLTEXT") || t.kw("VECTOR")) && t2.kw("INDEX"));
                if (ddl) {
                    p = q;
                    return parseCreateSchema();
                }
            }
        } catch (CypherException e) {
            if (isKw("SHOW")) {
                throw e;
            }
            p = save;
            throw e;
        }
        p = save;
        return null;
    }

    private Clause parseCreateSchema() {
        String startText = src.substring(toks.get(0).start);
        String indexType = "RANGE";
        boolean isConstraint = false;
        if (isKw("CONSTRAINT")) {
            next();
            isConstraint = true;
        } else {
            if (!isKw("INDEX")) {
                indexType = next().text.toUpperCase();
            }
            expectKw("INDEX");
        }
        String nm = null;
        boolean ifNot = false;
        if (!isKw("FOR") && !isKw("IF") && !isKw("ON")) {
            nm = name();
        }
        if (acceptKw("IF")) {
            expectKw("NOT");
            expectKw("EXISTS");
            ifNot = true;
        }
        String entity = "NODE", label = null;
        List<String> props = new ArrayList<>();
        String ctype = null;
        if (acceptKw("FOR")) {
            expectSym("(");
            String var = isName() && !isSym(":") ? variableName() : null;
            if (acceptSym(":")) {
                label = name();
            }
            if (isSym(")")) {
                next();
            } else {
                throw unexpected("')'");
            }
            if (isKw("REQUIRE")) {
                // constraint: REQUIRE n.p IS UNIQUE | IS NOT NULL | IS NODE KEY | IS :: TYPE
                next();
                boolean paren = acceptSym("(");
                do {
                    Expr e = parsePostfix(false);
                    props.add(propKey(e));
                } while (acceptSym(","));
                if (paren) {
                    expectSym(")");
                }
                expectKw("IS");
                if (acceptKw("UNIQUE")) {
                    ctype = "UNIQUENESS";
                } else if (acceptKw("NOT")) {
                    expectKw("NULL");
                    ctype = "EXISTENCE";
                } else if (acceptKw("NODE")) {
                    expectKw("KEY");
                    ctype = "NODE_KEY";
                } else if (acceptKw("RELATIONSHIP")) {
                    expectKw("KEY");
                    ctype = "REL_KEY";
                } else {
                    // IS :: TYPE
                    while (peek().type != T.EOF && !isSym(";")) {
                        next();
                    }
                    ctype = "PROPERTY_TYPE";
                }
            } else if (isKw("ON")) {
                next();
                expectSym("(");
                do {
                    Expr e = parsePostfix(false);
                    props.add(propKey(e));
                } while (acceptSym(","));
                expectSym(")");
            }
        }
        List<String> opts = new ArrayList<>();
        if (acceptKw("OPTIONS")) {
            while (peek().type != T.EOF && !isSym(";")) {
                next();
            }
        }
        if (indexType.equals("LOOKUP") || (label == null && !isConstraint)) {
            // relationship pattern form: FOR ()-[r:T]-() ON (r.p) is rejected below
        }
        acceptSym(";");
        if (peek().type != T.EOF) {
            throw unexpected("end of input");
        }
        return new SchemaCmd(isConstraint ? "CREATE_CONSTRAINT" : "CREATE_INDEX", nm, ifNot, false, entity, label, props,
                ctype, indexType, null, opts, startText);
    }

    private String propKey(Expr e) {
        if (e instanceof Prop pr) {
            return pr.key();
        }
        throw err("Invalid input: expected a property expression", peek().start);
    }

    // ------------------------------------------------------------------------------------------ projection

    private Projection parseProjection(boolean isReturn) {
        boolean distinct = acceptKw("DISTINCT");
        boolean star = false;
        List<ProjItem> items = new ArrayList<>();
        if (acceptSym("*")) {
            star = true;
            if (acceptSym(",")) {
                parseItems(items);
            }
        } else {
            parseItems(items);
        }
        List<SortItem> order = new ArrayList<>();
        if (isKw("ORDER")) {
            next();
            expectKw("BY");
            do {
                Expr e = expr();
                boolean desc = false;
                if (acceptKw("DESC") || acceptKw("DESCENDING")) {
                    desc = true;
                } else if (acceptKw("ASC") || acceptKw("ASCENDING")) {
                    desc = false;
                }
                order.add(new SortItem(e, desc));
            } while (acceptSym(","));
        }
        Expr skip = null, limit = null;
        if (isKw("SKIP") || isKw("OFFSET")) {
            next();
            skip = expr();
        }
        if (acceptKw("LIMIT")) {
            limit = expr();
        }
        return new Projection(distinct, star, items, order, skip, limit);
    }

    private void parseItems(List<ProjItem> items) {
        do {
            int s = peek().start;
            Expr e = expr();
            int end = toks.get(p - 1).end;
            String text = src.substring(s, end);
            String alias = null;
            if (acceptKw("AS")) {
                alias = variableName();
            }
            items.add(new ProjItem(e, alias, text));
        } while (acceptSym(","));
    }

    // ------------------------------------------------------------------------------------------ patterns

    private List<PatternPart> parsePatternList() {
        List<PatternPart> parts = new ArrayList<>();
        do {
            parts.add(parsePatternPart(false));
        } while (acceptSym(","));
        return parts;
    }

    private PatternPart parsePatternPart(boolean single) {
        String pathVar = null;
        if (isName() && peekAt(1).sym("=") ) {
            pathVar = variableName();
            expectSym("=");
        }
        int shortest = 0;
        if ((isKw("shortestPath") || isKw("allShortestPaths")) && peekAt(1).sym("(")) {
            shortest = isKw("shortestPath") ? 1 : 2;
            next();
            next();
            PatternPart inner = parsePatternChain(pathVar, shortest);
            expectSym(")");
            return inner;
        }
        return parsePatternChain(pathVar, 0);
    }

    private PatternPart parsePatternChain(String pathVar, int shortest) {
        List<Object> els = new ArrayList<>();
        els.add(parseNodePattern());
        while (isSym("-") || (isSym("<") && peekAt(1).sym("-"))) {
            els.add(parseRelPattern());
            els.add(parseNodePattern());
        }
        return new PatternPart(pathVar, els, shortest);
    }

    private NodePat parseNodePattern() {
        int pos = peek().start;
        expectSym("(");
        String var = null;
        if (isName() && !isSym(":")) {
            var = variableName();
        }
        List<String> labels = new ArrayList<>();
        if (isSym(":")) {
            labels = parseLabels();
        }
        Expr props = null;
        if (isSym("{")) {
            props = parseMapLiteral();
        } else if (peek().type == T.PARAM) {
            Tok t = next();
            props = new Param(t.text, t.start);
        }
        expectSym(")");
        return new NodePat(var, labels, props, pos);
    }

    private List<String> parseLabels() {
        List<String> labels = new ArrayList<>();
        while (acceptSym(":")) {
            labels.add(name());
            while (isSym("&")) {
                next();
                labels.add(name());
            }
        }
        return labels;
    }

    private RelPat parseRelPattern() {
        int pos = peek().start;
        boolean left = false, right = false;
        if (isSym("<")) {
            next();
            left = true;
        }
        expectSym("-");
        String var = null;
        List<String> types = new ArrayList<>();
        Expr props = null;
        boolean varLen = false;
        long min = 1, max = 1;
        if (acceptSym("[")) {
            if (isName() && !isSym(":")) {
                var = variableName();
            }
            if (acceptSym(":")) {
                types.add(name());
                while (isSym("|")) {
                    next();
                    acceptSym(":");
                    types.add(name());
                }
            }
            if (acceptSym("*")) {
                varLen = true;
                min = 1;
                max = -1;
                if (peek().type == T.INT) {
                    min = parseRangeInt();
                    max = min;
                    if (acceptSym("..")) {
                        max = peek().type == T.INT ? parseRangeInt() : -1;
                    }
                } else if (acceptSym("..")) {
                    min = 1;
                    max = peek().type == T.INT ? parseRangeInt() : -1;
                }
            }
            if (isSym("{")) {
                props = parseMapLiteral();
            } else if (peek().type == T.PARAM) {
                Tok t = next();
                props = new Param(t.text, t.start);
            }
            expectSym("]");
        }
        expectSym("-");
        if (isSym(">")) {
            next();
            right = true;
        }
        if (left && right) {
            // <-->: Cypher treats it as undirected; Neo4j accepts it
            left = right = false;
        }
        Dir d = left ? Dir.IN : right ? Dir.OUT : Dir.BOTH;
        return new RelPat(var, types, props, d, varLen, min, max, pos);
    }

    private long parseRangeInt() {
        Tok t = next();
        try {
            return Long.parseLong(t.text);
        } catch (NumberFormatException e) {
            throw err("Invalid input: invalid range bound", t.start);
        }
    }

    // ------------------------------------------------------------------------------------------ expressions

    Expr expr() {
        return parseOr();
    }

    private Expr parseOr() {
        Expr l = parseXor();
        while (isKw("OR")) {
            next();
            l = new Or(l, parseXor());
        }
        return l;
    }

    private Expr parseXor() {
        Expr l = parseAnd();
        while (isKw("XOR")) {
            next();
            l = new Xor(l, parseAnd());
        }
        return l;
    }

    private Expr parseAnd() {
        Expr l = parseNot();
        while (isKw("AND")) {
            next();
            l = new And(l, parseNot());
        }
        return l;
    }

    private Expr parseNot() {
        if (isKw("NOT")) {
            next();
            return new Not(parseNot());
        }
        return parseComparison();
    }

    /** Comparison operators are looser than the string / list / null operators (IN, STARTS WITH, IS NULL, =~ ...). */
    private Expr parseComparison() {
        Expr left = parseStringListNull();
        Expr result = null;
        Expr prev = left;
        while (true) {
            Tok t = peek();
            if (t.type == T.SYM && (t.text.equals("=") || t.text.equals("<>") || t.text.equals("<") || t.text.equals(">")
                    || t.text.equals("<=") || t.text.equals(">="))) {
                next();
                Expr r = parseStringListNull();
                Expr cmp = new Cmp(t.text, prev, r);
                result = result == null ? cmp : new And(result, cmp);
                prev = r;
            } else if (t.sym("!") && peekAt(1).sym("=")) {
                throw unexpected("an expression");
            } else {
                break;
            }
        }
        return result == null ? left : result;
    }

    private Expr parseStringListNull() {
        Expr l = parseAddSub();
        boolean inUsed = false;
        while (true) {
            Tok t = peek();
            if (t.sym("=~")) {
                next();
                l = new RegexMatch(l, parseAddSub());
            } else if (t.kw("IN")) {
                if (inUsed) {
                    throw unexpected("an expression");
                }
                next();
                l = new InList(l, parseAddSub());
                inUsed = true;
            } else if (t.kw("STARTS") && isKwAt(1, "WITH")) {
                next();
                next();
                l = new StrOp("STARTS", l, parseAddSub());
            } else if (t.kw("ENDS") && isKwAt(1, "WITH")) {
                next();
                next();
                l = new StrOp("ENDS", l, parseAddSub());
            } else if (t.kw("CONTAINS")) {
                next();
                l = new StrOp("CONTAINS", l, parseAddSub());
            } else if (t.kw("IS")) {
                next();
                boolean neg = acceptKw("NOT");
                if (!acceptKw("NULL")) {
                    throw unexpected("'NULL'");
                }
                l = new IsNull(l, neg);
            } else {
                return l;
            }
        }
    }

    private Expr parseAddSub() {
        Expr l = parseMulDiv();
        while (isSym("+") || (isSym("-") && !arrowAhead())) {
            char op = next().text.charAt(0);
            l = new Binary(op, l, parseMulDiv());
        }
        return l;
    }

    /** True when the tokens at the cursor start a relationship pattern continuation ("-[", "--", "->") rather than a
     * subtraction: only relevant right after a pattern predicate, which the atom parser consumes itself. */
    private boolean arrowAhead() {
        return false;
    }

    private Expr parseMulDiv() {
        Expr l = parsePower();
        while (isSym("*") || isSym("/") || isSym("%")) {
            char op = next().text.charAt(0);
            l = new Binary(op, l, parsePower());
        }
        return l;
    }

    private Expr parsePower() {
        Expr l = parseUnary();
        while (isSym("^")) {
            next();
            l = new Binary('^', l, parseUnary());
        }
        return l;
    }

    private Expr parseUnary() {
        if (isSym("-")) {
            Tok minus = next();
            Tok t = peek();
            if (t.type == T.INT && t.start == minus.end) {
                next();
                Lit lit = new Lit(parseIntLiteral(t, true));
                return parsePostfixOn(lit, true);
            }
            return new Unary('-', parseUnary());
        }
        if (isSym("+")) {
            next();
            return new Unary('+', parseUnary());
        }
        return parsePostfix(true);
    }

    private long parseIntLiteral(Tok t, boolean negative) {
        String s = t.text;
        try {
            if (s.startsWith("0x") || s.startsWith("0X")) {
                java.math.BigInteger v = new java.math.BigInteger(s.substring(2), 16);
                return bigToLong(negative ? v.negate() : v, t);
            }
            if (s.startsWith("0o") || s.startsWith("0O")) {
                java.math.BigInteger v = new java.math.BigInteger(s.substring(2), 8);
                return bigToLong(negative ? v.negate() : v, t);
            }
            if (s.length() > 1 && s.startsWith("0")) {
                return Long.parseLong((negative ? "-" : "") + s.substring(1), 8);
            }
            return Long.parseLong((negative ? "-" : "") + s);
        } catch (NumberFormatException e) {
            throw err("integer is too large", t.start);
        }
    }

    private long bigToLong(java.math.BigInteger v, Tok t) {
        if (v.bitLength() > 63) {
            throw err("integer is too large", t.start);
        }
        return v.longValue();
    }

    private Expr parsePostfix(boolean allowLabels) {
        Expr atom = parseAtom();
        return parsePostfixOn(atom, allowLabels);
    }

    private Expr parsePostfixOn(Expr e, boolean allowLabels) {
        while (true) {
            if (isSym(".") ) {
                Tok dot = next();
                Tok k = peek();
                if (!(k.type == T.WORD || k.type == T.QUOTED)) {
                    throw unexpected("a property key name");
                }
                next();
                e = new Prop(e, k.text, dot.start);
            } else if (isSym("[")) {
                next();
                if (acceptSym("..")) {
                    Expr to = isSym("]") ? null : expr();
                    expectSym("]");
                    e = new Slice(e, null, to);
                } else {
                    Expr idx = expr();
                    if (acceptSym("..")) {
                        Expr to = isSym("]") ? null : expr();
                        expectSym("]");
                        e = new Slice(e, idx, to);
                    } else {
                        expectSym("]");
                        e = new Index(e, idx);
                    }
                }
            } else if (allowLabels && isSym(":") && !peekAt(1).sym(":") && (peekAt(1).type == T.WORD || peekAt(1).type == T.QUOTED)) {
                e = new HasLabels(e, parseLabels());
            } else {
                return e;
            }
        }
    }

    private Expr parseAtom() {
        Tok t = peek();
        switch (t.type) {
            case INT -> {
                next();
                return new Lit(parseIntLiteral(t, false));
            }
            case FLOAT -> {
                next();
                double d;
                try {
                    d = Double.parseDouble(t.text);
                } catch (NumberFormatException e) {
                    throw err("Invalid input: invalid number literal", t.start);
                }
                if (Double.isInfinite(d)) {
                    throw err("floating point number is too large", t.start);
                }
                return new Lit(d);
            }
            case STRING -> {
                next();
                return new Lit(t.text);
            }
            case PARAM -> {
                next();
                return new Param(t.text, t.start);
            }
            case SYM -> {
                return parseSymbolAtom(t);
            }
            case EOF -> throw unexpected("an expression");
            default -> {
            }
        }
        // words
        if (t.type == T.WORD) {
            String up = t.text.toUpperCase();
            switch (up) {
                case "TRUE" -> {
                    next();
                    return new Lit(Boolean.TRUE);
                }
                case "FALSE" -> {
                    next();
                    return new Lit(Boolean.FALSE);
                }
                case "NULL" -> {
                    next();
                    return new Lit(null);
                }
                case "CASE" -> {
                    return parseCase();
                }
                case "COUNT" -> {
                    if (peekAt(1).sym("(") && peekAt(2).sym("*")) {
                        next();
                        next();
                        next();
                        expectSym(")");
                        return new CountStar();
                    }
                    if (peekAt(1).sym("{")) {
                        next();
                        next();
                        return parseSubqueryExpr("COUNT");
                    }
                }
                case "EXISTS" -> {
                    if (peekAt(1).sym("{")) {
                        next();
                        next();
                        return parseSubqueryExpr("EXISTS");
                    }
                }
                case "COLLECT" -> {
                    if (peekAt(1).sym("{")) {
                        next();
                        next();
                        return parseSubqueryExpr("COLLECT");
                    }
                }
                case "ALL", "ANY", "NONE", "SINGLE" -> {
                    if (peekAt(1).sym("(") && peekAt(2).type != T.EOF && (peekAt(2).type == T.WORD || peekAt(2).type == T.QUOTED)
                            && isKwAt(3, "IN")) {
                        next();
                        next();
                        String v = variableName();
                        expectKw("IN");
                        Expr list = expr();
                        Expr where = acceptKw("WHERE") ? expr() : null;
                        expectSym(")");
                        return new Quantifier(up, v, list, where);
                    }
                }
                case "REDUCE" -> {
                    if (peekAt(1).sym("(")) {
                        next();
                        next();
                        String acc = variableName();
                        expectSym("=");
                        Expr init = expr();
                        expectSym(",");
                        String v = variableName();
                        expectKw("IN");
                        Expr list = expr();
                        expectSym("|");
                        Expr body = expr();
                        expectSym(")");
                        return new Reduce(acc, init, v, list, body);
                    }
                }
                case "SHORTESTPATH", "ALLSHORTESTPATHS" -> {
                    if (peekAt(1).sym("(")) {
                        PatternPart part = parsePatternPart(false);
                        return new PathFn(part);
                    }
                }
                default -> {
                }
            }
            // function call?  ns.name(
            int save = p;
            StringBuilder nm = new StringBuilder(t.text);
            int k = 1;
            while (peekAt(k).sym(".") && (peekAt(k + 1).type == T.WORD || peekAt(k + 1).type == T.QUOTED)) {
                nm.append('.').append(peekAt(k + 1).text);
                k += 2;
            }
            if (peekAt(k).sym("(")) {
                p += k + 1;
                boolean distinct = acceptKw("DISTINCT");
                List<Expr> args = new ArrayList<>();
                if (!isSym(")")) {
                    do {
                        args.add(expr());
                    } while (acceptSym(","));
                }
                expectSym(")");
                return new Call(nm.toString(), distinct, args, t.start);
            }
            p = save;
            // plain variable, possibly with a map projection
            next();
            Var v = new Var(t.text, t.start);
            if (isSym("{") && looksLikeMapProjection()) {
                return parseMapProjection(v);
            }
            return v;
        }
        if (t.type == T.QUOTED) {
            next();
            Var v = new Var(t.text, t.start);
            if (isSym("{") && looksLikeMapProjection()) {
                return parseMapProjection(v);
            }
            return v;
        }
        throw unexpected("an expression");
    }

    private boolean looksLikeMapProjection() {
        Tok a = peekAt(1);
        if (a.sym("}") || a.sym(".")) {
            return true;
        }
        Tok b = peekAt(2);
        return (a.type == T.WORD || a.type == T.QUOTED) && (b.sym(":") || b.sym(",") || b.sym("}"));
    }

    private Expr parseMapProjection(Expr target) {
        expectSym("{");
        List<String> kinds = new ArrayList<>(), keys = new ArrayList<>();
        List<Expr> values = new ArrayList<>();
        if (!isSym("}")) {
            do {
                if (acceptSym(".")) {
                    if (acceptSym("*")) {
                        kinds.add("ALL");
                        keys.add(null);
                        values.add(null);
                    } else {
                        String k = name();
                        kinds.add("PROP");
                        keys.add(k);
                        values.add(null);
                    }
                } else {
                    String k = name();
                    if (acceptSym(":")) {
                        kinds.add("VALUE");
                        keys.add(k);
                        values.add(expr());
                    } else {
                        kinds.add("VAR");
                        keys.add(k);
                        values.add(new Var(k, peek().start));
                    }
                }
            } while (acceptSym(","));
        }
        expectSym("}");
        return new MapProj(target, kinds, keys, values);
    }

    private Expr parseSubqueryExpr(String kind) {
        // after "{": either a pattern [WHERE] or a full query
        int save = p;
        boolean patternForm = isSym("(") || (isName() && peekAt(1).sym("="));
        if (patternForm && !isKw("RETURN") && !isKw("MATCH")) {
            try {
                List<PatternPart> parts = new ArrayList<>();
                do {
                    parts.add(parsePatternPart(false));
                } while (acceptSym(","));
                Expr where = acceptKw("WHERE") ? expr() : null;
                expectSym("}");
                return kind.equals("EXISTS") ? new ExistsSub(parts, where, null) : new CountSub(parts, where, null);
            } catch (CypherException e) {
                p = save;
            }
        }
        if (isKw("MATCH") || isKw("OPTIONAL") || isKw("WITH") || isKw("UNWIND") || isKw("RETURN") || isKw("CALL")
                || isKw("CREATE") || isKw("MERGE")) {
            Query q = parseSubquery();
            return switch (kind) {
                case "EXISTS" -> new ExistsSub(null, null, q);
                case "COUNT" -> new CountSub(null, null, q);
                default -> new CollectSub(q);
            };
        }
        throw unexpected("a pattern or a query");
    }

    private Expr parseCase() {
        expectKw("CASE");
        Expr subject = null;
        if (!isKw("WHEN")) {
            subject = expr();
        }
        List<Expr> whens = new ArrayList<>(), thens = new ArrayList<>();
        while (acceptKw("WHEN")) {
            whens.add(expr());
            expectKw("THEN");
            thens.add(expr());
        }
        if (whens.isEmpty()) {
            throw unexpected("'WHEN'");
        }
        Expr other = acceptKw("ELSE") ? expr() : null;
        expectKw("END");
        return new CaseExpr(subject, whens, thens, other);
    }

    private Expr parseSymbolAtom(Tok t) {
        if (t.sym("(")) {
            // pattern predicate or parenthesised expression
            int save = p;
            try {
                NodePat first = parseNodePattern();
                if (isSym("-") || (isSym("<") && peekAt(1).sym("-"))) {
                    Tok a = peekAt(isSym("<") ? 2 : 1);
                    boolean arrow = isSym("<") || a.sym("[") || a.sym("-") || a.sym(">");
                    if (arrow) {
                        p = save;
                        PatternPart part = parsePatternChain(null, 0);
                        if (part.relCount() == 0) {
                            p = save;
                        } else {
                            return new PatternPred(part);
                        }
                    }
                }
            } catch (CypherException e) {
                // not a pattern
            }
            p = save;
            next();
            Expr e = expr();
            expectSym(")");
            return e;
        }
        if (t.sym("[")) {
            return parseListOrComprehension();
        }
        if (t.sym("{")) {
            return parseMapLiteral();
        }
        throw unexpected("an expression");
    }

    private Expr parseListOrComprehension() {
        expectSym("[");
        if (acceptSym("]")) {
            return new ListLit(List.of());
        }
        // list comprehension: [x IN list ...]
        if ((peek().type == T.WORD || peek().type == T.QUOTED) && isKwAt(1, "IN")) {
            int save = p;
            String v = variableName();
            next();
            Expr list = expr();
            Expr where = acceptKw("WHERE") ? expr() : null;
            Expr map = acceptSym("|") ? expr() : null;
            if (isSym("]")) {
                next();
                return new ListComp(v, list, where, map);
            }
            p = save;
        }
        // pattern comprehension: [ (a)-->(b) WHERE .. | expr ]  or  [ p = (a)-->(b) | expr ]
        if (isSym("(") || (isName() && peekAt(1).sym("=") && peekAt(2).sym("("))) {
            int save = p;
            try {
                PatternPart part = parsePatternPart(false);
                if (part.relCount() > 0) {
                    Expr where = acceptKw("WHERE") ? expr() : null;
                    expectSym("|");
                    Expr map = expr();
                    expectSym("]");
                    return new PatternComp(part, where, map);
                }
            } catch (CypherException e) {
                // fall through to a plain list
            }
            p = save;
        }
        List<Expr> items = new ArrayList<>();
        do {
            items.add(expr());
        } while (acceptSym(","));
        expectSym("]");
        return new ListLit(items);
    }

    private MapLit parseMapLiteral() {
        expectSym("{");
        List<String> keys = new ArrayList<>();
        List<Expr> values = new ArrayList<>();
        if (!isSym("}")) {
            do {
                keys.add(name());
                expectSym(":");
                values.add(expr());
            } while (acceptSym(","));
        }
        expectSym("}");
        return new MapLit(keys, values);
    }
}
