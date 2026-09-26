package com.sayonora.wire.gremlinwire;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A Groovy-subset interpreter for Gremlin scripts: literals, variables, arithmetic, collections, closures and method-call chains
 * that build {@link G.Bytecode} (executed by {@link Engine}) when a terminal method ({@code toList()}, {@code next()}, ...) is called
 * or when the script's value is itself a traversal. Anything outside that subset is reported as a script evaluation error.
 */
final class Script {

    private Script() {
    }

    // ================================================================== AST

    interface Node {
    }

    record Lit(Object v) implements Node {
    }

    record Var(String name) implements Node {
    }

    record ListLit(List<Node> items) implements Node {
    }

    record MapLit(List<Node[]> entries) implements Node {
    }

    record Call(Node target, String name, List<Node> args, boolean safe) implements Node {
    }

    record PropAccess(Node target, String name, boolean safe) implements Node {
    }

    record Index(Node target, Node index) implements Node {
    }

    record Bin(String op, Node l, Node r) implements Node {
    }

    record Un(String op, Node e) implements Node {
    }

    record Assign(Node target, String op, Node value) implements Node {
    }

    record Ternary(Node c, Node a, Node b) implements Node {
    }

    record ClosureNode(List<String> params, Block body) implements Node {
    }

    record New(String cls, List<Node> args) implements Node {
    }

    record RangeNode(Node l, Node r) implements Node {
    }

    record Block(List<Node> stmts) implements Node {
    }

    record If(Node c, Node a, Node b) implements Node {
    }

    record ForIn(String var, Node iter, Node body) implements Node {
    }

    record While(Node c, Node body) implements Node {
    }

    record Return(Node e) implements Node {
    }

    record DefFunc(String name, List<String> params, Block body) implements Node {
    }

    record GString(List<Object> parts) implements Node {
    }

    // ================================================================== lexer

    private enum K { NUM, STR, GSTR, ID, OP, NL, EOF }

    private record Tok(K k, String s, Object v, int pos) {
    }

    private static List<Tok> lex(String src) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '\n') {
                out.add(new Tok(K.NL, "\n", null, i));
                i++;
                continue;
            }
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int e = src.indexOf("*/", i + 2);
                i = e < 0 ? n : e + 2;
                continue;
            }
            if (Character.isDigit(c) || (c == '.' && i + 1 < n && Character.isDigit(src.charAt(i + 1)) && (out.isEmpty() || !isOperandEnd(out.get(out.size() - 1))))) {
                int st = i;
                if (c == '0' && i + 1 < n && (src.charAt(i + 1) == 'x' || src.charAt(i + 1) == 'X')) {
                    i += 2;
                    while (i < n && Character.digit(src.charAt(i), 16) >= 0) {
                        i++;
                    }
                    long v = Long.parseLong(src.substring(st + 2, i), 16);
                    if (i < n && (src.charAt(i) == 'L' || src.charAt(i) == 'l')) {
                        i++;
                        out.add(new Tok(K.NUM, src.substring(st, i), v, st));
                    } else {
                        out.add(new Tok(K.NUM, src.substring(st, i), v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE ? (Object) (int) v : (Object) v, st));
                    }
                    continue;
                }
                boolean dec = false;
                while (i < n && (Character.isDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                    i++;
                }
                if (i + 1 < n && src.charAt(i) == '.' && Character.isDigit(src.charAt(i + 1))) {
                    dec = true;
                    i++;
                    while (i < n && (Character.isDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                        i++;
                    }
                }
                if (i < n && (src.charAt(i) == 'e' || src.charAt(i) == 'E') && i + 1 < n
                        && (Character.isDigit(src.charAt(i + 1)) || ((src.charAt(i + 1) == '-' || src.charAt(i + 1) == '+') && i + 2 < n && Character.isDigit(src.charAt(i + 2))))) {
                    dec = true;
                    i += 2;
                    while (i < n && Character.isDigit(src.charAt(i))) {
                        i++;
                    }
                }
                String num = src.substring(st, i).replace("_", "");
                Object v;
                char suf = i < n ? Character.toLowerCase(src.charAt(i)) : ' ';
                if (suf == 'l' && !dec) {
                    i++;
                    v = Long.parseLong(num);
                } else if (suf == 'f') {
                    i++;
                    v = Float.parseFloat(num);
                } else if (suf == 'd') {
                    i++;
                    v = Double.parseDouble(num);
                } else if (suf == 'g') {
                    i++;
                    v = dec ? (Object) new BigDecimal(num) : (Object) new BigInteger(num);
                } else if (suf == 'i' && !dec) {
                    i++;
                    v = Integer.parseInt(num);
                } else if (dec) {
                    v = new BigDecimal(num);
                } else {
                    BigInteger b = new BigInteger(num);
                    v = b.bitLength() < 32 ? (Object) b.intValue() : b.bitLength() < 64 ? (Object) b.longValue() : (Object) b;
                }
                out.add(new Tok(K.NUM, num, v, st));
                continue;
            }
            if (Character.isJavaIdentifierStart(c)) {
                int st = i;
                while (i < n && Character.isJavaIdentifierPart(src.charAt(i))) {
                    i++;
                }
                out.add(new Tok(K.ID, src.substring(st, i), null, st));
                continue;
            }
            if (c == '\'' || c == '"') {
                int st = i;
                boolean triple = src.startsWith(String.valueOf(c).repeat(3), i);
                i += triple ? 3 : 1;
                StringBuilder sb = new StringBuilder();
                List<Object> parts = new ArrayList<>();
                boolean interp = false;
                while (true) {
                    if (i >= n) {
                        throw G.GremlinError.script("unexpected end of script inside a string literal");
                    }
                    char d = src.charAt(i);
                    if (triple ? src.startsWith(String.valueOf(c).repeat(3), i) : d == c) {
                        i += triple ? 3 : 1;
                        break;
                    }
                    if (d == '\\' && i + 1 < n) {
                        char e = src.charAt(i + 1);
                        i += 2;
                        switch (e) {
                            case 'n' -> sb.append('\n');
                            case 't' -> sb.append('\t');
                            case 'r' -> sb.append('\r');
                            case 'b' -> sb.append('\b');
                            case 'f' -> sb.append('\f');
                            case 'u' -> {
                                sb.append((char) Integer.parseInt(src.substring(i, i + 4), 16));
                                i += 4;
                            }
                            case '\n' -> {
                            }
                            default -> sb.append(e);
                        }
                        continue;
                    }
                    if (c == '"' && d == '$' && i + 1 < n && (src.charAt(i + 1) == '{' || Character.isJavaIdentifierStart(src.charAt(i + 1)))) {
                        interp = true;
                        if (sb.length() > 0) {
                            parts.add(sb.toString());
                            sb.setLength(0);
                        }
                        if (src.charAt(i + 1) == '{') {
                            int depth = 1;
                            int j = i + 2;
                            while (j < n && depth > 0) {
                                if (src.charAt(j) == '{') {
                                    depth++;
                                } else if (src.charAt(j) == '}') {
                                    depth--;
                                }
                                j++;
                            }
                            parts.add(new Parser(lex(src.substring(i + 2, j - 1))).program());
                            i = j;
                        } else {
                            int j = i + 1;
                            while (j < n && (Character.isJavaIdentifierPart(src.charAt(j)) && src.charAt(j) != '$')) {
                                j++;
                            }
                            // dotted property access ${a.b} is not supported without braces beyond simple names
                            parts.add(new Var(src.substring(i + 1, j)));
                            i = j;
                        }
                        continue;
                    }
                    sb.append(d);
                    i++;
                }
                if (interp) {
                    if (sb.length() > 0) {
                        parts.add(sb.toString());
                    }
                    out.add(new Tok(K.GSTR, "", parts, st));
                } else {
                    out.add(new Tok(K.STR, sb.toString(), sb.toString(), st));
                }
                continue;
            }
            String[] ops3 = {"<=>", "**=", "?.@", "..<", "<<=", ">>="};
            String[] ops2 = {"==", "!=", "<=", ">=", "&&", "||", "++", "--", "+=", "-=", "*=", "/=", "%=", "..", "?.", "?:", "->", "<<", ">>", "**", "=~", "==~"};
            String op = null;
            for (String o : ops3) {
                if (src.startsWith(o, i)) {
                    op = o;
                    break;
                }
            }
            if (op == null) {
                for (String o : ops2) {
                    if (src.startsWith(o, i)) {
                        op = o;
                        break;
                    }
                }
            }
            if (op == null) {
                if ("+-*/%=<>!?:.,;()[]{}&|~^@".indexOf(c) < 0) {
                    throw G.GremlinError.script("unexpected character '" + c + "' in script");
                }
                op = String.valueOf(c);
            }
            out.add(new Tok(K.OP, op, null, i));
            i += op.length();
        }
        out.add(new Tok(K.EOF, "", null, n));
        return out;
    }

    private static boolean isOperandEnd(Tok t) {
        return t.k == K.NUM || t.k == K.STR || t.k == K.ID || t.k == K.GSTR || (t.k == K.OP && (t.s.equals(")") || t.s.equals("]") || t.s.equals("}")));
    }

    // ================================================================== parser

    private static final class Parser {
        final List<Tok> toks;
        int p;
        int depth;

        Parser(List<Tok> toks) {
            this.toks = toks;
        }

        Tok peek() {
            if (depth > 0) {
                while (toks.get(p).k == K.NL) {
                    p++;
                }
            }
            return toks.get(p);
        }

        Tok next() {
            Tok t = peek();
            p++;
            return t;
        }

        boolean isOp(String s) {
            Tok t = peek();
            return t.k == K.OP && t.s.equals(s);
        }

        boolean isId(String s) {
            Tok t = peek();
            return t.k == K.ID && t.s.equals(s);
        }

        boolean eatOp(String s) {
            if (isOp(s)) {
                p++;
                return true;
            }
            return false;
        }

        void expectOp(String s) {
            if (!eatOp(s)) {
                throw G.GremlinError.script("unexpected token '" + peek().s + "' (expected '" + s + "') at position " + peek().pos);
            }
        }

        void skipNl() {
            while (toks.get(p).k == K.NL || (toks.get(p).k == K.OP && toks.get(p).s.equals(";"))) {
                p++;
            }
        }

        Block program() {
            List<Node> stmts = new ArrayList<>();
            skipNl();
            while (toks.get(p).k != K.EOF) {
                stmts.add(statement());
                skipNl();
            }
            return new Block(stmts);
        }

        Node statement() {
            Tok t = peek();
            if (t.k == K.ID) {
                switch (t.s) {
                    case "if": {
                        p++;
                        expectOp("(");
                        depth++;
                        Node c = expr();
                        depth--;
                        expectOp(")");
                        Node a = bodyOrStmt();
                        Node b = null;
                        int save = p;
                        skipNl();
                        if (isId("else")) {
                            p++;
                            b = bodyOrStmt();
                        } else {
                            p = save;
                        }
                        return new If(c, a, b);
                    }
                    case "while": {
                        p++;
                        expectOp("(");
                        depth++;
                        Node c = expr();
                        depth--;
                        expectOp(")");
                        return new While(c, bodyOrStmt());
                    }
                    case "for": {
                        p++;
                        expectOp("(");
                        depth++;
                        if (peek().k == K.ID && toks.get(p + 1).k == K.ID && !toks.get(p + 1).s.equals("in")) {
                            p++; // optional type
                        }
                        String v = next().s;
                        if (!isId("in") && !isOp(":")) {
                            throw G.GremlinError.script("only for (x in collection) loops are supported");
                        }
                        p++;
                        Node it = expr();
                        depth--;
                        expectOp(")");
                        return new ForIn(v, it, bodyOrStmt());
                    }
                    case "return": {
                        p++;
                        Tok n = toks.get(p);
                        if (n.k == K.NL || n.k == K.EOF || (n.k == K.OP && (n.s.equals(";") || n.s.equals("}")))) {
                            return new Return(new Lit(null));
                        }
                        return new Return(expr());
                    }
                    case "def", "var", "final": {
                        p++;
                        Tok name = peek();
                        if (name.k == K.ID && toks.get(p + 1).k == K.OP && toks.get(p + 1).s.equals("(")) {
                            p++;
                            expectOp("(");
                            List<String> params = new ArrayList<>();
                            depth++;
                            while (!isOp(")")) {
                                if (peek().k == K.ID && toks.get(p + 1).k == K.ID) {
                                    p++;
                                }
                                params.add(next().s);
                                eatOp(",");
                            }
                            depth--;
                            expectOp(")");
                            skipNlOnly();
                            Block body = block();
                            return new DefFunc(name.s, params, body);
                        }
                        p++;
                        if (eatOp("=")) {
                            return new Assign(new Var(name.s), "=", expr());
                        }
                        return new Assign(new Var(name.s), "=", new Lit(null));
                    }
                    default:
                        break;
                }
                // typed declaration: Type name = expr
                if (toks.get(p + 1).k == K.ID && toks.get(p + 2).k == K.OP && toks.get(p + 2).s.equals("=") && !t.s.equals("new")
                        && Character.isUpperCase(t.s.charAt(0)) || (t.k == K.ID && (t.s.equals("int") || t.s.equals("long") || t.s.equals("double")
                                || t.s.equals("boolean") || t.s.equals("float")) && toks.get(p + 1).k == K.ID)) {
                    p++;
                    String name = next().s;
                    if (eatOp("=")) {
                        return new Assign(new Var(name), "=", expr());
                    }
                    return new Assign(new Var(name), "=", new Lit(null));
                }
            }
            if (t.k == K.OP && t.s.equals("{")) {
                return block();
            }
            return expr();
        }

        void skipNlOnly() {
            while (toks.get(p).k == K.NL) {
                p++;
            }
        }

        Node bodyOrStmt() {
            skipNlOnly();
            if (isOp("{")) {
                return block();
            }
            return statement();
        }

        Block block() {
            expectOp("{");
            int sd = depth;
            depth = 0;
            List<Node> stmts = new ArrayList<>();
            skipNl();
            while (!isOp("}")) {
                if (toks.get(p).k == K.EOF) {
                    throw G.GremlinError.script("unexpected end of script (missing '}')");
                }
                stmts.add(statement());
                skipNl();
            }
            depth = sd;
            expectOp("}");
            return new Block(stmts);
        }

        Node expr() {
            Node l = ternary();
            Tok t = peek();
            if (t.k == K.OP) {
                switch (t.s) {
                    case "=", "+=", "-=", "*=", "/=", "%=": {
                        p++;
                        skipNlOnly();
                        Node r = expr();
                        if (!(l instanceof Var) && !(l instanceof Index) && !(l instanceof PropAccess)) {
                            throw G.GremlinError.script("invalid assignment target");
                        }
                        return new Assign(l, t.s, r);
                    }
                    default:
                        break;
                }
            }
            return l;
        }

        Node ternary() {
            Node c = binary(0);
            if (isOp("?")) {
                p++;
                Node a = expr();
                expectOp(":");
                Node b = expr();
                return new Ternary(c, a, b);
            }
            if (isOp("?:")) {
                p++;
                Node b = expr();
                return new Bin("?:", c, b);
            }
            return c;
        }

        private static final String[][] LEVELS = {{"||"}, {"&&"}, {"==", "!=", "<=>"}, {"<", ">", "<=", ">=", "in", "instanceof"}, {".."}, {"<<", ">>"},
                {"+", "-"}, {"*", "/", "%"}, {"**"}};

        Node binary(int level) {
            if (level >= LEVELS.length) {
                return unary();
            }
            Node l = binary(level + 1);
            while (true) {
                Tok t = peek();
                String op = null;
                if (t.k == K.OP || (t.k == K.ID && (t.s.equals("in") || t.s.equals("instanceof")))) {
                    for (String o : LEVELS[level]) {
                        if (o.equals(t.s)) {
                            op = o;
                        }
                    }
                }
                if (op == null) {
                    return l;
                }
                p++;
                skipNlOnly();
                Node r = binary(level + 1);
                l = op.equals("..") ? new RangeNode(l, r) : new Bin(op, l, r);
            }
        }

        Node unary() {
            Tok t = peek();
            if (t.k == K.OP) {
                switch (t.s) {
                    case "!":
                        p++;
                        return new Un("!", unary());
                    case "-":
                        p++;
                        return new Un("-", unary());
                    case "+":
                        p++;
                        return unary();
                    case "++", "--": {
                        p++;
                        Node e = unary();
                        return new Assign(e, t.s.equals("++") ? "+=" : "-=", new Lit(1));
                    }
                    default:
                        break;
                }
            }
            return postfix(primary());
        }

        Node postfix(Node e) {
            while (true) {
                // leading-dot continuation on the next line
                int save = p;
                if (depth == 0) {
                    int q = p;
                    while (toks.get(q).k == K.NL) {
                        q++;
                    }
                    Tok nt = toks.get(q);
                    if (q > p && nt.k == K.OP && (nt.s.equals(".") || nt.s.equals("?."))) {
                        p = q;
                    }
                }
                Tok t = peek();
                if (t.k != K.OP) {
                    p = save;
                    return e;
                }
                switch (t.s) {
                    case ".", "?.": {
                        p++;
                        skipNlOnly();
                        Tok n = next();
                        if (n.k != K.ID && n.k != K.STR) {
                            throw G.GremlinError.script("expected a member name after '.' at position " + n.pos);
                        }
                        boolean safe = t.s.equals("?.");
                        if (isOp("(")) {
                            List<Node> args = args();
                            if (isOp("{")) {
                                args.add(closure());
                            }
                            e = new Call(e, n.s, args, safe);
                        } else if (isOp("{") && n.k == K.ID) {
                            List<Node> args = new ArrayList<>();
                            args.add(closure());
                            e = new Call(e, n.s, args, safe);
                        } else {
                            e = new PropAccess(e, n.s, safe);
                        }
                        break;
                    }
                    case "[": {
                        p++;
                        depth++;
                        Node idx = expr();
                        depth--;
                        expectOp("]");
                        e = new Index(e, idx);
                        break;
                    }
                    case "(": {
                        if (e instanceof Var v) {
                            List<Node> args = args();
                            if (isOp("{")) {
                                args.add(closure());
                            }
                            e = new Call(null, v.name(), args, false);
                            break;
                        }
                        return e;
                    }
                    case "++", "--": {
                        if (e instanceof Var || e instanceof Index || e instanceof PropAccess) {
                            p++;
                            e = new Assign(e, t.s.equals("++") ? "+=" : "-=", new Lit(1));
                            break;
                        }
                        return e;
                    }
                    default:
                        p = save;
                        return e;
                }
            }
        }

        List<Node> args() {
            expectOp("(");
            depth++;
            List<Node> a = new ArrayList<>();
            while (!isOp(")")) {
                // named arguments (map style) are not supported; plain expressions only
                a.add(expr());
                if (!eatOp(",")) {
                    break;
                }
            }
            depth--;
            expectOp(")");
            return a;
        }

        Node closure() {
            expectOp("{");
            int sd = depth;
            depth = 0;
            List<String> params = null;
            int save = p;
            skipNlOnly();
            List<String> ps = new ArrayList<>();
            int q = p;
            boolean ok = false;
            while (true) {
                Tok a = toks.get(q);
                if (a.k == K.ID && toks.get(q + 1).k == K.ID && !toks.get(q + 1).s.equals("in")) {
                    q++;
                    a = toks.get(q);
                }
                if (a.k != K.ID) {
                    break;
                }
                ps.add(a.s);
                q++;
                Tok b = toks.get(q);
                if (b.k == K.OP && b.s.equals(",")) {
                    q++;
                    continue;
                }
                if (b.k == K.OP && b.s.equals("->")) {
                    ok = true;
                    q++;
                }
                break;
            }
            if (!ok && toks.get(p).k == K.OP && toks.get(p).s.equals("->")) {
                ok = true;
                ps.clear();
                q = p + 1;
            }
            if (ok) {
                params = ps;
                p = q;
            } else {
                p = save;
            }
            List<Node> stmts = new ArrayList<>();
            skipNl();
            while (!isOp("}")) {
                if (toks.get(p).k == K.EOF) {
                    throw G.GremlinError.script("unexpected end of script (missing '}')");
                }
                stmts.add(statement());
                skipNl();
            }
            depth = sd;
            expectOp("}");
            return new ClosureNode(params, new Block(stmts));
        }

        Node primary() {
            Tok t = next();
            switch (t.k) {
                case NUM:
                    return new Lit(t.v);
                case STR:
                    return new Lit(t.s);
                case GSTR: {
                    @SuppressWarnings("unchecked")
                    List<Object> parts = (List<Object>) t.v;
                    return new GString(parts);
                }
                case ID:
                    switch (t.s) {
                        case "true":
                            return new Lit(true);
                        case "false":
                            return new Lit(false);
                        case "null":
                            return new Lit(null);
                        case "new": {
                            StringBuilder cls = new StringBuilder(next().s);
                            while (isOp(".")) {
                                p++;
                                cls.append('.').append(next().s);
                            }
                            List<Node> args = isOp("(") ? args() : new ArrayList<>();
                            return new New(cls.toString(), args);
                        }
                        default:
                            return new Var(t.s);
                    }
                case OP:
                    switch (t.s) {
                        case "(": {
                            depth++;
                            Node e = expr();
                            depth--;
                            expectOp(")");
                            return e;
                        }
                        case "[": {
                            depth++;
                            try {
                                if (isOp(":")) {
                                    p++;
                                    expectOp("]");
                                    return new MapLit(new ArrayList<>());
                                }
                                if (isOp("]")) {
                                    p++;
                                    return new ListLit(new ArrayList<>());
                                }
                                Node first = mapKeyOrExpr();
                                if (isOp(":")) {
                                    p++;
                                    List<Node[]> entries = new ArrayList<>();
                                    entries.add(new Node[] {first, expr()});
                                    while (eatOp(",")) {
                                        if (isOp("]")) {
                                            break;
                                        }
                                        Node k = mapKeyOrExpr();
                                        expectOp(":");
                                        entries.add(new Node[] {k, expr()});
                                    }
                                    depth--;
                                    expectOp("]");
                                    depth++;
                                    return new MapLit(entries);
                                }
                                List<Node> items = new ArrayList<>();
                                items.add(first);
                                while (eatOp(",")) {
                                    if (isOp("]")) {
                                        break;
                                    }
                                    items.add(expr());
                                }
                                depth--;
                                expectOp("]");
                                depth++;
                                return new ListLit(items);
                            } finally {
                                depth--;
                            }
                        }
                        case "{":
                            p--;
                            return closure();
                        default:
                            throw G.GremlinError.script("unexpected token '" + t.s + "' at position " + t.pos);
                    }
                default:
                    throw G.GremlinError.script("unexpected end of script");
            }
        }

        Node mapKeyOrExpr() {
            Tok t = peek();
            if (t.k == K.ID && toks.get(p + 1).k == K.OP && toks.get(p + 1).s.equals(":")) {
                p++;
                return new Lit(t.s);
            }
            if (t.k == K.OP && t.s.equals("(")) {
                return expr();
            }
            return expr();
        }
    }

    static Block parse(String src) {
        return new Parser(lex(src)).program();
    }

    /** A g:Lambda from a driver: "{ it.get() }", "x -> x + 1" or a bare expression over {@code it}. */
    static G.Closure lambda(String source) {
        String s = source.trim();
        List<Tok> toks = lex(s);
        Parser ps = new Parser(toks);
        if (s.startsWith("{")) {
            ClosureNode c = (ClosureNode) ps.closure();
            return new G.Closure(c.params(), c.body(), null);
        }
        int arrow = -1;
        for (int i = 0; i < toks.size(); i++) {
            if (toks.get(i).k == K.OP && toks.get(i).s.equals("->")) {
                arrow = i;
                break;
            }
        }
        if (arrow > 0) {
            List<String> params = new ArrayList<>();
            for (int i = 0; i < arrow; i++) {
                if (toks.get(i).k == K.ID) {
                    params.add(toks.get(i).s);
                }
            }
            ps.p = arrow + 1;
            Block b = ps.program();
            return new G.Closure(params, b, null);
        }
        return new G.Closure(null, ps.program(), null);
    }

    // ================================================================== environment

    /** Variable scope + everything a running script needs. */
    static final class Env {
        final Map<String, Object> vars;
        final Env parent;
        final Session session;

        Env(Session session, Map<String, Object> vars, Env parent) {
            this.session = session;
            this.vars = vars;
            this.parent = parent;
        }

        boolean has(String n) {
            for (Env e = this; e != null; e = e.parent) {
                if (e.vars.containsKey(n)) {
                    return true;
                }
            }
            return false;
        }

        Object get(String n) {
            for (Env e = this; e != null; e = e.parent) {
                if (e.vars.containsKey(n)) {
                    return e.vars.get(n);
                }
            }
            return null;
        }

        void set(String n, Object v) {
            for (Env e = this; e != null; e = e.parent) {
                if (e.vars.containsKey(n)) {
                    e.vars.put(n, v);
                    return;
                }
            }
            Env root = this;
            while (root.parent != null && root.parent.parent != null) {
                root = root.parent;
            }
            // assignments without def bind in the script's own (top) scope
            Env top = this;
            while (top.parent != null && top.parent.session == session && top.isClosureScope()) {
                top = top.parent;
            }
            top.vars.put(n, v);
        }

        boolean isClosureScope() {
            return closure;
        }

        boolean closure;
    }

    /** What a script run needs from its host: the store, mode flags, the deadline. */
    static final class Session {
        final GraphStore store;
        final boolean readOnly;
        volatile long deadlineNanos;
        final Map<String, Object> functions = new HashMap<>();

        Session(GraphStore store, boolean readOnly, long deadlineNanos) {
            this.store = store;
            this.readOnly = readOnly;
            this.deadlineNanos = deadlineNanos;
        }

        Engine.Ctx ctx(Env env) {
            Engine.Ctx c = new Engine.Ctx(store);
            c.readOnly = readOnly;
            c.deadlineNanos = deadlineNanos;
            c.closureCaller = (cl, args) -> callClosure(this, cl, args);
            return c;
        }
    }

    private static final class ReturnEx extends RuntimeException {
        final transient Object value;

        ReturnEx(Object v) {
            super(null, null, false, false);
            this.value = v;
        }
    }

    /** A class-like name whose members are static (T, P, Order, Math, ...). */
    record Static(String name) {
    }

    /** The graph object exposed to scripts (graph.traversal(), graph.tx()). */
    record GraphHandle() {
    }

    record TxHandle() {
    }

    /** Runs a script and returns its final value (a traversal is returned unexecuted). */
    static Object eval(String src, Env env) {
        Block b = parse(src);
        try {
            return exec(b, env);
        } catch (ReturnEx r) {
            return r.value;
        } catch (StackOverflowError e) {
            throw G.GremlinError.script("script too deeply nested");
        }
    }

    static Env newRoot(Session s, Map<String, Object> bindings) {
        Env e = new Env(s, new LinkedHashMap<>(), null);
        G.Bytecode g = new G.Bytecode();
        g.bound = e;
        e.vars.put("g", g);
        e.vars.put("graph", new GraphHandle());
        if (bindings != null) {
            e.vars.putAll(bindings);
        }
        return e;
    }

    // ================================================================== evaluator

    private static Object exec(Node n, Env env) {
        switch (n) {
            case Block b: {
                Object last = null;
                for (Node s : b.stmts()) {
                    last = exec(s, env);
                }
                return last;
            }
            case Lit l:
                return l.v();
            case GString g: {
                StringBuilder sb = new StringBuilder();
                for (Object part : g.parts()) {
                    if (part instanceof String s) {
                        sb.append(s);
                    } else {
                        sb.append(str(exec((Node) part, env)));
                    }
                }
                return sb.toString();
            }
            case Var v:
                return variable(v.name(), env);
            case ListLit l: {
                List<Object> out = new ArrayList<>();
                for (Node i : l.items()) {
                    out.add(exec(i, env));
                }
                return out;
            }
            case MapLit m: {
                Map<Object, Object> out = new LinkedHashMap<>();
                for (Node[] e : m.entries()) {
                    Object k = e[0] instanceof Var kv && !env.has(kv.name()) && !isStaticName(kv.name()) ? kv.name() : exec(e[0], env);
                    out.put(k, exec(e[1], env));
                }
                return out;
            }
            case RangeNode r: {
                Object a = exec(r.l(), env);
                Object b = exec(r.r(), env);
                List<Object> out = new ArrayList<>();
                int x = ((Number) a).intValue();
                int y = ((Number) b).intValue();
                if (x <= y) {
                    for (int i = x; i <= y; i++) {
                        out.add(i);
                    }
                } else {
                    for (int i = x; i >= y; i--) {
                        out.add(i);
                    }
                }
                return out;
            }
            case Un u: {
                Object v = exec(u.e(), env);
                if (u.op().equals("!")) {
                    return !Steps.truthy(v);
                }
                if (v instanceof Number nn) {
                    return Cmp.negate(nn);
                }
                throw G.GremlinError.script("bad operand type for unary minus: " + v);
            }
            case Bin b:
                return binary(b, env);
            case Ternary t:
                return Steps.truthy(exec(t.c(), env)) ? exec(t.a(), env) : exec(t.b(), env);
            case Assign a:
                return assign(a, env);
            case If i:
                if (Steps.truthy(exec(i.c(), env))) {
                    return exec(i.a(), env);
                }
                return i.b() == null ? null : exec(i.b(), env);
            case While w: {
                while (Steps.truthy(exec(w.c(), env))) {
                    tick(env);
                    exec(w.body(), env);
                }
                return null;
            }
            case ForIn f: {
                Iterator<Object> it = Steps.iter(materialize(exec(f.iter(), env), env));
                while (it.hasNext()) {
                    tick(env);
                    env.set(f.var(), it.next());
                    exec(f.body(), env);
                }
                return null;
            }
            case Return r:
                throw new ReturnEx(exec(r.e(), env));
            case DefFunc d: {
                env.session.functions.put(d.name(), d);
                return null;
            }
            case ClosureNode c:
                return new G.Closure(c.params(), c.body(), env);
            case New nw:
                return construct(nw, env);
            case PropAccess pa: {
                Object t = exec(pa.target(), env);
                if (t == null && pa.safe()) {
                    return null;
                }
                return Builtins.property(t, pa.name(), env);
            }
            case Index ix: {
                Object t = exec(ix.target(), env);
                Object i = exec(ix.index(), env);
                return Builtins.index(t, i);
            }
            case Call c:
                return call(c, env);
            default:
                throw G.GremlinError.script("unsupported script construct");
        }
    }

    /** Enforces the evaluation timeout inside script loops and closure calls. */
    static void tick(Env env) {
        long d = env.session.deadlineNanos;
        if (d != 0 && System.nanoTime() > d) {
            throw new G.GremlinError(598, "Evaluation exceeded the configured 'evaluationTimeout' threshold");
        }
    }

    static boolean isStaticName(String n) {
        return Builtins.STATICS.contains(n) || Builtins.ENUM_CONSTS.containsKey(n);
    }

    private static Object variable(String name, Env env) {
        if (env.has(name)) {
            return env.get(name);
        }
        if (name.equals("__") || Builtins.STATICS.contains(name)) {
            return new Static(name);
        }
        Object c = Builtins.ENUM_CONSTS.get(name);
        if (c != null) {
            return c;
        }
        throw G.GremlinError.script("No such property: " + name + " for class: Script1");
    }

    private static Object assign(Assign a, Env env) {
        Object v = exec(a.value(), env);
        v = materialize(v, env, true);
        if (!a.op().equals("=")) {
            Object old = exec(a.target(), env);
            v = arith(a.op().substring(0, a.op().length() - 1), old, v);
        }
        switch (a.target()) {
            case Var var:
                env.set(var.name(), v);
                return v;
            case Index ix: {
                Object t = exec(ix.target(), env);
                Object i = exec(ix.index(), env);
                if (t instanceof Map<?, ?> m) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> mm = (Map<Object, Object>) m;
                    mm.put(i, v);
                } else if (t instanceof List<?> l) {
                    @SuppressWarnings("unchecked")
                    List<Object> ll = (List<Object>) l;
                    ll.set(((Number) i).intValue(), v);
                } else {
                    throw G.GremlinError.script("cannot assign into " + t);
                }
                return v;
            }
            case PropAccess pa: {
                Object t = exec(pa.target(), env);
                if (t instanceof Map<?, ?> m) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> mm = (Map<Object, Object>) m;
                    mm.put(pa.name(), v);
                    return v;
                }
                throw G.GremlinError.script("cannot assign property " + pa.name() + " of " + t);
            }
            default:
                throw G.GremlinError.script("invalid assignment target");
        }
    }

    /** Values stored in variables: a traversal is kept lazy (Groovy would keep the Traversal object too). */
    private static Object materialize(Object v, Env env, boolean keepTraversal) {
        return v;
    }

    /** A traversal in a collection position is iterated. */
    static Object materialize(Object v, Env env) {
        if (v instanceof G.Bytecode bc) {
            return terminal(bc, "toList", new Object[0], env);
        }
        return v;
    }

    private static Object construct(New n, Env env) {
        List<Object> args = new ArrayList<>();
        for (Node a : n.args()) {
            args.add(exec(a, env));
        }
        String c = n.cls();
        int dot = c.lastIndexOf('.');
        String simple = dot < 0 ? c : c.substring(dot + 1);
        switch (simple) {
            case "Date":
                return args.isEmpty() ? new Date() : new Date(((Number) args.get(0)).longValue());
            case "ArrayList", "LinkedList", "Vector":
                return args.isEmpty() || !(args.get(0) instanceof Collection<?>) ? new ArrayList<>() : new ArrayList<>((Collection<?>) args.get(0));
            case "HashMap", "LinkedHashMap", "TreeMap":
                return new LinkedHashMap<>();
            case "HashSet", "LinkedHashSet", "TreeSet":
                return args.isEmpty() || !(args.get(0) instanceof Collection<?>) ? new LinkedHashSet<>() : new LinkedHashSet<>((Collection<?>) args.get(0));
            case "UUID":
                return new UUID(((Number) args.get(0)).longValue(), ((Number) args.get(1)).longValue());
            case "BigDecimal":
                return new BigDecimal(String.valueOf(args.get(0)));
            case "BigInteger":
                return new BigInteger(String.valueOf(args.get(0)));
            case "String":
                return args.isEmpty() ? "" : String.valueOf(args.get(0));
            case "Random":
                return new java.util.Random();
            default:
                throw G.GremlinError.script("unable to resolve class " + c);
        }
    }

    // ------------------------------------------------------------------ binary operators

    private static Object binary(Bin b, Env env) {
        String op = b.op();
        if (op.equals("&&")) {
            Object l = exec(b.l(), env);
            return Steps.truthy(l) && Steps.truthy(exec(b.r(), env));
        }
        if (op.equals("||")) {
            Object l = exec(b.l(), env);
            return Steps.truthy(l) || Steps.truthy(exec(b.r(), env));
        }
        if (op.equals("?:")) {
            Object l = exec(b.l(), env);
            return Steps.truthy(l) ? l : exec(b.r(), env);
        }
        Object l = exec(b.l(), env);
        Object r = exec(b.r(), env);
        return arith(op, l, r);
    }

    static String str(Object o) {
        if (o instanceof G.Bytecode bc) {
            return bc.toString();
        }
        if (o instanceof Collection<?> c) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object x : c) {
                sb.append(first ? "" : ", ").append(str(x));
                first = false;
            }
            return sb.append("]").toString();
        }
        if (o instanceof Map<?, ?> m) {
            if (m.isEmpty()) {
                return "[:]";
            }
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                sb.append(first ? "" : ", ").append(str(e.getKey())).append(":").append(str(e.getValue()));
                first = false;
            }
            return sb.append("]").toString();
        }
        return String.valueOf(o);
    }

    @SuppressWarnings("unchecked")
    static Object arith(String op, Object l, Object r) {
        switch (op) {
            case "==":
                return looseEq(l, r);
            case "!=":
                return !looseEq(l, r);
            case "<", ">", "<=", ">=": {
                Integer c = Cmp.compare(l, r);
                if (c == null) {
                    throw G.GremlinError.script("Cannot compare " + (l == null ? "null" : l.getClass().getName()) + " with " + (r == null ? "null" : r.getClass().getName()));
                }
                return switch (op) {
                    case "<" -> c < 0;
                    case ">" -> c > 0;
                    case "<=" -> c <= 0;
                    default -> c >= 0;
                };
            }
            case "<=>":
                return Cmp.order(l, r);
            case "in":
                return r instanceof Collection<?> c ? c.contains(l) : r instanceof Map<?, ?> m && m.containsKey(l);
            case "instanceof":
                return false;
            case "<<":
                if (l instanceof Collection<?> c) {
                    ((Collection<Object>) c).add(r);
                    return l;
                }
                throw G.GremlinError.script("cannot apply << to " + l);
            default:
                break;
        }
        if (l instanceof Number a && r instanceof Number b) {
            // Groovy computes with double whenever a float is involved
            if (a instanceof Float) {
                a = a.doubleValue();
            }
            if (b instanceof Float) {
                b = b.doubleValue();
            }
            return switch (op) {
                case "+" -> Cmp.add(a, b);
                case "-" -> Cmp.sub(a, b);
                case "*" -> Cmp.mul(a, b);
                case "/" -> Cmp.groovyDiv(a, b);
                case "%" -> Cmp.mod(a, b);
                case "**" -> Math.pow(a.doubleValue(), b.doubleValue()) == Math.rint(Math.pow(a.doubleValue(), b.doubleValue())) && a instanceof Integer && b instanceof Integer
                        ? (Number) (int) Math.pow(a.doubleValue(), b.doubleValue()) : (Number) Math.pow(a.doubleValue(), b.doubleValue());
                default -> throw G.GremlinError.script("unsupported operator " + op);
            };
        }
        if (op.equals("+")) {
            if (l instanceof String || r instanceof String) {
                return str(l) + str(r);
            }
            if (l instanceof Collection<?> a) {
                List<Object> out = new ArrayList<>(a);
                if (r instanceof Collection<?> b) {
                    out.addAll(b);
                } else {
                    out.add(r);
                }
                return out;
            }
            if (l instanceof Map<?, ?> a && r instanceof Map<?, ?> b) {
                Map<Object, Object> out = new LinkedHashMap<>(a);
                out.putAll(b);
                return out;
            }
        }
        if (op.equals("-") && l instanceof Collection<?> a) {
            List<Object> out = new ArrayList<>(a);
            if (r instanceof Collection<?> b) {
                out.removeAll(b);
            } else {
                out.remove(r);
            }
            return out;
        }
        if (op.equals("*") && l instanceof String s && r instanceof Number n) {
            return s.repeat(n.intValue());
        }
        throw G.GremlinError.script("No signature of method: " + (l == null ? "null" : l.getClass().getName()) + "." + opName(op) + "() is applicable for argument types: ("
                + (r == null ? "null" : r.getClass().getName()) + ")");
    }

    private static String opName(String op) {
        return switch (op) {
            case "+" -> "plus";
            case "-" -> "minus";
            case "*" -> "multiply";
            case "/" -> "div";
            default -> op;
        };
    }

    static boolean looseEq(Object l, Object r) {
        if (l instanceof Number a && r instanceof Number b) {
            return Cmp.numCompare(a, b) == 0;
        }
        return Cmp.eq(l, r);
    }

    // ------------------------------------------------------------------ calls

    private static Object call(Call c, Env env) {
        List<Object> args = new ArrayList<>();
        Object target = null;
        if (c.target() != null) {
            target = exec(c.target(), env);
            if (target == null && c.safe()) {
                return null;
            }
        }
        for (Node a : c.args()) {
            args.add(exec(a, env));
        }
        Object[] av = args.toArray();
        if (c.target() == null) {
            return staticCall(c.name(), av, env);
        }
        if (target instanceof G.Bytecode bc) {
            return traversalCall(bc, c.name(), av, env);
        }
        if (target instanceof Static s) {
            return Builtins.staticMethod(s.name(), c.name(), av, env);
        }
        return Builtins.method(target, c.name(), av, env);
    }

    /** A call without receiver: a user function, a predicate factory or an anonymous-traversal step. */
    private static Object staticCall(String name, Object[] args, Env env) {
        Object f = env.session.functions.get(name);
        if (f instanceof DefFunc d) {
            Env local = new Env(env.session, new LinkedHashMap<>(), rootOf(env));
            for (int i = 0; i < d.params().size(); i++) {
                local.vars.put(d.params().get(i), i < args.length ? args[i] : null);
            }
            try {
                return exec(d.body(), local);
            } catch (ReturnEx r) {
                return r.value;
            }
        }
        if (env.has(name) && env.get(name) instanceof G.Closure cl) {
            return callClosure(env.session, cl, args);
        }
        return Builtins.bareCall(name, args);
    }

    private static Env rootOf(Env e) {
        while (e.parent != null) {
            e = e.parent;
        }
        return e;
    }

    static final Set<String> SOURCE_STEPS = Set.of("withSideEffect", "withSack", "withStrategies", "withBulk", "withPath", "withComputer", "with", "withoutStrategies");

    private static Object traversalCall(G.Bytecode bc, String name, Object[] args, Env env) {
        switch (name) {
            case "toList", "toSet", "next", "iterate", "hasNext", "tryNext", "explain", "toBulkSet", "fill", "forEachRemaining", "getBytecode", "profile_", "hasNext_",
                    "iterator", "getTraversal", "close", "toString", "size", "isEmpty", "collect", "each", "findAll", "first", "last", "join", "sort", "count_":
                if (!(name.equals("explain") || name.equals("profile_"))) {
                    return terminal(bc, name, args, env);
                }
                throw G.GremlinError.script("The explain() step is not supported by this Gremlin server");
            case "tx":
                return new TxHandle();
            case "getGraph":
                return new GraphHandle();
            default:
                break;
        }
        G.Bytecode n = bc.copy();
        if (bc.steps.isEmpty() && bc.bound != null && SOURCE_STEPS.contains(name)) {
            Object[] s = new Object[args.length + 1];
            s[0] = name;
            System.arraycopy(args, 0, s, 1, args.length);
            n.source.add(s);
            return n;
        }
        n.add(name, args);
        return n;
    }

    /** Executes a script traversal to completion. */
    static Object terminal(G.Bytecode bc, String name, Object[] args, Env env) {
        if (name.equals("toString")) {
            return bc.toString();
        }
        if (name.equals("getBytecode")) {
            return bc;
        }
        if (bc.consumed && !name.equals("iterate")) {
            if (name.equals("next")) {
                throw new G.GremlinError(597, "java.util.NoSuchElementException");
            }
            return name.equals("hasNext") ? (Object) false : name.equals("toSet") ? new LinkedHashSet<>() : new ArrayList<>();
        }
        Engine.Ctx ctx = env.session.ctx(env);
        Iterator<Object> it = Engine.execute(bc, ctx);
        switch (name) {
            case "iterate": {
                while (it.hasNext()) {
                    it.next();
                }
                bc.consumed = true;
                return bc;
            }
            case "hasNext":
                return it.hasNext();
            case "next": {
                if (args.length == 1) {
                    int n = ((Number) args[0]).intValue();
                    List<Object> out = new ArrayList<>();
                    while (out.size() < n && it.hasNext()) {
                        out.add(it.next());
                    }
                    return out;
                }
                if (!it.hasNext()) {
                    throw new G.GremlinError(597, "java.util.NoSuchElementException");
                }
                return it.next();
            }
            case "tryNext": {
                return it.hasNext() ? it.next() : null;
            }
            case "toSet": {
                Set<Object> s = new LinkedHashSet<>();
                while (it.hasNext()) {
                    s.add(it.next());
                }
                return s;
            }
            case "iterator":
                return it;
            default: {
                List<Object> out = new ArrayList<>();
                while (it.hasNext()) {
                    out.add(it.next());
                }
                bc.consumed = true;
                if (name.equals("fill") && args.length == 1 && args[0] instanceof Collection<?> c) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> cc = (Collection<Object>) c;
                    cc.addAll(out);
                    return c;
                }
                if (name.equals("size")) {
                    return out.size();
                }
                if (name.equals("isEmpty")) {
                    return out.isEmpty();
                }
                return out;
            }
        }
    }

    static Object callClosure(Session session, G.Closure c, Object... args) {
        Env scope = c.scope() instanceof Env e ? e : lambdaEnv(session);
        Env local = new Env(scope.session, new LinkedHashMap<>(), scope);
        local.closure = true;
        tick(scope);
        List<String> ps = c.params();
        if (ps == null || ps.isEmpty()) {
            local.vars.put("it", args.length > 0 ? args[0] : null);
        } else {
            for (int i = 0; i < ps.size(); i++) {
                local.vars.put(ps.get(i), i < args.length ? args[i] : null);
            }
        }
        try {
            return exec((Node) c.body(), local);
        } catch (ReturnEx r) {
            return r.value;
        }
    }

    /** Runs a closure created by a driver lambda (no lexical scope): a fresh top-level env over the session. */
    static Env lambdaEnv(Session s) {
        return newRoot(s, null);
    }
}
