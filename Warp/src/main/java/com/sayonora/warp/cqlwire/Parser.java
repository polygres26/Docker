package com.sayonora.warp.cqlwire;

import com.sayonora.warp.cqlwire.Ast.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/** Lexer and recursive-descent parser of the CQL subset Warp serves. */
final class Parser {

    enum T { ID, QID, STR, INT, FLOAT, HEX, UUID, DURATION, SYM, QMARK, NAMED, EOF }

    record Tok(T t, String text, int pos) {
        String lc() {
            return text.toLowerCase(Locale.ROOT);
        }
    }

    private static final Set<String> RESERVED = Set.of("add", "allow", "alter", "and", "apply", "asc", "authorize", "batch", "begin", "by",
            "columnfamily", "create", "delete", "desc", "describe", "drop", "entries", "execute", "from", "full", "grant", "if", "in",
            "index", "infinity", "insert", "into", "keyspace", "limit", "modify", "nan", "norecursive", "not", "null", "of", "on", "or",
            "order", "primary", "rename", "replace", "revoke", "schema", "select", "set", "table", "to", "token", "truncate", "unlogged",
            "update", "use", "using", "view", "where", "with");

    private static final java.util.regex.Pattern UUID_RE = java.util.regex.Pattern
            .compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final java.util.regex.Pattern DUR_RE = java.util.regex.Pattern
            .compile("(\\d+(?:y|mo|ms|w|d|h|m|us|µs|ns|s))+", java.util.regex.Pattern.CASE_INSENSITIVE);
    private static final java.util.regex.Pattern ISO_DUR = java.util.regex.Pattern.compile("[Pp]\\d{4}-\\d{2}-\\d{2}[Tt]\\d{2}:\\d{2}:\\d{2}");
    private static final java.util.regex.Pattern NUM_RE = java.util.regex.Pattern.compile("\\d+(\\.\\d*)?([eE][+-]?\\d+)?|\\.\\d+([eE][+-]?\\d+)?");

    private final String src;
    private final List<Tok> toks = new ArrayList<>();
    private int p;
    private int binds;

    private Parser(String src) {
        this.src = src;
        lex();
    }

    record Parsed(Stmt stmt, int binds) {
    }

    static Stmt parse(String cql) {
        return parseFull(cql).stmt();
    }

    static Parsed parseFull(String cql) {
        Parser ps = new Parser(cql);
        Stmt s = ps.statement();
        while (ps.symIf(";")) {
            // trailing semicolons
        }
        if (ps.peek().t != T.EOF) {
            throw ps.err("extraneous input '" + ps.peek().text + "'");
        }
        return new Parsed(s, ps.binds);
    }

    /** Number of bind markers of a parsed statement text (used by prepare). */
    static CqlType parseType(String text, String defaultKs, BiFunction<String, String, CqlType> udts) {
        Parser ps = new Parser(text);
        CqlType t = ps.type(defaultKs, udts);
        if (ps.peek().t != T.EOF) {
            throw ps.err("extraneous input in type '" + text + "'");
        }
        return t;
    }

    // ------------------------------------------------------------------ lexer

    private CqlError err(String msg) {
        Tok t = peek();
        int line = 1, col = 0;
        for (int i = 0; i < Math.min(t.pos, src.length()); i++) {
            if (src.charAt(i) == '\n') {
                line++;
                col = 0;
            } else {
                col++;
            }
        }
        return CqlError.syntax("line " + line + ":" + col + " " + msg);
    }

    private void lex() {
        int i = 0, n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '-' && i + 1 < n && src.charAt(i + 1) == '-' || c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int e = src.indexOf("*/", i + 2);
                if (e < 0) {
                    throw CqlError.syntax("line 1:" + i + " unterminated comment");
                }
                i = e + 2;
                continue;
            }
            int start = i;
            if (c == '\'') {
                StringBuilder b = new StringBuilder();
                i++;
                while (true) {
                    if (i >= n) {
                        throw CqlError.syntax("line 1:" + start + " unterminated string literal");
                    }
                    char d = src.charAt(i);
                    if (d == '\'') {
                        if (i + 1 < n && src.charAt(i + 1) == '\'') {
                            b.append('\'');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    b.append(d);
                    i++;
                }
                toks.add(new Tok(T.STR, b.toString(), start));
            } else if (c == '$' && i + 1 < n && src.charAt(i + 1) == '$') {
                int e = src.indexOf("$$", i + 2);
                if (e < 0) {
                    throw CqlError.syntax("line 1:" + start + " unterminated string literal");
                }
                toks.add(new Tok(T.STR, src.substring(i + 2, e), start));
                i = e + 2;
            } else if (c == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                while (true) {
                    if (i >= n) {
                        throw CqlError.syntax("line 1:" + start + " unterminated quoted identifier");
                    }
                    char d = src.charAt(i);
                    if (d == '"') {
                        if (i + 1 < n && src.charAt(i + 1) == '"') {
                            b.append('"');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    b.append(d);
                    i++;
                }
                toks.add(new Tok(T.QID, b.toString(), start));
            } else if (c == '0' && i + 1 < n && (src.charAt(i + 1) == 'x' || src.charAt(i + 1) == 'X')) {
                i += 2;
                while (i < n && Character.digit(src.charAt(i), 16) >= 0) {
                    i++;
                }
                toks.add(new Tok(T.HEX, src.substring(start + 2, i), start));
            } else if (Character.digit(c, 16) >= 0 && UUID_RE.matcher(src).region(i, Math.min(n, i + 36)).lookingAt()
                    && (i + 36 >= n || !Character.isLetterOrDigit(src.charAt(i + 36)))) {
                toks.add(new Tok(T.UUID, src.substring(i, i + 36), start));
                i += 36;
            } else if (Character.isDigit(c) || c == '.' && i + 1 < n && Character.isDigit(src.charAt(i + 1))) {
                java.util.regex.Matcher dm = DUR_RE.matcher(src).region(i, n);
                java.util.regex.Matcher nm = NUM_RE.matcher(src).region(i, n);
                if (dm.lookingAt() && !Character.isLetterOrDigit(dm.end() < n ? src.charAt(dm.end()) : ' ') ) {
                    toks.add(new Tok(T.DURATION, dm.group(), start));
                    i = dm.end();
                } else if (nm.lookingAt()) {
                    String s = nm.group();
                    i = nm.end();
                    // "1.": a trailing dot is not part of the number when a name follows (x.y access); keep as float otherwise
                    toks.add(new Tok(s.matches("\\d+") ? T.INT : T.FLOAT, s, start));
                } else {
                    throw CqlError.syntax("line 1:" + i + " bad number");
                }
            } else if ((c == 'P' || c == 'p') && ISO_DUR.matcher(src).region(i, n).lookingAt()) {
                java.util.regex.Matcher im = ISO_DUR.matcher(src).region(i, n);
                im.lookingAt();
                toks.add(new Tok(T.DURATION, im.group(), start));
                i = im.end();
            } else if (Character.isLetter(c) || c == '_') {
                while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                    i++;
                }
                toks.add(new Tok(T.ID, src.substring(start, i), start));
            } else if (c == '?') {
                toks.add(new Tok(T.QMARK, "?", start));
                i++;
            } else if (c == ':' && i + 1 < n && (Character.isLetter(src.charAt(i + 1)) || src.charAt(i + 1) == '_' || src.charAt(i + 1) == '"')) {
                i++;
                if (src.charAt(i) == '"') {
                    int e = src.indexOf('"', i + 1);
                    toks.add(new Tok(T.NAMED, src.substring(i + 1, e), start));
                    i = e + 1;
                } else {
                    int s0 = i;
                    while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) {
                        i++;
                    }
                    toks.add(new Tok(T.NAMED, src.substring(s0, i).toLowerCase(Locale.ROOT), start));
                }
            } else if ("<>!".indexOf(c) >= 0 && i + 1 < n && src.charAt(i + 1) == '=') {
                toks.add(new Tok(T.SYM, "" + c + '=', start));
                i += 2;
            } else if (c == '<' && i + 1 < n && src.charAt(i + 1) == '>') {
                toks.add(new Tok(T.SYM, "!=", start));
                i += 2;
            } else if ("(),.;=<>+-*/%[]{}:".indexOf(c) >= 0) {
                toks.add(new Tok(T.SYM, String.valueOf(c), start));
                i++;
            } else {
                throw CqlError.syntax("line 1:" + i + " no viable alternative at character '" + c + "'");
            }
        }
        toks.add(new Tok(T.EOF, "", n));
    }

    // ------------------------------------------------------------------ token helpers

    private Tok peek() {
        return toks.get(p);
    }

    private Tok peek(int k) {
        return toks.get(Math.min(p + k, toks.size() - 1));
    }

    private Tok next() {
        Tok t = toks.get(p);
        if (t.t != T.EOF) {
            p++;
        }
        return t;
    }

    private boolean kw(String w) {
        Tok t = peek();
        return t.t == T.ID && t.text.equalsIgnoreCase(w);
    }

    private boolean kwAt(int k, String w) {
        Tok t = peek(k);
        return t.t == T.ID && t.text.equalsIgnoreCase(w);
    }

    private boolean kwIf(String w) {
        if (kw(w)) {
            p++;
            return true;
        }
        return false;
    }

    private void expectKw(String w) {
        if (!kwIf(w)) {
            throw err("mismatched input '" + peek().text + "' expecting " + w.toUpperCase(Locale.ROOT));
        }
    }

    private boolean sym(String s) {
        Tok t = peek();
        return t.t == T.SYM && t.text.equals(s);
    }

    private boolean symIf(String s) {
        if (sym(s)) {
            p++;
            return true;
        }
        return false;
    }

    private void expectSym(String s) {
        if (!symIf(s)) {
            throw err("mismatched input '" + peek().text + "' expecting '" + s + "'");
        }
    }

    /** A name: unquoted lower-cased (reserved words refused), or quoted as written. */
    private String name() {
        Tok t = peek();
        if (t.t == T.QID) {
            p++;
            return t.text;
        }
        if (t.t == T.ID) {
            if (RESERVED.contains(t.lc())) {
                throw err("no viable alternative at input '" + t.text + "'");
            }
            p++;
            return t.lc();
        }
        throw err("no viable alternative at input '" + t.text + "'");
    }

    /** Like {@link #name} but tolerates reserved words (property names, function names). */
    private String anyName() {
        Tok t = peek();
        if (t.t == T.QID) {
            p++;
            return t.text;
        }
        if (t.t == T.ID) {
            p++;
            return t.lc();
        }
        throw err("no viable alternative at input '" + t.text + "'");
    }

    private String[] qname() {
        String a = name();
        if (symIf(".")) {
            return new String[] {a, name()};
        }
        return new String[] {null, a};
    }

    // ------------------------------------------------------------------ statements

    private Stmt statement() {
        Tok t = peek();
        if (t.t == T.EOF || sym(";")) {
            throw err("no viable alternative at input '<EOF>'");
        }
        if (t.t != T.ID) {
            throw err("no viable alternative at input '" + t.text + "'");
        }
        switch (t.lc()) {
            case "select":
                return select();
            case "insert":
                return insert();
            case "update":
                return update();
            case "delete":
                return delete();
            case "begin":
                return batch();
            case "use":
                p++;
                return new Use(name());
            case "create":
                return create();
            case "alter":
                return alter();
            case "drop":
                return drop();
            case "truncate": {
                p++;
                kwIf("table");
                kwIf("columnfamily");
                String[] q = qname();
                return new Truncate(q[0], q[1]);
            }
            case "grant", "revoke", "list":
                p = toks.size() - 1;
                return new Other(t.lc());
            case "describe", "desc":
                return describe();
            default:
                throw err("no viable alternative at input '" + t.text + "'");
        }
    }

    private Stmt describe() {
        p++;
        boolean full = false, only = false;
        if (kw("full") && (kwAt(1, "schema"))) {
            p++;
            full = true;
        }
        if (kw("only")) {
            p++;
            only = true;
        }
        Tok t = peek();
        String what = t.t == T.ID ? t.lc() : "";
        switch (what) {
            case "cluster", "schema", "keyspaces", "tables", "types", "functions", "aggregates", "columnfamilies" -> {
                p++;
                return new Describe(what.equals("columnfamilies") ? "tables" : what, null, null, full, only);
            }
            case "keyspace", "table", "columnfamily", "type", "index", "function", "aggregate", "materialized" -> {
                p++;
                String w = what.equals("columnfamily") ? "table" : what;
                if (what.equals("materialized")) {
                    expectKw("view");
                    w = "view";
                }
                if (peek().t == T.EOF || sym(";")) {
                    return new Describe(w, null, null, full, only);
                }
                String[] q = qname();
                if (w.equals("keyspace")) {
                    return new Describe(w, null, q[0] != null ? q[0] : q[1], full, only);
                }
                return new Describe(w, q[0], q[1], full, only);
            }
            default -> {
                if (t.t == T.EOF) {
                    throw err("no viable alternative at input '<EOF>'");
                }
                String[] q = qname();
                return new Describe("element", q[0], q[1], full, only);
            }
        }
    }

    private Map<String, Object> properties() {
        Map<String, Object> m = new LinkedHashMap<>();
        do {
            if (kw("clustering") && kwAt(1, "order")) {
                p += 2;
                expectKw("by");
                expectSym("(");
                List<Object> ord = new ArrayList<>();
                do {
                    String c = name();
                    boolean desc = false;
                    if (kwIf("desc")) {
                        desc = true;
                    } else {
                        kwIf("asc");
                    }
                    ord.add(new Order(c, desc));
                } while (symIf(","));
                expectSym(")");
                m.put("clustering order", ord);
                continue;
            }
            if (kw("compact") && kwAt(1, "storage")) {
                p += 2;
                m.put("compact storage", Boolean.TRUE);
                continue;
            }
            String k = anyName();
            expectSym("=");
            m.put(k, propValue());
        } while (kwIf("and"));
        return m;
    }

    private Object propValue() {
        if (sym("{")) {
            p++;
            Map<String, Object> m = new LinkedHashMap<>();
            if (!sym("}")) {
                do {
                    Object k = propAtom();
                    expectSym(":");
                    m.put(String.valueOf(k), propAtom());
                } while (symIf(","));
            }
            expectSym("}");
            return m;
        }
        return propAtom();
    }

    private Object propAtom() {
        Tok t = next();
        switch (t.t) {
            case STR:
                return t.text;
            case INT:
                return Long.parseLong(t.text);
            case FLOAT:
                return new BigDecimal(t.text).doubleValue();
            case ID:
                if (t.lc().equals("true") || t.lc().equals("false")) {
                    return Boolean.parseBoolean(t.lc());
                }
                return t.text;
            case SYM:
                if (t.text.equals("-") && peek().t == T.INT) {
                    return -Long.parseLong(next().text);
                }
                if (t.text.equals("{")) {
                    p--;
                    return propValue();
                }
            default:
                throw err("no viable alternative at input '" + t.text + "'");
        }
    }

    private boolean ifNotExists() {
        if (kw("if") && kwAt(1, "not") && kwAt(2, "exists")) {
            p += 3;
            return true;
        }
        return false;
    }

    private boolean ifExists() {
        if (kw("if") && kwAt(1, "exists")) {
            p += 2;
            return true;
        }
        return false;
    }

    private Stmt create() {
        p++;
        if (kw("keyspace") || kw("schema")) {
            p++;
            boolean ine = ifNotExists();
            String n = name();
            expectKw("with");
            return new CreateKeyspace(n, ine, properties());
        }
        if (kw("table") || kw("columnfamily")) {
            p++;
            return createTable();
        }
        boolean custom = kwIf("custom");
        if (kw("index")) {
            p++;
            return createIndex(custom);
        }
        if (kw("type")) {
            p++;
            boolean ine = ifNotExists();
            String[] q = qname();
            expectSym("(");
            List<String> f = new ArrayList<>(), ty = new ArrayList<>();
            do {
                f.add(name());
                ty.add(typeText());
            } while (symIf(","));
            expectSym(")");
            return new CreateType(q[0], q[1], ine, f, ty);
        }
        String what = peek().lc();
        if (what.equals("or") || what.equals("role") || what.equals("user") || what.equals("materialized") || what.equals("function")
                || what.equals("aggregate") || what.equals("trigger")) {
            String k = what.equals("or") ? "function" : what;
            p = toks.size() - 1;
            return new Other("create " + k);
        }
        throw err("no viable alternative at input '" + peek().text + "'");
    }

    private Stmt createTable() {
        boolean ine = ifNotExists();
        String[] q = qname();
        expectSym("(");
        List<ColDef> cols = new ArrayList<>();
        List<String> partition = new ArrayList<>(), clustering = new ArrayList<>();
        boolean sawPk = false;
        do {
            if (kw("primary") && kwAt(1, "key")) {
                p += 2;
                if (sawPk) {
                    throw CqlError.invalid("Multiple PRIMARY KEYs specified (exactly one required)");
                }
                sawPk = true;
                expectSym("(");
                if (symIf("(")) {
                    do {
                        partition.add(name());
                    } while (symIf(","));
                    expectSym(")");
                } else {
                    partition.add(name());
                }
                while (symIf(",")) {
                    clustering.add(name());
                }
                expectSym(")");
            } else {
                String n = name();
                String ty = typeText();
                boolean st = false, pk = false;
                while (true) {
                    if (kwIf("static")) {
                        st = true;
                    } else if (kw("primary") && kwAt(1, "key")) {
                        p += 2;
                        pk = true;
                    } else {
                        break;
                    }
                }
                if (pk) {
                    if (sawPk) {
                        throw CqlError.invalid("Multiple PRIMARY KEYs specified (exactly one required)");
                    }
                    sawPk = true;
                    partition.add(n);
                }
                cols.add(new ColDef(n, ty, st, pk));
            }
        } while (symIf(","));
        expectSym(")");
        Map<String, Object> opts = new LinkedHashMap<>();
        if (kwIf("with")) {
            opts = properties();
        }
        List<Boolean> desc = new ArrayList<>();
        for (int i = 0; i < clustering.size(); i++) {
            desc.add(false);
        }
        Object co = opts.remove("clustering order");
        if (co != null) {
            @SuppressWarnings("unchecked")
            List<Order> ord = (List<Order>) (List<?>) co;
            int seen = 0;
            List<String> notClustering = new ArrayList<>();
            for (Order o : ord) {
                if (!clustering.contains(o.col())) {
                    notClustering.add(o.col());
                }
            }
            if (!notClustering.isEmpty()) {
                throw CqlError.invalid("Only clustering key columns can be defined in CLUSTERING ORDER directive: " + notClustering
                        + " are not clustering columns");
            }
            for (Order o : ord) {
                int idx = clustering.indexOf(o.col());
                if (idx != seen) {
                    throw CqlError.invalid("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key ("
                            + String.join(", ", clustering) + ")");
                }
                desc.set(idx, o.desc());
                seen++;
            }
            if (seen != clustering.size()) {
                throw CqlError.invalid("Missing CLUSTERING ORDER for column " + clustering.get(seen));
            }
        }
        return new CreateTable(q[0], q[1], ine, cols, partition, clustering, desc, opts);
    }

    private Stmt createIndex(boolean custom) {
        boolean ine = ifNotExists();
        String name = null;
        if (!kw("on")) {
            name = name();
            if (symIf(".")) {
                name = name();
            }
        }
        expectKw("on");
        String[] q = qname();
        expectSym("(");
        String kind = "values", target;
        if (kw("keys") || kw("values") || kw("entries") || kw("full")) {
            kind = next().lc();
            expectSym("(");
            target = name();
            expectSym(")");
        } else {
            target = name();
        }
        expectSym(")");
        String cls = null;
        Map<String, Object> opts = new LinkedHashMap<>();
        if (kwIf("using")) {
            cls = next().text;
        }
        if (kwIf("with")) {
            opts = properties();
        }
        return new CreateIndex(name, ine, q[0], q[1], target, kind, cls, opts);
    }

    private Stmt alter() {
        p++;
        if (kw("keyspace") || kw("schema")) {
            p++;
            String n = name();
            expectKw("with");
            return new AlterKeyspace(n, properties());
        }
        if (kw("type")) {
            p++;
            String[] q = qname();
            if (kwIf("add")) {
                String f = name();
                return new AlterType(q[0], q[1], f, typeText(), Map.of());
            }
            expectKw("rename");
            Map<String, String> ren = new LinkedHashMap<>();
            do {
                String a = name();
                expectKw("to");
                ren.put(a, name());
            } while (kwIf("and"));
            return new AlterType(q[0], q[1], null, null, ren);
        }
        if (!(kw("table") || kw("columnfamily"))) {
            String what = peek().lc();
        if (what.equals("role") || what.equals("user") || what.equals("materialized")) {
            p = toks.size() - 1;
            return new Other("alter " + what);
        }
        throw err("no viable alternative at input '" + peek().text + "'");
        }
        p++;
        boolean ifEx = ifExists();
        String[] q = qname();
        List<ColDef> add = new ArrayList<>();
        List<String> drop = new ArrayList<>();
        Map<String, String> rename = new LinkedHashMap<>(), alterType = new LinkedHashMap<>();
        Map<String, Object> opts = new LinkedHashMap<>();
        boolean addIne = false, dropIe = false;
        if (kwIf("add")) {
            addIne = ifNotExists();
            boolean paren = symIf("(");
            do {
                String n = name();
                String ty = typeText();
                add.add(new ColDef(n, ty, kwIf("static"), false));
            } while (symIf(","));
            if (paren) {
                expectSym(")");
            }
        } else if (kwIf("drop")) {
            dropIe = ifExists();
            boolean paren = symIf("(");
            do {
                drop.add(name());
            } while (symIf(","));
            if (paren) {
                expectSym(")");
            }
            if (kwIf("using")) {
                expectKw("timestamp");
                next();
            }
        } else if (kwIf("rename")) {
            do {
                String a = name();
                expectKw("to");
                rename.put(a, name());
            } while (kwIf("and"));
        } else if (kwIf("alter")) {
            String c = name();
            expectKw("type");
            alterType.put(c, typeText());
        } else if (kwIf("with")) {
            opts = properties();
        } else {
            throw err("no viable alternative at input '" + peek().text + "'");
        }
        return new AlterTable(q[0], q[1], add, drop, rename, opts, alterType, ifEx, addIne, dropIe);
    }

    private Stmt drop() {
        p++;
        if (kw("keyspace") || kw("schema")) {
            p++;
            boolean ie = ifExists();
            return new DropKeyspace(name(), ie);
        }
        if (kw("table") || kw("columnfamily")) {
            p++;
            boolean ie = ifExists();
            String[] q = qname();
            return new DropTable(q[0], q[1], ie);
        }
        if (kw("index")) {
            p++;
            boolean ie = ifExists();
            String[] q = qname();
            return new DropIndex(q[0], q[1], ie);
        }
        if (kw("type")) {
            p++;
            boolean ie = ifExists();
            String[] q = qname();
            return new DropType(q[0], q[1], ie);
        }
        String what = peek().lc();
        if (what.equals("role") || what.equals("user") || what.equals("materialized") || what.equals("function") || what.equals("aggregate")
                || what.equals("trigger")) {
            p = toks.size() - 1;
            return new Other("drop " + what);
        }
        throw err("no viable alternative at input '" + peek().text + "'");
    }

    // ------------------------------------------------------------------ types

    /** Consumes a type and returns its source text. */
    private String typeText() {
        int s = peek().pos;
        type("", (a, b) -> CqlType.TEXT);
        int e = peek().pos;
        return src.substring(s, e).trim();
    }

    private CqlType type(String defaultKs, BiFunction<String, String, CqlType> udts) {
        Tok t = peek();
        if (t.t == T.ID && t.lc().equals("frozen")) {
            p++;
            expectSym("<");
            CqlType inner = type(defaultKs, udts);
            expectSym(">");
            return inner.asFrozen();
        }
        if (t.t == T.ID) {
            String lc = t.lc();
            switch (lc) {
                case "list", "set" -> {
                    p++;
                    expectSym("<");
                    CqlType e = type(defaultKs, udts);
                    expectSym(">");
                    CqlType lt = lc.equals("list") ? CqlType.list(e, false) : CqlType.set(e, false);
                    check(e, lt);
                    return lt;
                }
                case "map" -> {
                    p++;
                    expectSym("<");
                    CqlType k = type(defaultKs, udts);
                    expectSym(",");
                    CqlType v = type(defaultKs, udts);
                    expectSym(">");
                    CqlType mt = CqlType.map(k, v, false);
                    check(k, mt);
                    check(v, mt);
                    return mt;
                }
                case "tuple" -> {
                    p++;
                    expectSym("<");
                    List<CqlType> comps = new ArrayList<>();
                    do {
                        comps.add(type(defaultKs, udts));
                    } while (symIf(","));
                    expectSym(">");
                    return CqlType.tuple(comps, true);
                }
                default -> {
                }
            }
            CqlType nat = CqlType.natives().get(lc);
            if (nat != null && !(peek(1).t == T.SYM && peek(1).text.equals("."))) {
                p++;
                return nat;
            }
        }
        // a user defined type: [ks.]name
        String a = anyName();
        String ks = defaultKs, n = a;
        if (symIf(".")) {
            ks = a;
            n = anyName();
        }
        CqlType u = udts.apply(ks, n);
        if (u == null) {
            throw CqlError.invalid("Unknown type " + ks + "." + n);
        }
        return u;
    }

    private static void check(CqlType e, CqlType outer) {
        if (e.isMultiCell()) {
            throw CqlError.invalid("Non-frozen collections are not allowed inside collections: " + outer.cql());
        }
        if (e.k == CqlType.K.COUNTER) {
            throw CqlError.invalid("Counters are not allowed inside collections");
        }
    }

    // ------------------------------------------------------------------ DML

    private Stmt insert() {
        p++;
        expectKw("into");
        String[] q = qname();
        List<String> cols = new ArrayList<>();
        List<Term> vals = new ArrayList<>();
        String json = null;
        boolean defaultUnset = false;
        Term jsonTerm = null;
        if (kwIf("json")) {
            Tok t = peek();
            if (t.t == T.STR) {
                json = t.text;
                p++;
            } else {
                jsonTerm = term();
                json = "\u0000bind";
            }
            if (kwIf("default")) {
                if (kwIf("unset")) {
                    defaultUnset = true;
                } else {
                    expectKw("null");
                }
            }
            if (jsonTerm != null) {
                vals.add(jsonTerm);
            }
        } else {
            expectSym("(");
            do {
                cols.add(name());
            } while (symIf(","));
            expectSym(")");
            expectKw("values");
            expectSym("(");
            do {
                vals.add(term());
            } while (symIf(","));
            expectSym(")");
            if (cols.size() != vals.size()) {
                throw CqlError.invalid("Unmatched column names/values");
            }
        }
        boolean ine = ifNotExists();
        Term[] us = using();
        return new Insert(q[0], q[1], cols, vals, json, ine, us[0], us[1], defaultUnset);
    }

    private Term[] using() {
        Term ttl = null, ts = null;
        if (kwIf("using")) {
            do {
                if (kwIf("ttl")) {
                    ttl = term();
                } else if (kwIf("timestamp")) {
                    ts = term();
                } else {
                    throw err("no viable alternative at input '" + peek().text + "'");
                }
            } while (kwIf("and"));
        }
        return new Term[] {ttl, ts};
    }

    private Stmt update() {
        p++;
        String[] q = qname();
        Term[] us = using();
        expectKw("set");
        List<Assign> as = new ArrayList<>();
        do {
            as.add(assign());
        } while (symIf(","));
        java.util.Map<String, Boolean> plainSeen = new java.util.HashMap<>();
        for (Assign a : as) {
            boolean plain = a.arith() == '=' && a.index() == null;
            Boolean prev = plainSeen.put(a.col(), plain);
            if (prev != null && (prev || plain)) {
                throw CqlError.syntax("Multiple incompatible setting of column " + a.col());
            }
            if (prev != null && !plain) {
                plainSeen.put(a.col(), false);
            }
        }
        expectKw("where");
        List<Relation> where = relations();
        boolean ifEx = false;
        List<Cond> conds = List.of();
        if (kwIf("if")) {
            if (kwIf("exists")) {
                ifEx = true;
            } else {
                conds = conditions();
            }
        }
        return new Update(q[0], q[1], us[0], us[1], as, where, ifEx, conds);
    }

    private Assign assign() {
        String col = name();
        Term index = null;
        String field = null;
        if (symIf("[")) {
            index = term();
            expectSym("]");
        } else if (symIf(".")) {
            field = name();
        }
        expectSym("=");
        Term rhs = term();
        if (index == null && field == null && rhs instanceof Arith a) {
            if (a.l() instanceof ColRef c && c.name().equals(col) && c.index() == null && c.field() == null) {
                return new Assign(col, null, null, a.r(), a.op(), false);
            }
            if (a.r() instanceof ColRef c && c.name().equals(col) && a.op() == '+' && c.index() == null && c.field() == null) {
                return new Assign(col, null, null, a.l(), '+', true);
            }
        }
        return new Assign(col, index, field, rhs, '=', false);
    }

    private Stmt delete() {
        p++;
        List<DelTarget> targets = new ArrayList<>();
        if (!kw("from")) {
            do {
                String c = name();
                Term idx = null;
                String f = null;
                if (symIf("[")) {
                    idx = term();
                    expectSym("]");
                } else if (symIf(".")) {
                    f = name();
                }
                targets.add(new DelTarget(c, idx, f));
            } while (symIf(","));
        }
        expectKw("from");
        String[] q = qname();
        Term ts = null;
        if (kwIf("using")) {
            expectKw("timestamp");
            ts = term();
        }
        expectKw("where");
        List<Relation> where = relations();
        boolean ifEx = false;
        List<Cond> conds = List.of();
        if (kwIf("if")) {
            if (kwIf("exists")) {
                ifEx = true;
            } else {
                conds = conditions();
            }
        }
        return new Delete(q[0], q[1], targets, ts, where, ifEx, conds);
    }

    private Stmt batch() {
        p++;
        String kind = "LOGGED";
        if (kwIf("unlogged")) {
            kind = "UNLOGGED";
        } else if (kwIf("counter")) {
            kind = "COUNTER";
        }
        expectKw("batch");
        Term ts = null;
        if (kwIf("using")) {
            expectKw("timestamp");
            ts = term();
        }
        List<Stmt> st = new ArrayList<>();
        while (!kw("apply")) {
            if (kw("insert")) {
                st.add(insert());
            } else if (kw("update")) {
                st.add(update());
            } else if (kw("delete")) {
                st.add(delete());
            } else {
                throw err("no viable alternative at input '" + peek().text + "'");
            }
            symIf(";");
        }
        p++;
        expectKw("batch");
        return new Batch(kind, ts, st);
    }

    // ------------------------------------------------------------------ SELECT

    private Stmt select() {
        p++;
        boolean json = false, distinct = false;
        if (kw("json") && !kwAt(1, "from") && !sym0(1, ",")) {
            p++;
            json = true;
        }
        if (kw("distinct")) {
            p++;
            distinct = true;
        }
        List<Selector> sels = new ArrayList<>();
        if (sym("*")) {
            p++;
            sels.add(new Selector(null, null, true));
        } else {
            do {
                Term e = term();
                String alias = null;
                if (kwIf("as")) {
                    alias = name();
                }
                sels.add(new Selector(e, alias, false));
            } while (symIf(","));
        }
        expectKw("from");
        String[] q = qname();
        List<Relation> where = List.of();
        if (kwIf("where")) {
            where = relations();
        }
        List<String> group = List.of();
        if (kw("group") && kwAt(1, "by")) {
            p += 2;
            group = new ArrayList<>();
            do {
                group.add(name());
            } while (symIf(","));
        }
        List<Order> order = List.of();
        if (kw("order") && kwAt(1, "by")) {
            p += 2;
            order = new ArrayList<>();
            do {
                String c = name();
                boolean desc = false;
                if (kwIf("desc")) {
                    desc = true;
                } else {
                    kwIf("asc");
                }
                order.add(new Order(c, desc));
            } while (symIf(","));
        }
        Term ppl = null, limit = null;
        if (kw("per") && kwAt(1, "partition") && kwAt(2, "limit")) {
            p += 3;
            ppl = limitTerm();
        }
        if (kwIf("limit")) {
            limit = limitTerm();
        }
        boolean af = false;
        if (kw("allow") && kwAt(1, "filtering")) {
            p += 2;
            af = true;
        }
        return new Ast.Select(q[0], q[1], distinct, json, sels, where, group, order, limit, ppl, af);
    }

    /** A LIMIT operand: an integer constant or a bind marker. */
    private Term limitTerm() {
        Tok t = peek();
        if (t.t == T.INT || t.t == T.QMARK || t.t == T.NAMED || sym("-") && peek(1).t == T.INT) {
            return unary();
        }
        throw err("no viable alternative at input '" + t.text + "'");
    }

    private boolean sym0(int k, String s) {
        Tok t = peek(k);
        return t.t == T.SYM && t.text.equals(s);
    }

    private List<Relation> relations() {
        List<Relation> rs = new ArrayList<>();
        do {
            rs.add(relation());
        } while (kwIf("and"));
        return rs;
    }

    private static final Set<String> OPS = Set.of("=", "<", "<=", ">", ">=", "!=");

    private String relOp() {
        Tok t = peek();
        if (t.t == T.SYM && OPS.contains(t.text)) {
            p++;
            return t.text;
        }
        if (kw("in")) {
            p++;
            return "in";
        }
        if (kw("contains")) {
            p++;
            if (kwIf("key")) {
                return "contains key";
            }
            return "contains";
        }
        if (kw("like")) {
            p++;
            return "like";
        }
        if (kw("is") && kwAt(1, "not")) {
            p += 2;
            expectKw("null");
            return "is not null";
        }
        throw err("no viable alternative at input '" + t.text + "'");
    }

    private Relation relation() {
        if (kw("token") && sym0(1, "(")) {
            p += 2;
            List<String> cols = new ArrayList<>();
            do {
                cols.add(name());
            } while (symIf(","));
            expectSym(")");
            String op = relOp();
            return new Relation(cols, true, null, op, term(), null);
        }
        if (sym("(")) {
            p++;
            List<String> cols = new ArrayList<>();
            do {
                cols.add(name());
            } while (symIf(","));
            expectSym(")");
            String op = relOp();
            if (op.equals("in")) {
                Relation in = inRelation(cols, null);
                return new Relation(in.cols(), false, null, in.op(), in.rhs(), in.inList(), true);
            }
            return new Relation(cols, false, null, op, term(), null, true);
        }
        String col = name();
        Term index = null;
        if (symIf("[")) {
            index = term();
            expectSym("]");
        }
        String op = relOp();
        if (op.equals("in")) {
            return inRelation(List.of(col), index);
        }
        if (op.equals("is not null")) {
            return new Relation(List.of(col), false, index, op, null, null);
        }
        return new Relation(List.of(col), false, index, op, term(), null);
    }

    private Relation inRelation(List<String> cols, Term index) {
        if (peek().t == T.QMARK || peek().t == T.NAMED) {
            return new Relation(cols, false, index, "in", term(), null);
        }
        expectSym("(");
        List<Term> items = new ArrayList<>();
        if (!sym(")")) {
            do {
                items.add(term());
            } while (symIf(","));
        }
        expectSym(")");
        return new Relation(cols, false, index, "in", null, items);
    }

    private List<Cond> conditions() {
        List<Cond> cs = new ArrayList<>();
        do {
            String col = name();
            Term idx = null;
            String field = null;
            if (symIf("[")) {
                idx = term();
                expectSym("]");
            } else if (symIf(".")) {
                field = name();
            }
            String op = relOp();
            if (op.equals("in")) {
                if (peek().t == T.QMARK || peek().t == T.NAMED) {
                    cs.add(new Cond(col, idx, field, op, term(), null));
                } else {
                    expectSym("(");
                    List<Term> items = new ArrayList<>();
                    if (!sym(")")) {
                        do {
                            items.add(term());
                        } while (symIf(","));
                    }
                    expectSym(")");
                    cs.add(new Cond(col, idx, field, op, null, items));
                }
            } else {
                cs.add(new Cond(col, idx, field, op, term(), null));
            }
        } while (kwIf("and"));
        return cs;
    }

    // ------------------------------------------------------------------ terms

    private Term term() {
        Term l = unary();
        while (sym("+") || sym("-") || sym("*") && !sym0(1, ")") || sym("/") || sym("%")) {
            char op = next().text.charAt(0);
            l = new Arith(op, l, unary());
        }
        return l;
    }

    private Term unary() {
        if (sym("-")) {
            p++;
            Tok t = peek();
            if (t.t == T.INT || t.t == T.FLOAT || t.t == T.DURATION) {
                p++;
                return new Lit(numberValue(t, true), false);
            }
            if (kw("infinity")) {
                p++;
                return new Lit(Double.NEGATIVE_INFINITY, false);
            }
            throw err("mismatched character ' ' expecting set null");
        }
        return primary();
    }

    private static Object numberValue(Tok t, boolean neg) {
        if (t.t == T.INT) {
            BigInteger b = new BigInteger(t.text);
            return neg ? b.negate() : b;
        }
        if (t.t == T.DURATION) {
            long[] d = CqlType.parseDuration((neg ? "-" : "") + t.text);
            return new long[][] {d};
        }
        BigDecimal d = new BigDecimal(t.text);
        if (neg && d.signum() == 0) {
            return Double.valueOf(-0.0);
        }
        return neg ? d.negate() : d;
    }

    private Term primary() {
        Tok t = peek();
        switch (t.t) {
            case STR:
                p++;
                return new Lit(t.text, true);
            case INT, FLOAT, DURATION:
                p++;
                return new Lit(numberValue(t, false), false);
            case HEX:
                p++;
                return new Lit(CqlType.unhex(t.text), false);
            case UUID:
                p++;
                return new Lit(UUID.fromString(t.text), false);
            case QMARK:
                p++;
                return new Bind(binds++, null);
            case NAMED:
                p++;
                return new Bind(binds++, t.text);
            case SYM:
                return symTerm(t);
            case ID, QID:
                return idTerm(t);
            default:
                throw err("no viable alternative at input '" + t.text + "'");
        }
    }

    private Term symTerm(Tok t) {
        if (t.text.equals("[")) {
            p++;
            List<Term> items = new ArrayList<>();
            if (!sym("]")) {
                do {
                    items.add(term());
                } while (symIf(","));
            }
            expectSym("]");
            return new ListLit(items);
        }
        if (t.text.equals("{")) {
            p++;
            if (symIf("}")) {
                return new BraceLit(List.of(), List.of(), Map.of());
            }
            // UDT literal: { name : term, ... } where the key is a bare identifier
            if ((peek().t == T.ID || peek().t == T.QID) && sym0(1, ":") ) {
                Map<String, Term> f = new LinkedHashMap<>();
                boolean udt = true;
                int save = p;
                do {
                    if (!(peek().t == T.ID || peek().t == T.QID) || !sym0(1, ":")) {
                        udt = false;
                        break;
                    }
                    String n = peek().t == T.QID ? peek().text : peek().lc();
                    p += 2;
                    f.put(n, term());
                } while (symIf(","));
                if (udt) {
                    expectSym("}");
                    return new BraceLit(List.of(), List.of(), f);
                }
                p = save;
            }
            Term first = term();
            if (symIf(":")) {
                List<Term[]> pairs = new ArrayList<>();
                pairs.add(new Term[] {first, term()});
                while (symIf(",")) {
                    Term k = term();
                    expectSym(":");
                    pairs.add(new Term[] {k, term()});
                }
                expectSym("}");
                return new BraceLit(List.of(), pairs, Map.of());
            }
            List<Term> items = new ArrayList<>();
            items.add(first);
            while (symIf(",")) {
                items.add(term());
            }
            expectSym("}");
            return new BraceLit(items, List.of(), Map.of());
        }
        if (t.text.equals("(")) {
            p++;
            // (type) term hint
            if (peek().t == T.ID && CqlType.natives().containsKey(peek().lc()) && sym0(1, ")")) {
                String ty = next().lc();
                p++;
                return new TypeHint(ty, unary());
            }
            List<Term> items = new ArrayList<>();
            items.add(term());
            if (sym(")")) {
                p++;
                return items.get(0);
            }
            while (symIf(",")) {
                items.add(term());
            }
            expectSym(")");
            return new TupleLit(items);
        }
        throw err("no viable alternative at input '" + t.text + "'");
    }

    private Term idTerm(Tok t) {
        if (t.t == T.ID) {
            switch (t.lc()) {
                case "true", "false" -> {
                    p++;
                    return new Lit(Boolean.parseBoolean(t.lc()), false);
                }
                case "null" -> {
                    p++;
                    return new Lit(null, false);
                }
                case "nan" -> {
                    p++;
                    return new Lit(Double.NaN, false);
                }
                case "infinity" -> {
                    p++;
                    return new Lit(Double.POSITIVE_INFINITY, false);
                }
                case "cast" -> {
                    if (sym0(1, "(")) {
                        p += 2;
                        Term x = term();
                        expectKw("as");
                        String ty = next().lc();
                        expectSym(")");
                        return new Cast(x, ty);
                    }
                }
                default -> {
                }
            }
            if (t.lc().matches("p(\\d+y)?(\\d+m)?(\\d+w)?(\\d+d)?(t(\\d+h)?(\\d+m)?(\\d+s)?)?|p\\d{4}-\\d{2}-\\d{2}t\\d{2}:\\d{2}:\\d{2}") && t.text.length() > 1
                    && !sym0(1, "(") && !sym0(1, ".")) {
                p++;
                return new Lit(new long[][] {CqlType.parseDuration(t.text)}, false);
            }
        }
        // function call, possibly ks.func
        if (sym0(1, "(") || sym0(1, ".") && (peek(2).t == T.ID || peek(2).t == T.QID) && sym0(3, "(")) {
            String ks = null;
            String fn;
            if (sym0(1, ".")) {
                ks = anyName();
                p++;
                fn = anyName();
            } else {
                fn = anyName();
            }
            expectSym("(");
            List<Term> args = new ArrayList<>();
            boolean star = false;
            if (sym("*")) {
                p++;
                star = true;
            } else if (!sym(")")) {
                do {
                    args.add(term());
                } while (symIf(","));
            }
            expectSym(")");
            return new Func(ks, fn, args, star);
        }
        String col = t.t == T.QID ? t.text : t.lc();
        if (t.t == T.ID && RESERVED.contains(t.lc())) {
            throw err("no viable alternative at input '" + t.text + "'");
        }
        p++;
        if (symIf("[")) {
            Term idx = term();
            expectSym("]");
            return new ColRef(col, idx, null);
        }
        if (sym(".") && (peek(1).t == T.ID || peek(1).t == T.QID)) {
            p++;
            String path = anyName();
            while (sym(".") && (peek(1).t == T.ID || peek(1).t == T.QID)) {
                p++;
                path += "." + anyName();
            }
            return new ColRef(col, null, path);
        }
        return new ColRef(col, null, null);
    }

    /** Number of bind markers of {@code cql}. */
    static int countBinds(String cql) {
        Parser ps = new Parser(cql);
        ps.statement();
        return ps.binds;
    }
}
