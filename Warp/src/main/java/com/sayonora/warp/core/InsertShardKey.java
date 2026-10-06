package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Finds the shard-key value of every row of an {@code INSERT INTO <table> (<columns>) VALUES (...)[, (...)]} so a declaratively sharded
 * table ({@code WARP_TABLE_SHARDS}) can send the insert to the shard that owns it. Predicates ({@code column = value}) are all
 * {@link ValueShardLiteralMatcher} understands, so without this an insert into a sharded table fell through to the default backend, which
 * is not a shard (or is only one of them): the row then sat on the wrong node, invisible to every keyed read.
 *
 * <p>Dialect-neutral on purpose: the router sees the client's own SQL, so quoting ({@code "x"}, {@code `x`}, {@code [x]}), string literals
 * ({@code 'it''s'}, {@code N'x'}), comments, {@code VALUE}/{@code VALUES}, and the {@code ?}, {@code $n} and {@code :n} bind forms of
 * PostgreSQL, MySQL, SQL Server and Oracle are all read. Anything that cannot be resolved with certainty is {@link Unroutable} with a
 * reason, never guessed: no column list, {@code INSERT ... SELECT}, a shard key that is an expression, {@code NULL} or {@code DEFAULT}, or a
 * named bind.
 */
final class InsertShardKey {

    /** What the statement is, as far as routing it by shard key goes. */
    sealed interface Result permits NotApplicable, Routed, Unroutable {
    }

    /** Not an INSERT into the sharded table, so this analysis has nothing to say. */
    record NotApplicable() implements Result {
    }

    /** One shard-key value per inserted row, in row order. */
    record Routed(List<String> keyValues) implements Result {
    }

    record Unroutable(String reason) implements Result {
    }

    private InsertShardKey() {
    }

    static Result analyze(String sql, String table, String column, List<Object> binds) {
        Scanner s = new Scanner(sql == null ? "" : sql);
        s.skipBlank();
        if (!s.keyword("INSERT")) {
            return new NotApplicable();
        }
        // MySQL modifiers, then INTO (optional on MySQL and SQL Server)
        while (s.keyword("LOW_PRIORITY") || s.keyword("DELAYED") || s.keyword("HIGH_PRIORITY") || s.keyword("IGNORE")) {
            // modifiers carry no routing information
        }
        s.keyword("INTO");
        String target = s.qualifiedName();
        if (target == null || !lastPart(target).equalsIgnoreCase(lastPart(table))) {
            return new NotApplicable();
        }
        s.skipBlank();
        List<String> columns = null;
        if (s.peek() == '(') {
            if (s.looksLikeSelectInParens()) {
                return new Unroutable("INSERT ... SELECT cannot be routed by a shard key");
            }
            columns = s.identifierList();
            if (columns == null) {
                return new Unroutable("the column list could not be read");
            }
        }
        if (!(s.keyword("VALUES") || s.keyword("VALUE"))) {
            return new Unroutable("only INSERT ... VALUES can be routed by a shard key (not INSERT ... SELECT or DEFAULT VALUES)");
        }
        if (columns == null) {
            return new Unroutable("the INSERT has no column list, so the position of " + column + " is unknown");
        }
        int keyIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equalsIgnoreCase(column)) {
                keyIndex = i;
            }
        }
        if (keyIndex < 0) {
            return new Unroutable("the column list does not include the shard key " + column);
        }
        List<String> values = new ArrayList<>();
        int placeholders = 0; // '?' seen so far in the statement, outside literals and comments
        while (true) {
            s.skipBlank();
            if (s.peek() != '(') {
                return new Unroutable("expected a row of values");
            }
            List<String> row = s.valueRow();
            if (row == null) {
                return new Unroutable("a row of values could not be read");
            }
            if (row.size() != columns.size()) {
                return new Unroutable("a row has " + row.size() + " values for " + columns.size() + " columns");
            }
            for (int i = 0; i < row.size(); i++) {
                String expr = row.get(i);
                int here = placeholders;
                placeholders += countPlaceholders(expr);
                if (i == keyIndex) {
                    Object[] decoded = decode(expr, here, binds);
                    if (decoded[1] != null) {
                        return new Unroutable((String) decoded[1] + " (shard key " + column + ")");
                    }
                    values.add((String) decoded[0]);
                }
            }
            s.skipBlank();
            if (s.peek() == ',') {
                s.next();
                continue;
            }
            break;
        }
        return new Routed(values);
    }

    private static String lastPart(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(dot + 1);
    }

    /** Removes {@code --} and block comments that sit outside string literals. */
    private static String stripComments(String text) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\'') {
                int j = i + 1;
                while (j < text.length()) {
                    if (text.charAt(j) == '\'') {
                        if (j + 1 < text.length() && text.charAt(j + 1) == '\'') {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                out.append(text, i, Math.min(j + 1, text.length()));
                i = j + 1;
            } else if (c == '-' && i + 1 < text.length() && text.charAt(i + 1) == '-') {
                while (i < text.length() && text.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < text.length() && text.charAt(i + 1) == '*') {
                int end = text.indexOf("*/", i + 2);
                i = end < 0 ? text.length() : end + 2;
                out.append(' ');
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static int countPlaceholders(String expr) {
        int n = 0;
        Scanner s = new Scanner(expr);
        while (s.hasMore()) {
            char c = s.peek();
            if (c == '\'') {
                s.skipString();
            } else if (c == '?') {
                n++;
                s.next();
            } else {
                s.next();
            }
        }
        return n;
    }

    /** @return {value, null} or {null, reason} */
    private static Object[] decode(String raw, int placeholderOrdinal, List<Object> binds) {
        String e = stripComments(raw).strip();
        if (e.matches("-?\\d+(\\.\\d+)?")) {
            return new Object[] {e, null};
        }
        if (e.length() >= 2 && (e.charAt(0) == '\'' || ((e.charAt(0) == 'N' || e.charAt(0) == 'n') && e.charAt(1) == '\''))) {
            int open = e.indexOf('\'');
            if (e.endsWith("'") && e.length() > open + 1) {
                String body = e.substring(open + 1, e.length() - 1);
                // a closing quote that is really an escaped pair inside a longer expression ('a' || 'b') is not a plain literal
                if (!body.replace("''", "").contains("'")) {
                    return new Object[] {body.replace("''", "'"), null};
                }
            }
            return new Object[] {null, "the value is an expression, not a plain literal"};
        }
        if (e.equals("?")) {
            return bind(binds, placeholderOrdinal, "?");
        }
        if (e.matches("\\$\\d+")) {
            return bind(binds, Integer.parseInt(e.substring(1)) - 1, e);
        }
        if (e.matches(":\\d+")) {
            return bind(binds, Integer.parseInt(e.substring(1)) - 1, e);
        }
        if (e.matches("[:@]\\w+")) {
            return new Object[] {null, "the value is the named bind " + e + ", which cannot be matched to a bind parameter by position"};
        }
        if (e.equalsIgnoreCase("null") || e.equalsIgnoreCase("default")) {
            return new Object[] {null, "the value is " + e.toUpperCase(Locale.ROOT)};
        }
        return new Object[] {null, "the value is an expression (use a literal or a bind parameter)"};
    }

    private static Object[] bind(List<Object> binds, int index, String what) {
        if (binds == null || index < 0 || index >= binds.size()) {
            return new Object[] {null, "bind parameter " + what + " has no value to route on"};
        }
        Object v = binds.get(index);
        return v == null ? new Object[] {null, "the bind parameter " + what + " is NULL"} : new Object[] {String.valueOf(v), null};
    }

    /** A cursor over SQL text that knows quotes, comments and nesting. */
    private static final class Scanner {
        private final String t;
        private int p;

        Scanner(String text) {
            this.t = text;
        }

        boolean hasMore() {
            return p < t.length();
        }

        char peek() {
            return p < t.length() ? t.charAt(p) : '\0';
        }

        char next() {
            return t.charAt(p++);
        }

        void skipBlank() {
            while (p < t.length()) {
                char c = t.charAt(p);
                if (Character.isWhitespace(c)) {
                    p++;
                } else if (c == '-' && p + 1 < t.length() && t.charAt(p + 1) == '-') {
                    while (p < t.length() && t.charAt(p) != '\n') {
                        p++;
                    }
                } else if (c == '/' && p + 1 < t.length() && t.charAt(p + 1) == '*') {
                    int end = t.indexOf("*/", p + 2);
                    p = end < 0 ? t.length() : end + 2;
                } else {
                    return;
                }
            }
        }

        /** Consumes {@code word} if it is next, as a whole word, ignoring case. */
        boolean keyword(String word) {
            skipBlank();
            int end = p + word.length();
            if (end <= t.length() && t.regionMatches(true, p, word, 0, word.length())
                    && (end == t.length() || !(Character.isLetterOrDigit(t.charAt(end)) || t.charAt(end) == '_'))) {
                p = end;
                return true;
            }
            return false;
        }

        /** {@code a.b.c} with each part optionally quoted; returns it with the quotes removed, or null. */
        String qualifiedName() {
            StringBuilder out = new StringBuilder();
            while (true) {
                skipBlank();
                String part = identifier();
                if (part == null) {
                    return null;
                }
                out.append(part);
                if (peek() == '.') {
                    next();
                    out.append('.');
                } else {
                    return out.toString();
                }
            }
        }

        String identifier() {
            skipBlank();
            char c = peek();
            if (c == '"' || c == '`' || c == '[') {
                char close = c == '[' ? ']' : c;
                int end = t.indexOf(close, p + 1);
                if (end < 0) {
                    return null;
                }
                String name = t.substring(p + 1, end);
                p = end + 1;
                return name;
            }
            int start = p;
            while (p < t.length() && (Character.isLetterOrDigit(t.charAt(p)) || t.charAt(p) == '_' || t.charAt(p) == '$'
                    || t.charAt(p) == '#')) {
                p++;
            }
            return p == start ? null : t.substring(start, p);
        }

        boolean looksLikeSelectInParens() {
            int save = p;
            next(); // (
            boolean select = keyword("SELECT") || keyword("WITH");
            p = save;
            return select;
        }

        /** {@code (a, "b", [c])} */
        List<String> identifierList() {
            if (peek() != '(') {
                return null;
            }
            next();
            List<String> names = new ArrayList<>();
            while (true) {
                String id = identifier();
                if (id == null) {
                    return null;
                }
                names.add(id);
                skipBlank();
                char c = peek();
                if (c == ',') {
                    next();
                } else if (c == ')') {
                    next();
                    return names;
                } else {
                    return null;
                }
            }
        }

        /** {@code (expr, expr, ...)}, each expression as its raw text; nested parentheses, strings and comments are skipped over. */
        List<String> valueRow() {
            next(); // (
            List<String> exprs = new ArrayList<>();
            int depth = 0;
            int start = p;
            while (p < t.length()) {
                char c = t.charAt(p);
                if (c == '\'') {
                    skipString();
                } else if (c == '"' || c == '`') {
                    int end = t.indexOf(c, p + 1);
                    p = end < 0 ? t.length() : end + 1;
                } else if (c == '-' && p + 1 < t.length() && t.charAt(p + 1) == '-' || c == '/' && p + 1 < t.length() && t.charAt(p + 1) == '*') {
                    skipBlank();
                } else if (c == '(') {
                    depth++;
                    p++;
                } else if (c == ')') {
                    if (depth == 0) {
                        exprs.add(t.substring(start, p));
                        p++;
                        return exprs;
                    }
                    depth--;
                    p++;
                } else if (c == ',' && depth == 0) {
                    exprs.add(t.substring(start, p));
                    p++;
                    start = p;
                } else {
                    p++;
                }
            }
            return null;
        }

        void skipString() {
            p++; // opening quote
            while (p < t.length()) {
                if (t.charAt(p) == '\'') {
                    if (p + 1 < t.length() && t.charAt(p + 1) == '\'') {
                        p += 2;
                        continue;
                    }
                    p++;
                    return;
                }
                p++;
            }
        }
    }
}
