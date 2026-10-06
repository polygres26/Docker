package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Decides where an {@code UPDATE} or {@code DELETE} of a declaratively sharded table ({@code WARP_TABLE_SHARDS}) has to run.
 *
 * <ul>
 *   <li>{@link Keyed}: the {@code WHERE} clause is a plain conjunction that contains {@code key = value} (a literal or a {@code ?},
 *       {@code $n}, {@code :n} bind), so exactly one shard owns every row it can touch.</li>
 *   <li>{@link Broadcast}: no usable key predicate (none at all, only under an {@code OR}, only inside a subquery, or a range), so rows
 *       on any shard may match and the statement has to run on all of them.</li>
 *   <li>{@link KeyChange}: the {@code SET} list assigns the shard key itself, which would have to move the row to another shard.</li>
 * </ul>
 *
 * <p>Deliberately more conservative than the text matcher a {@code SELECT} uses: that one finds {@code key = value} anywhere, so
 * {@code UPDATE t SET key = 9 WHERE id = 1} was routed by the new value and {@code WHERE key = 5 OR id = 7} by the 5, each silently
 * skipping rows on the other shards. A write that cannot be proven to touch one shard goes to all of them.
 */
final class WriteShardKey {

    sealed interface Result permits NotApplicable, Keyed, Broadcast, KeyChange {
    }

    /** Not an UPDATE or DELETE of this table. */
    record NotApplicable() implements Result {
    }

    record Keyed(String keyValue) implements Result {
    }

    record Broadcast() implements Result {
    }

    record KeyChange(String column) implements Result {
    }

    private WriteShardKey() {
    }

    static Result analyze(String sql, String table, String column, List<Object> binds) {
        if (sql == null) {
            return new NotApplicable();
        }
        Masked q = Masked.of(sql);
        int p = q.skipBlank(0);
        boolean update;
        if (q.wordAt(p, "UPDATE")) {
            update = true;
            p += 6;
        } else if (q.wordAt(p, "DELETE")) {
            update = false;
            p += 6;
        } else {
            return new NotApplicable();
        }
        p = q.skipBlank(p);
        // modifiers: MySQL LOW_PRIORITY / IGNORE / QUICK, PostgreSQL ONLY, and FROM (DELETE FROM, optional on SQL Server)
        boolean moved = true;
        while (moved) {
            moved = false;
            for (String w : new String[] {"LOW_PRIORITY", "IGNORE", "QUICK", "ONLY", "FROM"}) {
                if (q.wordAt(p, w)) {
                    p = q.skipBlank(p + w.length());
                    moved = true;
                }
            }
        }
        int[] name = q.qualifiedName(p);
        if (name == null || !unquote(lastPart(sql.substring(name[0], name[1]))).equalsIgnoreCase(lastPart(table))) {
            return new NotApplicable();
        }
        int after = name[1];
        int where = q.keywordAtDepthZero("WHERE", after);
        if (update) {
            int set = q.keywordAtDepthZero("SET", after);
            if (set < 0) {
                return new Broadcast();
            }
            int setEnd = q.firstOf(set + 3, where < 0 ? sql.length() : where, "FROM", "WHERE");
            for (String assignment : q.splitTopLevelCommas(set + 3, setEnd)) {
                int eq = assignment.indexOf('=');
                if (eq > 0 && lastPart(unquote(assignment.substring(0, eq).strip())).equalsIgnoreCase(column)) {
                    return new KeyChange(column);
                }
            }
        }
        if (where < 0) {
            return new Broadcast();
        }
        int whereStart = where + 5;
        if (q.keywordAtDepthZero("OR", whereStart) >= 0) {
            return new Broadcast();
        }
        Matcher m = Pattern.compile("(?i)(?:^|[^\\w$.\"`\\]])((?:[\\w$\"`\\[\\]]+\\.)*[\"`\\[]?" + Pattern.quote(column)
                + "[\"`\\]]?)\\s*=\\s*('((?:[^']|'')*)'|-?\\d+(?:\\.\\d+)?|\\?|\\$\\d+|:\\d+)").matcher(sql.substring(0, sql.length()));
        int from = whereStart;
        while (m.find(from)) {
            int predicateStart = m.start(1);
            from = m.end();
            if (predicateStart < whereStart || q.depthAt(predicateStart) != 0 || q.insideLiteral(predicateStart)) {
                continue;
            }
            String v = m.group(2);
            if (m.group(3) != null) {
                return new Keyed(m.group(3).replace("''", "'"));
            }
            if (v.equals("?")) {
                return bound(binds, q.placeholdersBefore(m.start(2)));
            }
            if (v.startsWith("$") || v.startsWith(":")) {
                return bound(binds, Integer.parseInt(v.substring(1)) - 1);
            }
            return new Keyed(v);
        }
        return new Broadcast();
    }

    private static Result bound(List<Object> binds, int index) {
        if (binds == null || index < 0 || index >= binds.size() || binds.get(index) == null) {
            return new Broadcast(); // cannot prove a single shard
        }
        return new Keyed(String.valueOf(binds.get(index)));
    }

    private static String lastPart(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(dot + 1);
    }

    private static String unquote(String id) {
        String t = id.strip();
        if (t.length() >= 2 && (t.charAt(0) == '"' || t.charAt(0) == '`' || t.charAt(0) == '[')) {
            return t.substring(1, t.length() - 1);
        }
        return t;
    }

    /**
     * The SQL with every string literal and comment blanked out (same length), so keywords, parentheses, commas and placeholders can be
     * found without being fooled by their appearance inside text, plus the parenthesis depth at each position.
     */
    private static final class Masked {
        private final String original;
        private final char[] masked;
        private final int[] depth;
        private final boolean[] literal;

        private Masked(String original, char[] masked, int[] depth, boolean[] literal) {
            this.original = original;
            this.masked = masked;
            this.depth = depth;
            this.literal = literal;
        }

        static Masked of(String sql) {
            char[] m = sql.toCharArray();
            int[] d = new int[m.length + 1];
            boolean[] lit = new boolean[m.length];
            int level = 0;
            int i = 0;
            while (i < m.length) {
                char c = m[i];
                if (c == '\'') {
                    int j = i + 1;
                    while (j < m.length) {
                        if (m[j] == '\'') {
                            if (j + 1 < m.length && m[j + 1] == '\'') {
                                j += 2;
                                continue;
                            }
                            break;
                        }
                        j++;
                    }
                    int end = Math.min(j, m.length - 1);
                    for (int k = i; k <= end; k++) {
                        m[k] = '\u0001';
                        lit[k] = true;
                        d[k] = level;
                    }
                    i = end + 1;
                    continue;
                }
                if (c == '-' && i + 1 < m.length && m[i + 1] == '-') {
                    while (i < m.length && sql.charAt(i) != '\n') {
                        m[i] = ' ';
                        lit[i] = true;
                        d[i] = level;
                        i++;
                    }
                    continue;
                }
                if (c == '/' && i + 1 < m.length && m[i + 1] == '*') {
                    int end = sql.indexOf("*/", i + 2);
                    int stop = end < 0 ? m.length : end + 2;
                    for (int k = i; k < stop; k++) {
                        m[k] = ' ';
                        lit[k] = true;
                        d[k] = level;
                    }
                    i = stop;
                    continue;
                }
                if (c == '(') {
                    d[i] = level;
                    level++;
                } else if (c == ')') {
                    level = Math.max(0, level - 1);
                    d[i] = level;
                } else {
                    d[i] = level;
                }
                i++;
            }
            d[m.length] = level;
            return new Masked(sql, m, d, lit);
        }

        int depthAt(int i) {
            return depth[Math.min(i, depth.length - 1)];
        }

        boolean insideLiteral(int i) {
            return i < literal.length && literal[i];
        }

        int skipBlank(int p) {
            while (p < masked.length && Character.isWhitespace(masked[p])) {
                p++;
            }
            return p;
        }

        boolean wordAt(int p, String word) {
            int end = p + word.length();
            return end <= masked.length && new String(masked, p, word.length()).equalsIgnoreCase(word)
                    && (end == masked.length || !isWordChar(masked[end])) && (p == 0 || !isWordChar(masked[p - 1]));
        }

        private static boolean isWordChar(char c) {
            return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
        }

        /** Start and end of {@code a.b.c} (each part optionally quoted) at {@code p}, or null. */
        int[] qualifiedName(int p) {
            int start = p;
            while (true) {
                if (p >= masked.length) {
                    return null;
                }
                char c = masked[p];
                if (c == '"' || c == '`' || c == '[') {
                    int end = original.indexOf(c == '[' ? ']' : c, p + 1);
                    if (end < 0) {
                        return null;
                    }
                    p = end + 1;
                } else if (isWordChar(c)) {
                    while (p < masked.length && isWordChar(masked[p])) {
                        p++;
                    }
                } else {
                    return null;
                }
                if (p < masked.length && masked[p] == '.') {
                    p++;
                } else {
                    return new int[] {start, p};
                }
            }
        }

        /** Index of {@code word} outside literals and parentheses at or after {@code from}, or -1. */
        int keywordAtDepthZero(String word, int from) {
            int base = depthAt(from);
            for (int i = Math.max(from, 0); i + word.length() <= masked.length; i++) {
                if (depth[i] == base && wordAt(i, word)) {
                    return i;
                }
            }
            return -1;
        }

        /** The first of {@code words} found at the same depth between {@code from} and {@code to}, or {@code to}. */
        int firstOf(int from, int to, String... words) {
            int base = depthAt(from);
            for (int i = from; i < to && i < masked.length; i++) {
                if (depth[i] == base) {
                    for (String w : words) {
                        if (wordAt(i, w)) {
                            return i;
                        }
                    }
                }
            }
            return to;
        }

        /** The original text between {@code from} and {@code to} split at top-level commas. */
        List<String> splitTopLevelCommas(int from, int to) {
            List<String> parts = new ArrayList<>();
            int base = depthAt(from);
            int start = from;
            for (int i = from; i < to && i < masked.length; i++) {
                if (masked[i] == ',' && depth[i] == base) {
                    parts.add(original.substring(start, i));
                    start = i + 1;
                }
            }
            parts.add(original.substring(start, Math.min(to, original.length())));
            return parts;
        }

        /** How many {@code ?} placeholders precede {@code index} in the statement (outside literals and comments). */
        int placeholdersBefore(int index) {
            int n = 0;
            for (int i = 0; i < index && i < masked.length; i++) {
                if (masked[i] == '?') {
                    n++;
                }
            }
            return n;
        }
    }
}
