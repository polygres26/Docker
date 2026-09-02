package com.sayonora.wire.core;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class SqlLiterals {

    private SqlLiterals() {
    }

    static String replaceOutsideLiterals(String sql, Pattern pattern, Function<Matcher, String> replacementFor) {
        StringBuilder out = new StringBuilder();
        Matcher matcher = pattern.matcher(sql);
        int last = 0;
        while (matcher.find(last)) {
            if (isInsideStringLiteral(sql, matcher.start())) {
                out.append(sql, last, matcher.end());
                last = matcher.end();
                continue;
            }
            out.append(sql, last, matcher.start());
            out.append(replacementFor.apply(matcher));
            last = matcher.end();
        }
        out.append(sql.substring(last));
        return out.toString();
    }

    /** {@code true} if {@code pattern} matches anywhere in {@code sql} outside a string literal --
     * same literal-awareness as {@link #replaceOutsideLiterals}, for callers that only need a
     * yes/no check (e.g. "does this still contain a construct I don't know how to rewrite?"). */
    static boolean findOutsideLiterals(String sql, Pattern pattern) {
        Matcher matcher = pattern.matcher(sql);
        int last = 0;
        while (matcher.find(last)) {
            if (!isInsideStringLiteral(sql, matcher.start())) {
                return true;
            }
            last = matcher.end();
        }
        return false;
    }

    static boolean isInsideStringLiteral(String sql, int position) {
        boolean inString = false;
        int i = 0;
        while (i < position) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i += 2;
                    continue;
                }
                inString = !inString;
            }
            i++;
        }
        return inString;
    }
}
