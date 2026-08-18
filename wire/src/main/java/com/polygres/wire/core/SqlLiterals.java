package com.polygres.wire.core;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared quote-literal-aware regex rewriting, factored out of {@link DbLinkStage} (which has its
 * own private copy of the same {@code isInsideStringLiteral} check predating this class) for reuse
 * by {@link DialectTranslationStage}. Not merged into {@code DbLinkStage} itself to avoid touching
 * a working, already-verified file for a refactor with no behavior change.
 */
final class SqlLiterals {

    private SqlLiterals() {
    }

    /**
     * Replaces every non-overlapping match of {@code pattern} in {@code sql}, skipping any match
     * that starts inside a single-quoted string literal (tracking {@code ''} as an escaped quote,
     * standard SQL) — so a rewrite rule never mangles a string constant that happens to contain a
     * keyword it's looking for (e.g. an Oracle error message literal containing the word
     * {@code "sysdate"}).
     */
    static String replaceOutsideLiterals(String sql, Pattern pattern, Function<Matcher, String> replacementFor) {
        StringBuilder out = new StringBuilder();
        Matcher matcher = pattern.matcher(sql);
        int last = 0;
        while (matcher.find(last)) {
            if (isInsideStringLiteral(sql, matcher.start())) {
                out.append(sql, last, matcher.end()); // preserve the skipped (in-literal) span verbatim
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

    static boolean isInsideStringLiteral(String sql, int position) {
        boolean inString = false;
        int i = 0;
        while (i < position) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i += 2; // escaped '' inside a string literal -- not a terminator
                    continue;
                }
                inString = !inString;
            }
            i++;
        }
        return inString;
    }
}
