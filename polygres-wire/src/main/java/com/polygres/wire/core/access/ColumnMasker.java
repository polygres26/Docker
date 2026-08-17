package com.polygres.wire.core.access;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implements {@link AccessPolicy.OnViolation#MASK} — Omni's {@code mask_unless_access_grants}
 * pattern: instead of rejecting a statement outright when it references a column the caller isn't
 * entitled to, rewrite that column reference to a literal ({@code NULL}) so the rest of the query
 * still runs. See {@code docs/design/end-user-data-access-security.md} §3.3 for why this is opt-in
 * per grant rather than the default (silently returning {@code NULL} instead of an error can
 * itself surprise a caller who genuinely needed the value).
 *
 * <p>Same regex-level fidelity as the rest of this package — matches a bare column reference
 * (optionally table/alias-qualified: {@code t.salary}, {@code salary}) as a whole identifier, not
 * inside a string literal or another identifier ({@code salary_band} is untouched by a
 * {@code salary} grant). Doesn't attempt to rewrite {@code SELECT *} into an explicit column list
 * — a masked column reached only via {@code *} is a known gap, matching this class's declared
 * text-level fidelity rather than requiring a real SQL parser.
 */
public final class ColumnMasker {

    private ColumnMasker() {
    }

    /** Replaces every bare or qualified reference to {@code column} in {@code sqlText} with {@code NULL}. */
    public static String mask(String sqlText, String column) {
        Pattern reference = Pattern.compile("(?:\\b[\\w$]+\\.)?\\b" + Pattern.quote(column) + "\\b", Pattern.CASE_INSENSITIVE);
        Matcher matcher = reference.matcher(sqlText);
        StringBuilder result = new StringBuilder();
        int last = 0;
        while (matcher.find()) {
            result.append(sqlText, last, matcher.start()).append("NULL");
            last = matcher.end();
        }
        result.append(sqlText.substring(last));
        return result.toString();
    }
}
