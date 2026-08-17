package com.polygres.wire.core.access;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Appends a bound {@code <column> = ?} row-filter predicate to a SQL statement's {@code WHERE}
 * clause (creating one if none exists) — the mechanism behind {@link AccessPolicy.RowFilter}
 * enforcement in {@code AccessControlStage}, matching Cube.js's {@code queryRewrite} pattern of
 * deriving the filter before the statement reaches a backend. Uses a bind parameter, never string
 * interpolation, so the injected value can never itself be an injection vector.
 *
 * <p>Regex/text-level, not a real SQL parser — same documented fidelity {@code RouterStage}'s
 * schema matching and {@link SqlTableReferences} already accept: it finds the first top-level
 * {@code GROUP BY}/{@code HAVING}/{@code ORDER BY}/{@code LIMIT}/{@code OFFSET}/{@code FETCH}
 * keyword textually and inserts before it, honoring an existing {@code WHERE} by ANDing rather
 * than replacing. <b>Known gap</b>: doesn't track parenthesis depth, so a subquery containing one
 * of those keywords (e.g. {@code WHERE id IN (SELECT id FROM t ORDER BY x LIMIT 1)}) can produce
 * an insertion point earlier than intended — flagged as a real limitation, not silently assumed
 * away; {@code docs/design/end-user-data-access-security.md} §3.6 is exactly why native RLS/VPD
 * pass-through exists as the preferred mode where available, immune to this class of gap entirely.
 */
public final class WhereClauseInjector {

    private WhereClauseInjector() {
    }

    private static final Pattern CLAUSE_BOUNDARY = Pattern.compile(
            "\\b(group\\s+by|having|order\\s+by|limit|offset|fetch\\s+(?:first|next))\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern WHERE_KEYWORD = Pattern.compile("\\bwhere\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern PLACEHOLDER = Pattern.compile("\\?");

    public record Injected(String sqlText, List<Object> bindParams) {
    }

    /** Injects {@code <filterColumn> = ?} bound to {@code value}, returning the rewritten SQL and bind list (original {@code bindParams} untouched, a new list is returned). */
    public static Injected inject(String sqlText, List<Object> bindParams, String filterColumn, Object value) {
        String trimmed = sqlText.stripTrailing();
        boolean hadTrailingSemicolon = trimmed.endsWith(";");
        String body = hadTrailingSemicolon ? trimmed.substring(0, trimmed.length() - 1) : trimmed;

        int insertAt = findInsertionPoint(body);
        boolean hasWhere = WHERE_KEYWORD.matcher(body.substring(0, insertAt)).find();
        String predicate = (hasWhere ? " AND " : " WHERE ") + filterColumn + " = ?";

        // Trim whitespace on both sides of the insertion point so the predicate's own leading
        // space (and, if there's trailing content, one separating space before it) don't double
        // up into "  WHERE" / "?GROUP BY".
        String before = body.substring(0, insertAt).stripTrailing();
        String after = body.substring(insertAt).stripLeading();
        String newSql = before + predicate + (after.isEmpty() ? "" : " " + after) + (hadTrailingSemicolon ? ";" : "");

        int placeholderIndexBeforeInsertion = countPlaceholders(body.substring(0, insertAt));
        List<Object> newBinds = new ArrayList<>(bindParams);
        newBinds.add(placeholderIndexBeforeInsertion, value);

        return new Injected(newSql, newBinds);
    }

    private static int findInsertionPoint(String body) {
        Matcher matcher = CLAUSE_BOUNDARY.matcher(body);
        return matcher.find() ? matcher.start() : body.length();
    }

    private static int countPlaceholders(String sqlPrefix) {
        int count = 0;
        Matcher matcher = PLACEHOLDER.matcher(sqlPrefix);
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
