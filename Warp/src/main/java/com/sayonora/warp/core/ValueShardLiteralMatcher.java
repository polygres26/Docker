package com.sayonora.warp.core;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts a sharding column's value from raw SQL text, for {@link RouterStage.ValueShardColumnRule}
 * and {@link RouterStage.TableShardRule}. Regex-on-raw-SQL, not a real parser -- deliberately
 * matching the same scope {@link RouterStage}'s existing {@code SchemaRule}/{@code ShardRule}
 * already operate at (see {@code RouterStage.fromConfig}), not a new, higher bar.
 */
final class ValueShardLiteralMatcher {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\?");

    private ValueShardLiteralMatcher() {
    }

    /** Recognizes {@code column = 'text'} and {@code column = 123} (equality only, not {@code IN
     * (...)}, ranges, or any other operator -- those need the value-shard column's value to be
     * genuinely singular to route correctly, and equality is by far the common case for a sharding
     * predicate). Does not distinguish a real WHERE-clause comparison from the same shape appearing
     * inside a string literal or comment, exactly like the pre-existing schema/shard-table regexes.
     */
    static String findLiteralValue(String sql, String columnName) {
        Pattern pattern = Pattern.compile(
                "(?i)\\b" + Pattern.quote(columnName) + "\\b\\s*=\\s*(?:'((?:[^']|'')*)'|(-?\\d+(?:\\.\\d+)?))");
        Matcher matcher = pattern.matcher(sql);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(1) != null ? matcher.group(1).replace("''", "'") : matcher.group(2);
    }

    /**
     * Real bug, found via the same competitive-comparison audit {@link RouterStage.ValueShardRule}'s
     * own javadoc already documents (that one fixed the GLOBAL value-shard rule's literal-vs-bind
     * gap; this closes the identical gap for {@code WARP_TABLE_SHARDS}'s per-table rule, which had
     * no bind-aware path at all -- {@link #findLiteralValue} alone means a real prepared-statement
     * client sending the shard key as {@code column = ?} (the overwhelming majority of real driver
     * traffic -- JDBC {@code PreparedStatement}, psycopg2 parameterized queries, any ORM) never gets
     * pruned to one shard; it silently scatter-gathers across every shard member on every single-row
     * lookup instead. Not a wrong-result bug -- {@code RouterStage}'s own scatter-gather path is
     * still correct -- but a real, needless fan-out cost on exactly the query shape sharding exists
     * to make cheap.
     *
     * <p>Finds {@code column = ?} anywhere in the SQL text (same equality-only, comment/string-
     * literal-blind scope as {@link #findLiteralValue}) and maps it to {@code binds}' matching
     * element by counting how many {@code ?} placeholders occur earlier in the statement -- the
     * same left-to-right positional convention every JDBC driver and this codebase's own bind-
     * ordering code (e.g. orawire's {@code BindVariableRewriter}) already uses, so no new per-rule
     * config (an explicit bind index, the way {@link RouterStage.ValueShardRule} needs one) is
     * required: the column name alone is enough, at any bind position, in any statement shape.
     *
     * @return the bind value's {@code toString()}, ready for {@link RouterStage.ShardingStrategy
     *      #resolve}, or {@code null} if the column doesn't appear as an equality-bind predicate at
     *      all, or the computed position falls outside {@code binds} (a statement shape this method
     *      can't safely reason about -- callers already treat a {@code null} return as "no route
     *      decision from this rule," so this only ever narrows what gets pruned, never misroutes).
     */
    static String findBindValue(String sql, String columnName, List<Object> binds) {
        if (binds == null || binds.isEmpty()) {
            return null;
        }
        Matcher m = Pattern.compile("(?i)\\b" + Pattern.quote(columnName) + "\\b\\s*=\\s*\\?").matcher(sql);
        if (!m.find()) {
            return null;
        }
        int placeholderIndex = 0;
        Matcher placeholders = PLACEHOLDER.matcher(sql);
        // The matched predicate's own "?" is the LAST character of m's match -- count every "?"
        // strictly before that position to get this placeholder's 0-based left-to-right ordinal.
        int thisPlaceholderPos = m.end() - 1;
        while (placeholders.find()) {
            if (placeholders.start() == thisPlaceholderPos) {
                break;
            }
            if (placeholders.start() < thisPlaceholderPos) {
                placeholderIndex++;
            }
        }
        if (placeholderIndex < 0 || placeholderIndex >= binds.size()) {
            return null;
        }
        Object value = binds.get(placeholderIndex);
        return value == null ? null : value.toString();
    }
}
