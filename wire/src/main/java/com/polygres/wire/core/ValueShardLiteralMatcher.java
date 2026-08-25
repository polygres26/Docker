package com.polygres.wire.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts a sharding column's literal value from raw SQL text, for {@link RouterStage.ValueShardColumnRule}.
 * Regex-on-raw-SQL, not a real parser -- deliberately matching the same scope {@link RouterStage}'s
 * existing {@code SchemaRule}/{@code ShardRule} already operate at (see {@code RouterStage.fromConfig}),
 * not a new, higher bar. Recognizes {@code column = 'text'} and {@code column = 123} (equality only,
 * not {@code IN (...)}, ranges, or any other operator -- those need the value-shard column's value to
 * be genuinely singular to route correctly, and equality is by far the common case for a sharding
 * predicate). Does not distinguish a real WHERE-clause comparison from the same shape appearing
 * inside a string literal or comment, exactly like the pre-existing schema/shard-table regexes.
 */
final class ValueShardLiteralMatcher {

    private ValueShardLiteralMatcher() {
    }

    static String findLiteralValue(String sql, String columnName) {
        Pattern pattern = Pattern.compile(
                "(?i)\\b" + Pattern.quote(columnName) + "\\b\\s*=\\s*(?:'((?:[^']|'')*)'|(-?\\d+(?:\\.\\d+)?))");
        Matcher matcher = pattern.matcher(sql);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(1) != null ? matcher.group(1).replace("''", "'") : matcher.group(2);
    }
}
