package com.polygres.wire.core.access;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Best-effort extraction of table names referenced by a SQL statement's {@code FROM}/{@code JOIN}
 * clauses — regex-based, at the same documented fidelity {@code RouterStage}'s own schema-name
 * matching already accepts ("true column-aware routing needs a real SQL parser — future work").
 * {@link AccessControlStage} uses this to decide which {@code column_grants}/{@code row_filters}
 * rules are even in play for a given statement, matching each rule's {@code table_pattern} regex
 * against every extracted name rather than against the raw SQL text directly — narrower and less
 * prone to a rule accidentally matching an unrelated substring (a column named the same as a
 * restricted table, a string literal, etc.) than a bare full-text search would be.
 */
public final class SqlTableReferences {

    private SqlTableReferences() {
    }

    // "from"/"join" followed by a dotted identifier — deliberately not "\\w+\\." (schema-qualified
    // only) since most queries reference bare table names; captures the qualified-or-bare name
    // whole so a rule's table_pattern can match either "orders" or "public.orders".
    private static final Pattern FROM_OR_JOIN = Pattern.compile(
            "\\b(?:from|join)\\s+([a-zA-Z_][\\w$]*(?:\\.[a-zA-Z_][\\w$]*)*)", Pattern.CASE_INSENSITIVE);

    /** Returns every distinct table reference found, in the case it appeared in the SQL (matching against a rule's pattern is separately case-insensitive — see {@code AccessPolicyYamlConfig}). */
    public static Set<String> extract(String sqlText) {
        Set<String> tables = new LinkedHashSet<>();
        if (sqlText == null) {
            return tables;
        }
        Matcher matcher = FROM_OR_JOIN.matcher(sqlText);
        while (matcher.find()) {
            tables.add(matcher.group(1));
        }
        return tables;
    }

    /** True if any extracted table reference matches {@code tablePattern}. */
    public static boolean anyMatches(String sqlText, Pattern tablePattern) {
        for (String table : extract(sqlText)) {
            if (tablePattern.matcher(table).find()) {
                return true;
            }
        }
        return false;
    }
}
