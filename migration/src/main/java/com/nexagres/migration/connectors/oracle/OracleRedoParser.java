package com.nexagres.migration.connectors.oracle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the {@code SQL_REDO} text LogMiner itself generates for one {@code V$LOGMNR_CONTENTS} row
 * into a column-name -> literal-text map -- LogMiner has no structured column/value interface the
 * way DynamoDB Streams or a MySQL binlog row event does; the redo is always a synthesized SQL
 * statement, and this is the real, standard technique for consuming it (the same approach
 * pre-XStream/OCI Oracle CDC tooling has always used).
 *
 * <p>Requires table-level {@code ALTER TABLE ... ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS} (see
 * {@code OracleSource#ensureSupplementalLogging}) -- confirmed live, not assumed: with only
 * database-level MINIMAL supplemental logging, an UPDATE's generated {@code WHERE} clause contains
 * only the CHANGED column's old value plus {@code ROWID}, not the primary key, making it impossible
 * to know which row to update without a separate {@code ROWID} lookup. With {@code (ALL) COLUMNS}
 * enabled on the table, every UPDATE/DELETE's {@code WHERE} clause includes every column's value
 * (or old value), including the primary key, which is what this parser actually relies on.
 *
 * <p><b>Deterministic date/timestamp parsing, a real fix not a guess</b>: confirmed live that
 * {@code ALTER SESSION SET NLS_DATE_FORMAT = '...'} (set once per LogMiner session, before {@code
 * DBMS_LOGMNR.START_LOGMNR}) controls the format Oracle renders {@code TO_DATE(...)} literals with
 * inside the generated {@code SQL_REDO} text, REGARDLESS of whatever NLS settings were active on
 * the session that originally made the change. {@link OracleSource} sets this to a single
 * ISO-8601-like format before every polling session specifically so this parser never has to guess
 * or handle multiple date formats.
 *
 * <p><b>Known, scoped limitations</b> (documented, not silently guessed): a quoted string value
 * containing the literal text {@code " where "} could, in principle, confuse the top-level SET/
 * WHERE clause splitter (a genuinely rare edge case in the SQL Oracle itself generates, not
 * arbitrary user SQL); {@code BLOB}/{@code CLOB} values beyond a certain size are represented by
 * LogMiner across multiple chained redo rows, which this parser does not reassemble -- large LOB
 * CDC is a real, separately scoped follow-up.
 */
final class OracleRedoParser {

    record ParsedChange(Map<String, String> values) {
    }

    private OracleRedoParser() {
    }

    static ParsedChange parseInsert(String sqlRedo) {
        int valuesIdx = indexOfTopLevel(sqlRedo, " values (", 0);
        int colsOpen = sqlRedo.indexOf('(');
        String columnList = sqlRedo.substring(colsOpen + 1, findMatchingClose(sqlRedo, colsOpen));
        String valuesList = sqlRedo.substring(valuesIdx + " values (".length(), findMatchingClose(sqlRedo, valuesIdx + " values (".length() - 1));

        List<String> columns = splitTopLevel(columnList, ',').stream().map(OracleRedoParser::unquoteIdentifier).toList();
        List<String> values = splitTopLevel(valuesList, ',');

        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < columns.size() && i < values.size(); i++) {
            result.put(columns.get(i), parseLiteral(values.get(i).trim()));
        }
        return new ParsedChange(result);
    }

    static ParsedChange parseUpdateOrDelete(String sqlRedo, boolean isUpdate) {
        int whereIdx = indexOfTopLevelWhereKeyword(sqlRedo);
        Map<String, String> result = new LinkedHashMap<>();

        if (isUpdate) {
            int setIdx = indexOfTopLevel(sqlRedo, " set ", 0);
            String setClause = sqlRedo.substring(setIdx + " set ".length(), whereIdx);
            for (String assignment : splitTopLevel(setClause, ',')) {
                int eq = indexOfTopLevel(assignment, " = ", 0);
                if (eq < 0) {
                    continue;
                }
                String col = unquoteIdentifier(assignment.substring(0, eq).trim());
                String value = parseLiteral(assignment.substring(eq + " = ".length()).trim());
                result.put(col, value);
            }
        }

        String whereClause = sqlRedo.substring(whereIdx + " where ".length());
        if (whereClause.endsWith(";")) {
            whereClause = whereClause.substring(0, whereClause.length() - 1);
        }
        for (String condition : splitTopLevel(whereClause, " and ")) {
            String trimmed = condition.trim();
            if (trimmed.startsWith("ROWID")) {
                continue; // not a real column -- LogMiner's own row-identity anchor, never migrated
            }
            int isNullIdx = indexOfTopLevel(trimmed, " IS NULL", 0);
            if (isNullIdx >= 0) {
                String col = unquoteIdentifier(trimmed.substring(0, isNullIdx).trim());
                result.putIfAbsent(col, null);
                continue;
            }
            int eq = indexOfTopLevel(trimmed, " = ", 0);
            if (eq < 0) {
                continue;
            }
            String col = unquoteIdentifier(trimmed.substring(0, eq).trim());
            String value = parseLiteral(trimmed.substring(eq + " = ".length()).trim());
            result.putIfAbsent(col, value); // WHERE reflects the OLD value -- SET (already applied
            // above for UPDATE) always wins for a column present in both, since putIfAbsent no-ops
            // when SET already put a value for that column.
        }
        return new ParsedChange(result);
    }

    /** {@code null} for a literal {@code NULL} keyword; otherwise the literal's real text value --
     * unescaping a quoted string's doubled {@code ''}, or unwrapping {@code TO_DATE('text', ...)}/
     * {@code TO_TIMESTAMP('text', ...)} to just the inner ISO-formatted text (see this class's own
     * javadoc on why that inner text is always in one known, fixed format). */
    private static String parseLiteral(String token) {
        if ("NULL".equals(token)) {
            return null;
        }
        if (token.startsWith("TO_DATE(") || token.startsWith("TO_TIMESTAMP(")) {
            int firstQuote = token.indexOf('\'');
            int secondQuote = token.indexOf('\'', firstQuote + 1);
            return token.substring(firstQuote + 1, secondQuote);
        }
        if (token.startsWith("'") && token.endsWith("'")) {
            return token.substring(1, token.length() - 1).replace("''", "'");
        }
        return token; // an unquoted, non-NULL, non-TO_DATE token -- not seen in practice, kept as
        // a safe fallback rather than throwing.
    }

    private static String unquoteIdentifier(String ident) {
        String trimmed = ident.trim();
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    /** Splits on {@code delimiter} (a single char or the word {@code "and"}) at paren-depth 0,
     * OUTSIDE single-quoted strings (respecting {@code ''} as an escaped quote, not a string
     * terminator) -- required because a value like {@code TO_DATE('30-AUG-26', 'DD-MON-RR')}
     * contains its own commas and parens that must NOT be treated as top-level separators. */
    private static List<String> splitTopLevel(String text, char delimiter) {
        return splitTopLevel(text, String.valueOf(delimiter));
    }

    private static List<String> splitTopLevel(String text, String delimiter) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inQuotes = false;
        int start = 0;
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\'') {
                if (inQuotes && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                    i += 2; // escaped '' -- skip both, still inside the string
                    continue;
                }
                inQuotes = !inQuotes;
                i++;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                } else if (depth == 0 && matchesDelimiterAt(text, i, delimiter)) {
                    parts.add(text.substring(start, i));
                    i += delimiter.length();
                    start = i;
                    continue;
                }
            }
            i++;
        }
        parts.add(text.substring(start));
        return parts;
    }

    /** {@code delimiter} is matched literally, spaces included where the caller wants them (e.g.
     * {@code " and "} for a WHERE clause) -- this is what actually gives correct word-boundary
     * behavior for a multi-char delimiter, no separate boundary logic needed. */
    private static boolean matchesDelimiterAt(String text, int i, String delimiter) {
        return text.regionMatches(i, delimiter, 0, delimiter.length());
    }

    private static int indexOfTopLevel(String text, String needle, int from) {
        boolean inQuotes = false;
        for (int i = from; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'') {
                if (inQuotes && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes && text.regionMatches(i, needle, 0, needle.length())) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOfTopLevelWhereKeyword(String text) {
        return indexOfTopLevel(text, " where ", 0);
    }

    private static int findMatchingClose(String text, int openParenIndex) {
        int depth = 0;
        boolean inQuotes = false;
        for (int i = openParenIndex; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'') {
                if (inQuotes && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inQuotes = !inQuotes;
                continue;
            }
            if (inQuotes) {
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("unbalanced parentheses in SQL_REDO: " + text);
    }
}
