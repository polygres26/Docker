package com.sayonora.wire.dynamowire;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A deliberately narrow, hand-rolled parser for the PartiQL subset dynamowire's ExecuteStatement/
 * BatchExecuteStatement operations actually support: single-table SELECT/INSERT/UPDATE/DELETE,
 * {@code ?} positional parameters, and a WHERE clause restricted to the same shape
 * {@link KeyConditionParser} already requires for SELECT (partition-key equality, optional
 * sort-key condition) or an exact partition[+sort]-key equality for UPDATE/DELETE (those always
 * target exactly one item, the same restriction real DynamoDB PartiQL UPDATE/DELETE impose). Not
 * the full PartiQL grammar -- no JOIN (DynamoDB itself has none), no nested-document paths in
 * UPDATE SET beyond a bare top-level attribute name, no non-key WHERE filtering in SELECT.
 *
 * <p>Every {@code ?} in the statement is rewritten to a synthetic {@code :pN} token (in
 * left-to-right order of appearance, the same order the request's own {@code Parameters} array is
 * positional in) before any structural parsing happens. That's what lets everything downstream --
 * this class's own WHERE-clause handling, and {@link OperationHandlers}' reuse of
 * {@link KeyConditionParser} and {@link UpdateExpressionParser} for the rest -- bind those tokens
 * into a real {@link ExpressionContext} and execute through the exact same code paths
 * GetItem/Query/UpdateItem already use, not a second implementation of key-condition or
 * update-expression semantics.
 */
final class PartiQlParser {

    sealed interface Statement permits Select, Insert, Update, Delete {}

    record Select(String table, String whereExpr) implements Statement {}
    record Insert(String table, String valueToken) implements Statement {}
    record Update(String table, String setClause, Map<String, String> keyTokens) implements Statement {}
    record Delete(String table, Map<String, String> keyTokens) implements Statement {}

    private static final String TABLE = "\"?([A-Za-z_][\\w.-]*)\"?";

    private static final Pattern SELECT_STMT = Pattern.compile(
            "(?is)^\\s*select\\s+.+?\\s+from\\s+" + TABLE + "\\s+where\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern INSERT_STMT = Pattern.compile(
            "(?is)^\\s*insert\\s+into\\s+" + TABLE + "\\s+value\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern UPDATE_STMT = Pattern.compile(
            "(?is)^\\s*update\\s+" + TABLE + "\\s+set\\s+(.+?)\\s+where\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern DELETE_STMT = Pattern.compile(
            "(?is)^\\s*delete\\s+from\\s+" + TABLE + "\\s+where\\s+(.+?)\\s*;?\\s*$");

    private PartiQlParser() {}

    /** Rewrites every {@code ?} outside a single-quoted string literal into {@code :p0},
     * {@code :p1}, ... in left-to-right order. Call this BEFORE {@link #parse}, once, on the raw
     * statement text -- everything else in this class and in OperationHandlers works on the
     * substituted form. */
    static String substitutePositionalParams(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        boolean inStr = false;
        int n = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inStr = !inStr;
                out.append(c);
            } else if (c == '?' && !inStr) {
                out.append(":p").append(n++);
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    static Statement parse(String sql) {
        Matcher m = SELECT_STMT.matcher(sql);
        if (m.matches()) {
            return new Select(m.group(1), stripIdentifierQuotes(m.group(2)));
        }
        m = INSERT_STMT.matcher(sql);
        if (m.matches()) {
            return new Insert(m.group(1), m.group(2).trim());
        }
        m = UPDATE_STMT.matcher(sql);
        if (m.matches()) {
            return new Update(m.group(1), stripIdentifierQuotes(m.group(2).trim()), parseExactKeyWhere(m.group(3)));
        }
        m = DELETE_STMT.matcher(sql);
        if (m.matches()) {
            return new Delete(m.group(1), parseExactKeyWhere(m.group(2)));
        }
        throw new DynamoException("ValidationException",
                "dynamowire's PartiQL support covers single-table SELECT/INSERT/UPDATE/DELETE with "
                        + "a plain key-equality WHERE clause -- could not parse: " + sql);
    }

    private static Map<String, String> parseExactKeyWhere(String whereExpr) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String clause : whereExpr.split("(?i)\\s+AND\\s+")) {
            Matcher m = Pattern.compile("^\\s*\"?([\\w.-]+)\"?\\s*=\\s*(\\S+)\\s*$").matcher(clause.trim());
            if (!m.matches()) {
                throw new DynamoException("ValidationException",
                        "UPDATE/DELETE's WHERE clause must be an exact key equality (e.g. "
                                + "id = ? [AND sk = ?]), not: " + clause);
            }
            out.put(m.group(1), m.group(2));
        }
        return out;
    }

    private static String stripIdentifierQuotes(String expr) {
        return expr.replaceAll("\"([A-Za-z_]\\w*)\"", "$1");
    }
}
