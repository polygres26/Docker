package com.polygres.wire.dynamowire;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses {@code Query}'s {@code KeyConditionExpression} — a much narrower grammar than
 * {@code FilterExpression}: exactly one partition-key equality, optionally ANDed with one sort-key
 * condition ({@code =, <, <=, >, >=, BETWEEN x AND y, begins_with(sk, v)}). Real DynamoDB requires
 * the partition key clause to use {@code =}; this parser enforces the same.
 */
public final class KeyConditionParser {

    public enum SortOp { EQ, LT, LE, GT, GE, BETWEEN, BEGINS_WITH, NONE }

    public final String partitionValueToken;
    public SortOp sortOp = SortOp.NONE;
    public String sortValueToken;      // for EQ/LT/LE/GT/GE/BEGINS_WITH
    public String sortValueToken2;     // for BETWEEN's high bound

    private KeyConditionParser(String partitionValueToken) {
        this.partitionValueToken = partitionValueToken;
    }

    private static final Pattern EQ = Pattern.compile("^(\\S+)\\s*=\\s*(\\S+)$");

    public static KeyConditionParser parse(String expr, TableSchema schema, ExpressionContext ctx) {
        String pkName = ctx.names.getOrDefault("#" + schema.partitionKeyName(), schema.partitionKeyName());
        String[] clauses = splitOnAnd(expr.trim());
        String pkValueToken = null;
        KeyConditionParser result = null;
        String sortAttr = schema.sortKeyName();

        for (String clause : clauses) {
            clause = clause.trim();
            String resolvedAttr = resolveLeadingAttr(clause, ctx);
            if (resolvedAttr.equals(schema.partitionKeyName())) {
                Matcher m = EQ.matcher(clause);
                if (!m.matches()) throw new DynamoException("ValidationException", "Partition key condition must use =");
                pkValueToken = m.group(2);
            } else if (sortAttr != null && resolvedAttr.equals(sortAttr)) {
                result = parseSortClause(clause, sortAttr, ctx);
            } else {
                throw new DynamoException("ValidationException", "KeyConditionExpression references non-key attribute: " + resolvedAttr);
            }
        }
        if (pkValueToken == null) throw new DynamoException("ValidationException", "KeyConditionExpression must include the partition key");
        KeyConditionParser out = new KeyConditionParser(pkValueToken);
        if (result != null) {
            out.sortOp = result.sortOp;
            out.sortValueToken = result.sortValueToken;
            out.sortValueToken2 = result.sortValueToken2;
        }
        return out;
    }

    private static String resolveLeadingAttr(String clause, ExpressionContext ctx) {
        int i = 0;
        while (i < clause.length() && (Character.isLetterOrDigit(clause.charAt(i)) || clause.charAt(i) == '_' || clause.charAt(i) == '#')) i++;
        return ctx.resolveName(clause.substring(0, i));
    }

    private static KeyConditionParser parseSortClause(String clause, String sortAttr, ExpressionContext ctx) {
        KeyConditionParser p = new KeyConditionParser(null);
        Matcher begins = Pattern.compile("^begins_with\\(\\s*\\S+\\s*,\\s*(\\S+)\\s*\\)$").matcher(clause);
        if (begins.matches()) { p.sortOp = SortOp.BEGINS_WITH; p.sortValueToken = begins.group(1); return p; }
        Matcher between = Pattern.compile("^\\S+\\s+BETWEEN\\s+(\\S+)\\s+AND\\s+(\\S+)$").matcher(clause);
        if (between.matches()) { p.sortOp = SortOp.BETWEEN; p.sortValueToken = between.group(1); p.sortValueToken2 = between.group(2); return p; }
        Matcher ge = Pattern.compile("^\\S+\\s*(=|<=|>=|<|>)\\s*(\\S+)$").matcher(clause);
        if (ge.matches()) {
            p.sortOp = switch (ge.group(1)) {
                case "=" -> SortOp.EQ;
                case "<" -> SortOp.LT;
                case "<=" -> SortOp.LE;
                case ">" -> SortOp.GT;
                case ">=" -> SortOp.GE;
                default -> throw new DynamoException("ValidationException", "Unsupported sort key operator");
            };
            p.sortValueToken = ge.group(2);
            return p;
        }
        throw new DynamoException("ValidationException", "Unrecognized sort key condition: " + clause);
    }

    /**
     * KeyConditionExpression has at most two clauses (partition key, then optionally sort key)
     * joined by exactly one top-level AND — the partition-key clause is always a bare equality
     * and never itself contains AND, so splitting on the *first* case-insensitive " AND " token
     * correctly separates the two clauses even when the sort clause is itself a BETWEEN (which
     * contains a second, nested AND that must NOT be split on).
     */
    private static String[] splitOnAnd(String expr) {
        Matcher m = Pattern.compile("(?i)\\s+AND\\s+").matcher(expr);
        if (m.find()) {
            return new String[] { expr.substring(0, m.start()), expr.substring(m.end()) };
        }
        return new String[] { expr };
    }
}
