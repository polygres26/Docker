package com.sayonora.wire.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Correct cross-shard aggregation for {@link RoutingBackendExecutor}'s scatter-gather path.
 *
 * <p>Before this class, scatter-gather ran identical SQL against every shard and concatenated
 * the raw rows -- correct for a plain row-fetch, silently wrong for an aggregate: a
 * {@code SELECT COUNT(*) FROM orders} across 3 shards returned 3 separate count rows, not one
 * summed total; {@code SUM}/{@code MIN}/{@code MAX}/{@code AVG} had the same problem. This was
 * flagged as the highest-risk gap in a competitive comparison against Apache ShardingSphere
 * (which has a real federated query engine) -- risk, not just missing depth, because the old
 * behavior didn't error, it returned data a caller could easily misread as already-correct.
 *
 * <p>Scope, deliberately: single-statement {@code SELECT ... FROM ... [WHERE ...] [GROUP BY ...]
 * [LIMIT n]} with a select-list of plain columns/expressions and/or {@code COUNT}/{@code SUM}/
 * {@code AVG}/{@code MIN}/{@code MAX} calls. No subqueries in the select list, no window
 * functions, no {@code DISTINCT} inside an aggregate (a distributed count-distinct is a genuinely
 * different, harder problem -- {@link #plan} refuses rather than silently mis-merging), no
 * unrecognized aggregate/window function (refused for the same reason -- see {@link #plan}'s
 * javadoc). Non-aggregate select-list items are treated as the group key, whether or not a
 * {@code GROUP BY} clause is textually present (SQL requires them to match anyway when one is);
 * this also correctly handles the common "no GROUP BY, one or more plain aggregates" case as a
 * single implicit group. {@code ORDER BY} is not re-applied across the merged result -- a known,
 * disclosed remaining gap, not attempted here.
 */
final class ScatterGatherAggregateMerge {

    private ScatterGatherAggregateMerge() {
    }

    private static final Pattern FUNCTION_CALL = Pattern.compile("(?i)^([A-Za-z_][A-Za-z0-9_]*)\\s*\\(");
    private static final Pattern AS_ALIAS = Pattern.compile("(?i)\\s+AS\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*$");
    private static final Pattern LIMIT_CLAUSE = Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)\\s*;?\\s*$");
    private static final java.util.Set<String> MERGEABLE_AGGREGATES =
            java.util.Set.of("COUNT", "SUM", "AVG", "MIN", "MAX");

    enum AggKind { COUNT, SUM, AVG, MIN, MAX }

    /** One select-list item, resolved to either a group-key passthrough column or a mergeable
     * aggregate. {@code rewrittenExpr} is what actually gets sent to each shard (unchanged for a
     * passthrough column; for AVG, two appended helper columns replace it -- see {@link #plan}). */
    private record PlannedColumn(String outputName, boolean isAggregate, AggKind kind, String rawArg) {
    }

    record Plan(String rewrittenSql, List<PlannedColumn> columns, Integer limit) {
    }

    /**
     * Returns {@code null} if this statement doesn't need (or can't safely get) aggregate
     * merging -- callers fall back to the original plain-append scatter-gather in that case. A
     * {@code null} return covers both "no aggregate present" (append is already correct) and
     * "not a SELECT" (caller's existing guard handles that). Throws {@link SQLException} only for
     * the cases that would otherwise be silently wrong if merged the naive way: {@code DISTINCT}
     * inside an aggregate, or a function call this class doesn't know how to merge.
     */
    static Plan plan(String sql) throws SQLException {
        String trimmed = sql.strip();
        Integer fromIndex = findTopLevelFrom(trimmed);
        if (fromIndex == null) {
            return null;
        }
        String selectListText = trimmed.substring(6, fromIndex).strip(); // after "SELECT"
        if (selectListText.toUpperCase(Locale.ROOT).startsWith("DISTINCT ")) {
            return null; // SELECT DISTINCT ... isn't a shape this class attempts to merge
        }
        List<String> items = splitTopLevelCommas(selectListText);

        List<PlannedColumn> planned = new ArrayList<>();
        boolean anyAggregate = false;
        StringBuilder rewrittenSelectList = new StringBuilder();
        int helperIndex = 0;
        for (String rawItem : items) {
            String item = rawItem.strip();
            String alias = null;
            Matcher aliasMatch = AS_ALIAS.matcher(item);
            String withoutAlias = item;
            if (aliasMatch.find()) {
                alias = aliasMatch.group(1);
                withoutAlias = item.substring(0, aliasMatch.start()).strip();
            }
            Matcher fn = FUNCTION_CALL.matcher(withoutAlias);
            if (!fn.find()) {
                // Plain column/expression -- a group-key passthrough, unchanged in the rewrite.
                planned.add(new PlannedColumn(alias != null ? alias : withoutAlias, false, null, withoutAlias));
                appendWithComma(rewrittenSelectList, item);
                continue;
            }
            String funcName = fn.group(1).toUpperCase(Locale.ROOT);
            if (!MERGEABLE_AGGREGATES.contains(funcName)) {
                throw ErrorCatalog.sqlException("ERR_SCATTER_UNSUPPORTED_AGGREGATE", funcName);
            }
            int argStart = fn.end();
            int argEnd = matchingCloseParen(withoutAlias, argStart - 1);
            if (argEnd < 0) {
                return null; // malformed/unparseable -- let the caller's existing path handle it
            }
            String arg = withoutAlias.substring(argStart, argEnd).strip();
            if (arg.toUpperCase(Locale.ROOT).startsWith("DISTINCT ") || arg.equalsIgnoreCase("DISTINCT")) {
                throw ErrorCatalog.sqlException("ERR_SCATTER_DISTINCT_UNSUPPORTED", funcName);
            }
            anyAggregate = true;
            AggKind kind = AggKind.valueOf(funcName);
            String outputName = alias != null ? alias : (funcName.toLowerCase(Locale.ROOT) + "_" + (helperIndex));
            if (kind == AggKind.AVG) {
                // "agg_avg_sum_N"/"agg_avg_cnt_N", not "__avg_sum_N"/"__avg_cnt_N" -- a leading
                // underscore is a valid unquoted Postgres identifier but NOT valid unquoted Oracle
                // syntax (Oracle requires an unquoted identifier to start with a letter);
                // confirmed live as a real ORA-00911 the instant a scatter-gather aggregate query
                // touched a real Oracle shard. mergeShardResult reads these back by ordinal
                // position, never by name, so any valid-and-unique-per-helperIndex alias works.
                String sumCol = "agg_avg_sum_" + helperIndex;
                String cntCol = "agg_avg_cnt_" + helperIndex;
                appendWithComma(rewrittenSelectList, "SUM(" + arg + ") AS " + sumCol);
                appendWithComma(rewrittenSelectList, "COUNT(" + arg + ") AS " + cntCol);
                planned.add(new PlannedColumn(outputName, true, AggKind.AVG, arg));
            } else {
                String helperCol = "agg_" + helperIndex;
                appendWithComma(rewrittenSelectList, funcName + "(" + arg + ") AS " + helperCol);
                planned.add(new PlannedColumn(outputName, true, kind, arg));
            }
            helperIndex++;
        }

        if (!anyAggregate) {
            return null; // pure passthrough -- the existing append path is already correct
        }

        String tail = trimmed.substring(fromIndex);
        Integer limit = null;
        Matcher limitMatch = LIMIT_CLAUSE.matcher(tail);
        if (limitMatch.find()) {
            limit = Integer.parseInt(limitMatch.group(1));
            tail = tail.substring(0, limitMatch.start()).stripTrailing();
        }
        String rewritten = "SELECT " + rewrittenSelectList + " " + tail;
        return new Plan(rewritten, List.copyOf(planned), limit);
    }

    /** Merges one shard's rows (from executing {@link Plan#rewrittenSql}) into the running
     * per-group accumulator. Call once per shard, in any order -- merging is commutative for
     * every supported aggregate kind. */
    static void mergeShardResult(Plan plan, ExecutionResult shardResult,
            Map<List<Object>, Object[]> accumulatorsByGroupKey) {
        for (List<Object> row : shardResult.rows()) {
            List<Object> groupKey = new ArrayList<>();
            Object[] acc = new Object[plan.columns().size()];
            int rawIdx = 0;
            for (int i = 0; i < plan.columns().size(); i++) {
                PlannedColumn col = plan.columns().get(i);
                if (!col.isAggregate()) {
                    Object value = row.get(rawIdx++);
                    groupKey.add(value);
                    acc[i] = value;
                    continue;
                }
                if (col.kind() == AggKind.AVG) {
                    Object sum = row.get(rawIdx++);
                    Object count = row.get(rawIdx++);
                    acc[i] = new double[] { toDouble(sum), toDouble(count) };
                } else {
                    acc[i] = row.get(rawIdx++);
                }
            }
            Object[] existing = accumulatorsByGroupKey.get(groupKey);
            if (existing == null) {
                accumulatorsByGroupKey.put(groupKey, acc);
                continue;
            }
            for (int i = 0; i < plan.columns().size(); i++) {
                PlannedColumn col = plan.columns().get(i);
                if (!col.isAggregate()) {
                    continue; // group-key columns are identical by construction (same key)
                }
                existing[i] = combine(col.kind(), existing[i], acc[i]);
            }
        }
    }

    private static Object combine(AggKind kind, Object a, Object b) {
        return switch (kind) {
            case COUNT, SUM -> toDouble(a) + toDouble(b);
            case MIN -> compareNumericOrComparable(a, b) <= 0 ? a : b;
            case MAX -> compareNumericOrComparable(a, b) >= 0 ? a : b;
            case AVG -> {
                double[] x = (double[]) a;
                double[] y = (double[]) b;
                yield new double[] { x[0] + y[0], x[1] + y[1] };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static int compareNumericOrComparable(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        return ((Comparable<Object>) a).compareTo(b);
    }

    private static double toDouble(Object o) {
        return o == null ? 0.0 : ((Number) o).doubleValue();
    }

    /** Builds the final client-facing result: original column names/order, {@code AVG} computed
     * from its accumulated sum/count, {@code LIMIT} applied to the merged (not per-shard) set. */
    static ExecutionResult buildResult(Plan plan, Map<List<Object>, Object[]> accumulatorsByGroupKey) {
        List<ColumnInfo> columns = new ArrayList<>();
        for (PlannedColumn col : plan.columns()) {
            int jdbcType = switch (col.isAggregate() ? col.kind() : null) {
                case null -> java.sql.Types.VARCHAR; // passthrough -- exact type doesn't matter for wire display
                case COUNT -> java.sql.Types.BIGINT;
                case SUM, AVG -> java.sql.Types.DOUBLE;
                case MIN, MAX -> java.sql.Types.VARCHAR;
            };
            columns.add(new ColumnInfo(col.outputName(), jdbcType, 0, 0, 0, true));
        }
        List<List<Object>> rows = new ArrayList<>();
        for (Object[] acc : accumulatorsByGroupKey.values()) {
            List<Object> row = new ArrayList<>(acc.length);
            for (int i = 0; i < acc.length; i++) {
                PlannedColumn col = plan.columns().get(i);
                if (col.isAggregate() && col.kind() == AggKind.AVG) {
                    double[] sumAndCount = (double[]) acc[i];
                    row.add(sumAndCount[1] == 0 ? null : sumAndCount[0] / sumAndCount[1]);
                } else {
                    row.add(acc[i]);
                }
            }
            rows.add(row);
            if (plan.limit() != null && rows.size() >= plan.limit()) {
                break;
            }
        }
        return ExecutionResult.ofQuery(columns, rows);
    }

    private static void appendWithComma(StringBuilder sb, String item) {
        if (sb.length() > 0) {
            sb.append(", ");
        }
        sb.append(item);
    }

    /** Finds the top-level (paren/string-literal-aware) {@code FROM} keyword right after the
     * leading {@code SELECT}. Returns {@code null} if this doesn't even look like a plain
     * {@code SELECT ... FROM ...} (e.g. no FROM at all) -- caller treats that as "can't plan,
     * fall back". */
    private static Integer findTopLevelFrom(String sql) {
        if (sql.length() < 6 || !sql.regionMatches(true, 0, "SELECT", 0, 6)) {
            return null;
        }
        int depth = 0;
        boolean inString = false;
        for (int i = 6; i < sql.length() - 3; i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
            } else if (!inString && c == '(') {
                depth++;
            } else if (!inString && c == ')') {
                depth--;
            } else if (!inString && depth == 0 && isWordBoundaryFrom(sql, i)) {
                return i;
            }
        }
        return null;
    }

    private static boolean isWordBoundaryFrom(String sql, int i) {
        if (!sql.regionMatches(true, i, "FROM", 0, 4)) {
            return false;
        }
        boolean leftOk = i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1));
        int after = i + 4;
        boolean rightOk = after >= sql.length() || !Character.isLetterOrDigit(sql.charAt(after));
        return leftOk && rightOk;
    }

    private static int matchingCloseParen(String s, int openIdx) {
        int depth = 0;
        boolean inString = false;
        for (int i = openIdx; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < s.length() && s.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
            } else if (!inString && c == '(') {
                depth++;
            } else if (!inString && c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitTopLevelCommas(String text) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
            } else if (!inString && c == '(') {
                depth++;
            } else if (!inString && c == ')') {
                depth--;
            } else if (!inString && depth == 0 && c == ',') {
                parts.add(text.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(text.substring(start));
        return parts;
    }
}
