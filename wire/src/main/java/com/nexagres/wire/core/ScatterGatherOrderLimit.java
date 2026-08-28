package com.nexagres.wire.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Correct cross-shard {@code ORDER BY}/{@code LIMIT}/{@code OFFSET} for
 * {@link RoutingBackendExecutor}'s scatter-gather path -- both the plain row-fetch path and (via
 * {@link ScatterGatherAggregateMerge}'s already-merged output) the aggregate path.
 *
 * <p>Before this class, scatter-gather sent the client's full SQL -- including its own
 * {@code ORDER BY}/{@code LIMIT} -- unmodified to every shard and just concatenated the results.
 * Each shard applied the client's {@code LIMIT} to its own local rows, so a 3-shard
 * {@code SELECT * FROM t ORDER BY x LIMIT 10} could return up to 30 rows, in shard-arrival order,
 * not the correct globally-ordered top 10. Same failure class as the aggregate-merge gap this
 * fixed earlier in the same competitive comparison against ShardingSphere: not an error, silently
 * wrong data.
 *
 * <p>Fix shape: strip {@code ORDER BY}/{@code LIMIT}/{@code OFFSET} from what's sent to each
 * shard (so every shard returns its full matching row set, unsorted/untruncated), gather
 * everything, then sort and truncate once, centrally, here. This trades some scan cost for
 * correctness -- no per-shard partial-limit pushdown is attempted, deliberately; that optimization
 * requires reasoning about whether a shard's local top-N is guaranteed to contain the true global
 * top-N; without it, "fetch everything, merge correctly" is the same conservative choice this
 * project already made for the aggregate-merge case.
 *
 * <p>Scope: {@code ORDER BY} keys must resolve to an actual result column, by name (matching an
 * output column, case-insensitively -- including an aggregate's alias) or by 1-based ordinal
 * position -- not an arbitrary expression, which this class has no way to evaluate centrally.
 * {@link #applyOrderAndLimit} throws {@link SQLException} rather than silently ignoring an
 * unresolvable sort key, same "refuse over silently wrong" convention as
 * {@link ScatterGatherAggregateMerge}.
 */
final class ScatterGatherOrderLimit {

    private ScatterGatherOrderLimit() {
    }

    record SortKey(String columnRef, boolean descending, boolean nullsFirst) {
    }

    record Spec(List<SortKey> orderBy, Integer limit, Integer offset) {
        static final Spec NONE = new Spec(List.of(), null, null);

        boolean isTrivial() {
            return orderBy.isEmpty() && limit == null && offset == null;
        }
    }

    record Parsed(String withoutOrderLimitOffset, Spec spec) {
    }

    private static final Pattern LIMIT_THEN_OFFSET =
            Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)(?:\\s+OFFSET\\s+(\\d+))?\\s*;?\\s*$");
    private static final Pattern OFFSET_THEN_LIMIT =
            Pattern.compile("(?i)\\bOFFSET\\s+(\\d+)\\s+LIMIT\\s+(\\d+)\\s*;?\\s*$");
    private static final Pattern NULLS_SUFFIX = Pattern.compile("(?i)\\s+NULLS\\s+(FIRST|LAST)\\s*$");
    private static final Pattern DIRECTION_SUFFIX = Pattern.compile("(?i)\\s+(ASC|DESC)\\s*$");

    /** Strips a trailing {@code LIMIT}/{@code OFFSET} (either order) and top-level {@code ORDER
     * BY} clause from {@code sql}, returning both the remaining SQL and what was parsed out. Safe
     * to call on SQL with none of these clauses -- returns the original SQL and an empty/null
     * {@link Spec}. */
    static Parsed parse(String sql) {
        String s = sql.strip();
        if (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).stripTrailing();
        }

        Integer limit = null;
        Integer offset = null;
        // OFFSET-then-LIMIT must be tried first: for "... OFFSET 5 LIMIT 10", the more permissive
        // LIMIT_THEN_OFFSET pattern still matches just the trailing "LIMIT 10" on its own (its
        // OFFSET group is optional), which would strip only that and leave the OFFSET behind.
        Matcher ol = OFFSET_THEN_LIMIT.matcher(s);
        if (ol.find()) {
            offset = Integer.parseInt(ol.group(1));
            limit = Integer.parseInt(ol.group(2));
            s = s.substring(0, ol.start()).stripTrailing();
        } else {
            Matcher lo = LIMIT_THEN_OFFSET.matcher(s);
            if (lo.find()) {
                limit = Integer.parseInt(lo.group(1));
                if (lo.group(2) != null) {
                    offset = Integer.parseInt(lo.group(2));
                }
                s = s.substring(0, lo.start()).stripTrailing();
            }
        }

        List<SortKey> orderBy = List.of();
        int orderByStart = findTopLevelOrderBy(s);
        if (orderByStart >= 0) {
            String clause = s.substring(orderByStart + "ORDER BY".length());
            orderBy = parseOrderByItems(clause);
            s = s.substring(0, orderByStart).stripTrailing();
        }

        if (orderBy.isEmpty() && limit == null && offset == null) {
            return new Parsed(sql, Spec.NONE);
        }
        return new Parsed(s, new Spec(orderBy, limit, offset));
    }

    /** Sorts and truncates a fully-gathered (all shards, unsorted/untruncated) result according to
     * {@code spec}. A trivial spec (no ORDER BY/LIMIT/OFFSET at all) returns {@code merged}
     * unchanged. */
    static ExecutionResult applyOrderAndLimit(ExecutionResult merged, Spec spec) throws SQLException {
        if (spec.isTrivial()) {
            return merged;
        }
        List<ColumnInfo> columns = merged.columns();
        List<List<Object>> rows = new ArrayList<>(merged.rows());

        if (!spec.orderBy().isEmpty()) {
            List<Integer> resolvedIndexes = new ArrayList<>();
            for (SortKey key : spec.orderBy()) {
                resolvedIndexes.add(resolveColumn(columns, key.columnRef()));
            }
            rows.sort((a, b) -> {
                for (int i = 0; i < spec.orderBy().size(); i++) {
                    SortKey key = spec.orderBy().get(i);
                    int idx = resolvedIndexes.get(i);
                    int cmp = compareForSort(a.get(idx), b.get(idx), key.nullsFirst());
                    if (cmp != 0) {
                        return key.descending() ? -cmp : cmp;
                    }
                }
                return 0;
            });
        }

        int offset = spec.offset() == null ? 0 : spec.offset();
        int fromIndex = Math.min(offset, rows.size());
        int toIndex = spec.limit() == null ? rows.size() : Math.min(rows.size(), fromIndex + spec.limit());
        List<List<Object>> page = fromIndex >= toIndex ? List.of() : rows.subList(fromIndex, toIndex);
        return ExecutionResult.ofQuery(columns, page);
    }

    private static int resolveColumn(List<ColumnInfo> columns, String ref) throws SQLException {
        String trimmed = ref.strip();
        if (trimmed.matches("\\d+")) {
            int ordinal = Integer.parseInt(trimmed);
            if (ordinal >= 1 && ordinal <= columns.size()) {
                return ordinal - 1;
            }
            throw ErrorCatalog.sqlException("ERR_SCATTER_ORDERBY_POSITION_RANGE", ordinal, columns.size());
        }
        String unquoted = trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2
                ? trimmed.substring(1, trimmed.length() - 1)
                : trimmed;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(unquoted)) {
                return i;
            }
        }
        throw ErrorCatalog.sqlException("ERR_SCATTER_ORDERBY_NO_MATCH", ref);
    }

    @SuppressWarnings("unchecked")
    private static int compareForSort(Object a, Object b, boolean nullsFirst) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return nullsFirst ? -1 : 1;
        }
        if (b == null) {
            return nullsFirst ? 1 : -1;
        }
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        if (a instanceof Comparable && a.getClass().isInstance(b)) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    private static List<SortKey> parseOrderByItems(String clause) {
        List<SortKey> keys = new ArrayList<>();
        for (String rawItem : splitTopLevelCommas(clause)) {
            String item = rawItem.strip();
            if (item.isEmpty()) {
                continue;
            }
            boolean nullsFirst = false; // Postgres default: NULLS LAST for ASC, NULLS FIRST for DESC
            boolean nullsExplicit = false;
            Matcher nullsMatch = NULLS_SUFFIX.matcher(item);
            if (nullsMatch.find()) {
                nullsFirst = nullsMatch.group(1).equalsIgnoreCase("FIRST");
                nullsExplicit = true;
                item = item.substring(0, nullsMatch.start()).stripTrailing();
            }
            boolean descending = false;
            Matcher dirMatch = DIRECTION_SUFFIX.matcher(item);
            if (dirMatch.find()) {
                descending = dirMatch.group(1).equalsIgnoreCase("DESC");
                item = item.substring(0, dirMatch.start()).stripTrailing();
            }
            if (!nullsExplicit) {
                nullsFirst = descending;
            }
            keys.add(new SortKey(item, descending, nullsFirst));
        }
        return keys;
    }

    /** Top-level (paren/string-literal-aware) search for the {@code ORDER BY} keyword pair --
     * mirrors {@code ScatterGatherAggregateMerge}'s {@code findTopLevelFrom} scanning discipline,
     * so an {@code ORDER BY} inside a subquery or string literal is never mistaken for the
     * statement's own trailing clause. Returns -1 if not found. */
    private static int findTopLevelOrderBy(String sql) {
        int depth = 0;
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
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
            } else if (!inString && depth == 0 && matchesOrderByHere(sql, i)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean matchesOrderByHere(String sql, int i) {
        if (!sql.regionMatches(true, i, "ORDER", 0, 5)) {
            return false;
        }
        boolean leftOk = i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1));
        if (!leftOk) {
            return false;
        }
        int j = i + 5;
        while (j < sql.length() && Character.isWhitespace(sql.charAt(j))) {
            j++;
        }
        if (!sql.regionMatches(true, j, "BY", 0, 2)) {
            return false;
        }
        int after = j + 2;
        return after >= sql.length() || !Character.isLetterOrDigit(sql.charAt(after));
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
