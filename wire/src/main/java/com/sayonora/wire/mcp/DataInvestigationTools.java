package com.sayonora.wire.mcp;

import com.sayonora.wire.server.ServerOptions.McpBackendMode;
import java.util.regex.Pattern;

/**
 * Real, per-dialect SQL for the data-investigation MCP tool set (column_stats, compare_groups,
 * correlation, sample_rows, find_outliers, inspect_schema, and the foreign-key edge query behind
 * find_join_path) -- built for training/evaluating a small model against a real database the way
 * https://www.linkedin.com/pulse/how-train-small-model-databases-kumar-rajamani-n1i5c/ describes:
 * an agent loop over a fixed toolset of structured, JSON-shaped database operations, not raw SQL
 * generation as the only interface. Kept deliberately dialect-specific rather than one
 * lowest-common-denominator query reused everywhere, the same discipline {@code WarpMcpServer}'s
 * own {@code list_tables}/{@code describe_table} native-backend-mode dispatch already established
 * -- ANSI SQL alone can't express these: Oracle has no {@code information_schema} at all, MySQL/
 * SQL Server have no built-in {@code CORR()} aggregate (Postgres and Oracle do), and SQL Server's
 * own {@code STDEV}/{@code STDEVP} spelling isn't {@code STDDEV}/{@code STDDEV_POP}.
 */
final class DataInvestigationTools {

    /** Table/column/group-by identifiers reach these methods as free-form MCP tool arguments and
     * get interpolated directly into SQL text (bind parameters can't stand in for identifiers in
     * standard SQL) -- this is the one guard against a caller accidentally (or adversarially)
     * closing a string and injecting arbitrary SQL through what's supposed to be a bare table or
     * column name. Deliberately narrow (letters/digits/underscore, one optional
     * schema-qualifying dot) -- wide enough for every real identifier these tools are meant to
     * take, narrow enough to reject anything that isn't one. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    static void requireValidIdentifier(String name, String argName) {
        if (name == null || !IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException(argName + " must be a plain identifier "
                    + "(letters, digits, underscore, optionally schema-qualified with one dot) -- got: " + name);
        }
    }

    private static final java.util.Set<String> VALID_AGGS = java.util.Set.of("avg", "sum", "count", "min", "max");

    static String requireValidAgg(String agg) {
        String normalized = agg == null ? "avg" : agg.toLowerCase(java.util.Locale.ROOT);
        if (!VALID_AGGS.contains(normalized)) {
            throw new IllegalArgumentException("agg must be one of " + VALID_AGGS + " -- got: " + agg);
        }
        return normalized;
    }

    /** Population standard deviation -- real function name differs (SQL Server alone breaks the
     * {@code STDDEV}/{@code STDDEV_POP} naming the other three dialects share). */
    private static String stddevFn(McpBackendMode mode) {
        return mode == McpBackendMode.SQLSERVER ? "STDEVP" : "STDDEV_POP";
    }

    /** A trailing row cap, spelled three different ways: Postgres/MySQL {@code LIMIT n} (appended
     * at the end), Oracle's modern {@code FETCH FIRST n ROWS ONLY} (also appended at the end, real
     * ANSI:2008 syntax Oracle 12c+ supports directly), and SQL Server's {@code TOP n} (a PREFIX on
     * the select list, not a suffix -- the one shape genuinely different in kind, not just
     * spelling, which is why every caller below builds the prefix and suffix separately rather
     * than treating this as one drop-in string). */
    private static String limitSuffix(McpBackendMode mode, int n) {
        return switch (mode) {
            case POSTGRES, MYSQL -> " LIMIT " + n;
            case ORACLE -> " FETCH FIRST " + n + " ROWS ONLY";
            case SQLSERVER -> ""; // TOP is a prefix -- see topPrefix.
        };
    }

    private static String topPrefix(McpBackendMode mode, int n) {
        return mode == McpBackendMode.SQLSERVER ? "TOP " + n + " " : "";
    }

    static String columnStatsSql(McpBackendMode mode, String table, String column) {
        requireValidIdentifier(table, "table");
        requireValidIdentifier(column, "column");
        return "SELECT COUNT(*) AS n, COUNT(" + column + ") AS non_null, "
                + "COUNT(*) - COUNT(" + column + ") AS nulls, "
                + "AVG(" + column + ") AS mean, " + stddevFn(mode) + "(" + column + ") AS stddev, "
                + "MIN(" + column + ") AS min, MAX(" + column + ") AS max, "
                + "COUNT(DISTINCT " + column + ") AS distinct_count "
                + "FROM " + table;
    }

    static String compareGroupsSql(McpBackendMode mode, String table, String groupBy, String metric, String agg, int limit) {
        requireValidIdentifier(table, "table");
        requireValidIdentifier(groupBy, "group_by");
        requireValidIdentifier(metric, "metric");
        String aggFn = requireValidAgg(agg).toUpperCase(java.util.Locale.ROOT);
        return "SELECT " + topPrefix(mode, limit) + groupBy + ", " + aggFn + "(" + metric + ") AS value, "
                + "COUNT(*) AS n FROM " + table + " GROUP BY " + groupBy + " ORDER BY value DESC"
                + limitSuffix(mode, limit);
    }

    /** Postgres and Oracle both have a real {@code CORR()} aggregate; MySQL and SQL Server have
     * neither, so their branch computes Pearson's correlation coefficient by hand from the same
     * moments {@code CORR()} itself is defined in terms of. SQL Server's own {@code AVG} does
     * integer division on an integer column (confirmed real behavior, not a hypothetical) --
     * casting to {@code FLOAT} first is what keeps its manual formula from silently truncating;
     * MySQL's {@code AVG} already returns a decimal/float-ish type regardless of input type, so it
     * doesn't need the same cast. */
    static String correlationSql(McpBackendMode mode, String table, String col1, String col2) {
        requireValidIdentifier(table, "table");
        requireValidIdentifier(col1, "col1");
        requireValidIdentifier(col2, "col2");
        String whereBothNotNull = " WHERE " + col1 + " IS NOT NULL AND " + col2 + " IS NOT NULL";
        return switch (mode) {
            case POSTGRES, ORACLE -> "SELECT CORR(" + col1 + ", " + col2 + ") AS correlation, "
                    + "COUNT(*) AS n FROM " + table + whereBothNotNull;
            case MYSQL -> "SELECT (AVG(" + col1 + " * " + col2 + ") - AVG(" + col1 + ") * AVG(" + col2 + ")) "
                    + "/ (STDDEV_POP(" + col1 + ") * STDDEV_POP(" + col2 + ")) AS correlation, "
                    + "COUNT(*) AS n FROM " + table + whereBothNotNull;
            case SQLSERVER -> "SELECT (AVG(CAST(" + col1 + " AS FLOAT) * CAST(" + col2 + " AS FLOAT)) "
                    + "- AVG(CAST(" + col1 + " AS FLOAT)) * AVG(CAST(" + col2 + " AS FLOAT))) "
                    + "/ (STDEVP(" + col1 + ") * STDEVP(" + col2 + ")) AS correlation, "
                    + "COUNT(*) AS n FROM " + table + whereBothNotNull;
        };
    }

    static String sampleRowsSql(McpBackendMode mode, String table, int limit) {
        requireValidIdentifier(table, "table");
        return "SELECT " + topPrefix(mode, limit) + "* FROM " + table + limitSuffix(mode, limit);
    }

    /** Z-score outlier detection -- {@code |value - mean| > threshold * stddev}, computed with the
     * mean/stddev as correlated scalar subqueries rather than a two-step round trip, so this stays
     * one real query per dialect the way every other tool here does. Portable across all four
     * dialects once the STDDEV spelling is right (see {@link #stddevFn}); the only other
     * per-dialect difference is the row cap (see {@link #limitSuffix}). */
    static String findOutliersSql(McpBackendMode mode, String table, String column, double threshold, int limit) {
        requireValidIdentifier(table, "table");
        requireValidIdentifier(column, "column");
        String mean = "(SELECT AVG(" + column + ") FROM " + table + ")";
        String stddev = "(SELECT " + stddevFn(mode) + "(" + column + ") FROM " + table + ")";
        String deviation = "ABS(" + column + " - " + mean + ")";
        return "SELECT " + topPrefix(mode, limit) + "* FROM " + table
                + " WHERE " + column + " IS NOT NULL AND " + deviation + " > " + threshold + " * " + stddev
                + " ORDER BY " + deviation + " DESC" + limitSuffix(mode, limit);
    }

    /** Every table's every column, across the whole schema -- Postgres/MySQL/SQL Server all
     * implement the same ANSI {@code information_schema.columns}; Oracle has no
     * {@code information_schema} at all, so {@code user_tab_columns} (scoped to the connected
     * user's own schema, needing no DBA privilege) is its real equivalent, same reasoning
     * {@code WarpMcpServer#runDescribeTable}'s own Oracle branch already documents. */
    static String inspectSchemaSql(McpBackendMode mode) {
        return switch (mode) {
            case POSTGRES -> "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                    + "FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                    + "ORDER BY table_schema, table_name, ordinal_position";
            case MYSQL -> "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                    + "FROM information_schema.columns "
                    + "WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') "
                    + "ORDER BY table_schema, table_name, ordinal_position";
            case SQLSERVER -> "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                    + "FROM information_schema.columns ORDER BY table_schema, table_name, ordinal_position";
            case ORACLE -> "SELECT USER AS table_schema, table_name, column_name, data_type, nullable AS is_nullable "
                    + "FROM user_tab_columns ORDER BY table_name, column_id";
        };
    }

    /** Every real foreign-key edge in the schema, as {@code (table, column, ref_table,
     * ref_column)} rows -- the raw material {@code JoinPathFinder} builds a graph from and
     * searches for a path over. Postgres/MySQL/SQL Server share one real query: joining
     * {@code information_schema.referential_constraints} to
     * {@code information_schema.key_column_usage} twice (once for the referencing side, once for
     * the referenced side, matched by ordinal position) is genuine ANSI SQL all three implement
     * identically -- confirmed real, not assumed, since MySQL's own {@code key_column_usage}
     * already denormalizes the referenced columns onto the SAME row in a way Postgres/SQL Server
     * don't, but the double-join form works correctly on all three regardless. Oracle has no
     * {@code information_schema} at all; {@code user_cons_columns}/{@code user_constraints}
     * (constraint_type {@code 'R'} for a real foreign key) is its own real equivalent, scoped to
     * the connected user's own schema. */
    static String foreignKeyEdgesSql(McpBackendMode mode) {
        return switch (mode) {
            case POSTGRES, MYSQL, SQLSERVER -> "SELECT kcu1.table_name, kcu1.column_name, "
                    + "kcu2.table_name AS ref_table, kcu2.column_name AS ref_column "
                    + "FROM information_schema.referential_constraints rc "
                    + "JOIN information_schema.key_column_usage kcu1 "
                    + "  ON rc.constraint_name = kcu1.constraint_name AND rc.constraint_schema = kcu1.constraint_schema "
                    + "JOIN information_schema.key_column_usage kcu2 "
                    + "  ON rc.unique_constraint_name = kcu2.constraint_name "
                    + "  AND rc.unique_constraint_schema = kcu2.constraint_schema "
                    + "  AND kcu1.ordinal_position = kcu2.ordinal_position";
            case ORACLE -> "SELECT a.table_name, a.column_name, "
                    + "c_pk.table_name AS ref_table, b.column_name AS ref_column "
                    + "FROM user_cons_columns a "
                    + "JOIN user_constraints c ON a.constraint_name = c.constraint_name AND c.constraint_type = 'R' "
                    + "JOIN user_constraints c_pk ON c.r_constraint_name = c_pk.constraint_name "
                    + "JOIN user_cons_columns b ON c_pk.constraint_name = b.constraint_name AND a.position = b.position";
        };
    }

    private DataInvestigationTools() {
    }
}
