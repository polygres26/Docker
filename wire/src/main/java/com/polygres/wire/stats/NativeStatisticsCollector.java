package com.polygres.wire.stats;

import com.polygres.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads each real JDBC dialect's own optimizer-maintained statistics — cheap (catalog-view lookups,
 * no scan of the actual table) and about as accurate as the source database's own query planner
 * gets, versus this project running its own {@code COUNT(*)}/{@code COUNT(DISTINCT ...)} (real table
 * scans, expensive at scale) or approximate sampling. {@code null}/absent wherever the source has
 * never gathered stats for that table or column (a real, honest possibility — see each dialect's own
 * note below), which {@link StatisticsScheduler} treats as "nothing to store," never as zero.
 *
 * <p><b>Row count and per-column distinct-value counts</b> — see {@link TableStatistics}'s javadoc
 * for why these feed Calcite through two entirely different mechanisms downstream.
 *
 * <p>{@code GENERIC_REST}-shaped backends (REST/S3/SharePoint) have no native optimizer to read from
 * at all — out of scope for this collector; a sampling-based collector for those is real, separate
 * future work, not attempted here.
 */
final class NativeStatisticsCollector {

    private static final Logger log = LoggerFactory.getLogger(NativeStatisticsCollector.class);

    /**
     * @param schema the real database schema/owner the table lives in (already resolved by the
     *   caller — see {@code FederationStage}'s own "backend name doubles as the real schema name"
     *   convention this project already relies on elsewhere).
     * @param table the bare table name, unqualified.
     * @return {@code null} if this dialect isn't supported, the table isn't found, or the source has
     *   no stats gathered for it yet.
     */
    Long rowCount(Connection connection, SourceDialect dialect, String schema, String table) {
        try {
            return switch (dialect) {
                case ORACLE -> oracleRowCount(connection, schema, table);
                case POSTGRES -> postgresRowCount(connection, schema, table);
                case MYSQL -> mysqlRowCount(connection, schema, table);
                default -> null; // no native optimizer to read from -- see class javadoc
            };
        } catch (SQLException e) {
            log.warn("stats: failed to read native row-count for {}.{} ({}): {}", schema, table, dialect, e.getMessage());
            return null;
        }
    }

    /**
     * Per-column distinct-value (NDV) counts — real numbers only for columns the source has
     * actually gathered/estimated stats for; a column absent from the returned map means "unknown,"
     * not "one distinct value." See each dialect branch's own note for exactly what's read and its
     * honest coverage gaps.
     */
    Map<String, Long> columnDistinctCounts(Connection connection, SourceDialect dialect, String schema, String table, long rowCount) {
        try {
            return switch (dialect) {
                case ORACLE -> oracleColumnDistinctCounts(connection, schema, table);
                case POSTGRES -> postgresColumnDistinctCounts(connection, schema, table, rowCount);
                case MYSQL -> mysqlColumnDistinctCounts(connection, schema, table);
                default -> Map.of(); // no native optimizer to read from -- see class javadoc
            };
        } catch (SQLException e) {
            log.warn("stats: failed to read native column-distinct-counts for {}.{} ({}): {}", schema, table, dialect, e.getMessage());
            return Map.of();
        }
    }

    /** {@code ALL_TABLES.NUM_ROWS} — populated by {@code DBMS_STATS.GATHER_TABLE_STATS} (or the scheduler's own auto-stats job, on by default in modern Oracle); {@code NULL} until stats have ever been gathered for this table, a real and common state for a freshly created table. */
    private Long oracleRowCount(Connection connection, String schema, String table) throws SQLException {
        String sql = "SELECT num_rows FROM all_tables WHERE owner = ? AND table_name = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema.toUpperCase(java.util.Locale.ROOT));
            ps.setString(2, table.toUpperCase(java.util.Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                long value = rs.getLong(1);
                return rs.wasNull() ? null : value;
            }
        }
    }

    /** {@code ALL_TAB_COL_STATISTICS.NUM_DISTINCT} — the same {@code DBMS_STATS.GATHER_TABLE_STATS} call that populates {@code NUM_ROWS} gathers this per column too, so coverage tracks the table-level stat exactly: if row count was collected, column NDV almost always was too. */
    private Map<String, Long> oracleColumnDistinctCounts(Connection connection, String schema, String table) throws SQLException {
        String sql = "SELECT column_name, num_distinct FROM all_tab_col_statistics "
                + "WHERE owner = ? AND table_name = ? AND num_distinct IS NOT NULL";
        Map<String, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema.toUpperCase(java.util.Locale.ROOT));
            ps.setString(2, table.toUpperCase(java.util.Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString(1).toLowerCase(java.util.Locale.ROOT), rs.getLong(2));
                }
            }
        }
        return result;
    }

    /** {@code pg_stat_user_tables.n_live_tup} -- Postgres's own live-tuple estimate, maintained continuously by autovacuum/autoanalyze (not just after an explicit {@code ANALYZE}), so it's rarely null for a table that's had any real traffic. */
    private Long postgresRowCount(Connection connection, String schema, String table) throws SQLException {
        String sql = "SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = ? AND relname = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema.toLowerCase(java.util.Locale.ROOT));
            ps.setString(2, table.toLowerCase(java.util.Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                long value = rs.getLong(1);
                return rs.wasNull() ? null : value;
            }
        }
    }

    /**
     * {@code pg_stats.n_distinct} — populated by {@code ANALYZE}. Postgres's own well-documented
     * dual encoding, handled explicitly rather than passed through raw: a positive value is a real
     * absolute distinct count; a negative value is {@code -(distinct/rowCount)} — a ratio, used when
     * the planner believes distinct values scale with table size (most non-enum columns) rather than
     * staying fixed — converted here to an absolute count using the row count already collected for
     * this table, so every value this method returns is a real count, never a ratio a caller would
     * have to know to reinterpret.
     */
    private Map<String, Long> postgresColumnDistinctCounts(Connection connection, String schema, String table, long rowCount) throws SQLException {
        String sql = "SELECT attname, n_distinct FROM pg_stats WHERE schemaname = ? AND tablename = ?";
        Map<String, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema.toLowerCase(java.util.Locale.ROOT));
            ps.setString(2, table.toLowerCase(java.util.Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String column = rs.getString(1);
                    double nDistinct = rs.getDouble(2);
                    if (rs.wasNull() || nDistinct == 0) {
                        continue;
                    }
                    long absolute = nDistinct < 0
                            ? Math.round(-nDistinct * rowCount)
                            : Math.round(nDistinct);
                    if (absolute > 0) {
                        result.put(column, absolute);
                    }
                }
            }
        }
        return result;
    }

    /** {@code information_schema.TABLES.TABLE_ROWS} -- InnoDB's own approximate row-count estimate (refreshed by {@code ANALYZE TABLE}, and periodically by InnoDB's background stats thread); approximate by MySQL's own documentation, not exact, same honest caveat as Postgres's live-tuple estimate. */
    private Long mysqlRowCount(Connection connection, String schema, String table) throws SQLException {
        String sql = "SELECT table_rows FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                long value = rs.getLong(1);
                return rs.wasNull() ? null : value;
            }
        }
    }

    /**
     * {@code information_schema.STATISTICS.CARDINALITY} — MySQL/InnoDB's approximate per-index
     * column cardinality (refreshed by {@code ANALYZE TABLE}). A real, honest coverage gap unique to
     * this dialect: unlike Oracle/Postgres, MySQL exposes no general per-column NDV independent of
     * an index — a column with no index on it gets no entry here at all, not an approximation.
     */
    private Map<String, Long> mysqlColumnDistinctCounts(Connection connection, String schema, String table) throws SQLException {
        String sql = "SELECT column_name, cardinality FROM information_schema.statistics "
                + "WHERE table_schema = ? AND table_name = ? AND cardinality IS NOT NULL "
                + "AND seq_in_index = 1"; // first column of each index only -- cardinality for later columns is compound, not per-column
        Map<String, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.putIfAbsent(rs.getString(1), rs.getLong(2)); // putIfAbsent -- multiple indexes can share a leading column, first wins
                }
            }
        }
        return result;
    }
}
