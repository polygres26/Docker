package com.nexagres.wire.stats;

import com.nexagres.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class NativeStatisticsCollector {

    private static final Logger log = LoggerFactory.getLogger(NativeStatisticsCollector.class);

    Long rowCount(Connection connection, SourceDialect dialect, String schema, String table) {
        try {
            return switch (dialect) {
                case ORACLE -> oracleRowCount(connection, schema, table);
                case POSTGRES -> postgresRowCount(connection, schema, table);
                case MYSQL -> mysqlRowCount(connection, schema, table);
                default -> null;
            };
        } catch (SQLException e) {
            log.warn("stats: failed to read native row-count for {}.{} ({}): {}", schema, table, dialect, e.getMessage());
            return null;
        }
    }

    Map<String, Long> columnDistinctCounts(Connection connection, SourceDialect dialect, String schema, String table, long rowCount) {
        try {
            return switch (dialect) {
                case ORACLE -> oracleColumnDistinctCounts(connection, schema, table);
                case POSTGRES -> postgresColumnDistinctCounts(connection, schema, table, rowCount);
                case MYSQL -> mysqlColumnDistinctCounts(connection, schema, table);
                default -> Map.of();
            };
        } catch (SQLException e) {
            log.warn("stats: failed to read native column-distinct-counts for {}.{} ({}): {}", schema, table, dialect, e.getMessage());
            return Map.of();
        }
    }

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

    private Map<String, Long> mysqlColumnDistinctCounts(Connection connection, String schema, String table) throws SQLException {
        String sql = "SELECT column_name, cardinality FROM information_schema.statistics "
                + "WHERE table_schema = ? AND table_name = ? AND cardinality IS NOT NULL "
                + "AND seq_in_index = 1";
        Map<String, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.putIfAbsent(rs.getString(1), rs.getLong(2));
                }
            }
        }
        return result;
    }
}
