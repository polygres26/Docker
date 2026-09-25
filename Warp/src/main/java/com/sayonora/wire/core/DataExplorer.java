package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Object browser + ad-hoc query console for a single {@link BackendTarget} -- lists
 * schemas/tables/columns via {@code information_schema} (every backend Warp fronts today is
 * Postgres-wire-compatible on the target side, so one query set covers all of them) and runs
 * whatever SQL the caller sends, capped and time-boxed.
 *
 * <p><b>Deliberately bypasses the wire pipeline</b> -- this borrows a connection straight from
 * {@link BackendTarget}, not through pgwire/mywire/etc, so SQL Firewall rules and ACL don't apply
 * here. That's intentional (it's how an admin actually inspects data), but it's why this is
 * reached only through the same bearer-token-guarded admin API surface as the rest of Warp's
 * config endpoints, never from a client-facing wire protocol.
 */
public final class DataExplorer {

    private static final int MAX_ROWS = 500;
    private static final int QUERY_TIMEOUT_SECONDS = 15;

    public record ColumnInfo(String name, String type, boolean nullable) {
    }

    public record TableInfo(String schema, String name, String type) {
    }

    public record QueryResult(List<String> columns, List<List<Object>> rows, int rowCount, boolean truncated, long tookMs) {
    }

    private DataExplorer() {
    }

    public static List<TableInfo> listTables(BackendTarget target) throws SQLException {
        String sql = "SELECT table_schema, table_name, table_type FROM information_schema.tables "
                + "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name";
        List<TableInfo> tables = new ArrayList<>();
        try (Connection conn = target.open();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(new TableInfo(rs.getString(1), rs.getString(2), rs.getString(3)));
            }
        }
        return tables;
    }

    public static List<ColumnInfo> listColumns(BackendTarget target, String schema, String table) throws SQLException {
        String sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        List<ColumnInfo> columns = new ArrayList<>();
        try (Connection conn = target.open(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(new ColumnInfo(rs.getString(1), rs.getString(2), "YES".equals(rs.getString(3))));
                }
            }
        }
        return columns;
    }

    /**
     * Runs {@code sql} as-is against {@code target}, capped at {@value #MAX_ROWS} rows and
     * {@value #QUERY_TIMEOUT_SECONDS}s. No statement-type restriction here (an admin console
     * running DDL/DML against a target it manages is legitimate) -- if that's ever a concern for
     * a given deployment, gate it at the admin-token level, not by parsing the SQL.
     */
    public static QueryResult runQuery(BackendTarget target, String sql) throws SQLException {
        long start = System.nanoTime();
        try (Connection conn = target.open(); Statement st = conn.createStatement()) {
            st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            st.setMaxRows(MAX_ROWS + 1);
            boolean hasResultSet = st.execute(sql);
            long tookMs = (System.nanoTime() - start) / 1_000_000;
            if (!hasResultSet) {
                int updateCount = st.getUpdateCount();
                return new QueryResult(List.of("rows_affected"), List.of(List.of((Object) updateCount)), 1, false, tookMs);
            }
            try (ResultSet rs = st.getResultSet()) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    columns.add(meta.getColumnLabel(i));
                }
                List<List<Object>> rows = new ArrayList<>();
                int count = 0;
                while (rs.next() && count < MAX_ROWS) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    rows.add(row);
                    count++;
                }
                boolean truncated = rs.next();
                return new QueryResult(columns, rows, rows.size(), truncated, tookMs);
            }
        }
    }
}
