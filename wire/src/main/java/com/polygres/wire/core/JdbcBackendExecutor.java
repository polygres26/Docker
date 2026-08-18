package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class JdbcBackendExecutor implements BackendExecutor {

    private Connection connection;
    private final com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer;

    public JdbcBackendExecutor(Connection connection) {
        this(connection, null);
    }

    public JdbcBackendExecutor(Connection connection, com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        this.connection = connection;
        this.nativeRlsInitializer = nativeRlsInitializer;
    }

    public void rebind(Connection connection) {
        this.connection = connection;
    }

    @Override
    public ExecutionResult execute(Statement statement) throws SQLException {
        
        if (nativeRlsInitializer != null && !statement.accessContext().isAnonymous()) {
            nativeRlsInitializer.initialize(connection, statement.accessContext());
        }
        String sqlText = stripTrailingSemicolon(statement.sqlText());
        try (PreparedStatement stmt = connection.prepareStatement(sqlText)) {
            return executeOnPreparedStatement(stmt, statement.bindParams());
        }
    }

    static ExecutionResult executeOnPreparedStatement(PreparedStatement stmt, List<Object> binds) throws SQLException {
        for (int i = 0; i < binds.size(); i++) {
            stmt.setObject(i + 1, coerce(binds.get(i)));
        }
        boolean hasResultSet = stmt.execute();
        if (hasResultSet) {
            try (ResultSet rs = stmt.getResultSet()) {
                return readResultSet(rs);
            }
        }
        return ExecutionResult.ofUpdate(Math.max(stmt.getUpdateCount(), 0));
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.stripTrailing();
        return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : sql;
    }

    private static Object coerce(Object value) {
        if (!(value instanceof String s) || s.isEmpty()) {
            return value;
        }
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException ignoredNotAnInteger) {
            
        }
        try {
            return new java.math.BigDecimal(s);
        } catch (NumberFormatException ignoredNotANumber) {
            return s;
        }
    }

    private static ExecutionResult readResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        List<ColumnInfo> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new ColumnInfo(md.getColumnLabel(i), md.getColumnType(i), md.getPrecision(i), md.getScale(i),
                    md.getColumnDisplaySize(i), md.isNullable(i) != ResultSetMetaData.columnNoNulls));
        }
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                row.add(rs.wasNull() ? null : value);
            }
            rows.add(row);
        }
        return ExecutionResult.ofQuery(columns, rows);
    }
}
