package com.polygres.wire.core;

import java.io.Serializable;
import java.util.List;

/**
 * Backend-agnostic result of running a {@link Statement}: either a row set or an update count.
 * {@code Serializable} so the distributed cache (ARCHITECTURE.md §12.3,
 * {@code com.polygres.wire.cluster.CacheStage}) can store one across the network — every row value
 * must itself be {@link Serializable} for that to actually work at runtime; that's already true
 * of everything {@link JdbcBackendExecutor#readResultSet} puts in a row (JDBC's standard
 * String/Number/Boolean/java.sql.Date-family types), not a new constraint introduced here.
 */
public record ExecutionResult(
        boolean isQuery,
        List<ColumnInfo> columns,
        List<List<Object>> rows,
        long updateCount) implements Serializable {

    public static ExecutionResult ofUpdate(long updateCount) {
        return new ExecutionResult(false, List.of(), List.of(), updateCount);
    }

    public static ExecutionResult ofQuery(List<ColumnInfo> columns, List<List<Object>> rows) {
        return new ExecutionResult(true, columns, rows, 0);
    }

    public List<String> columnNames() {
        return columns.stream().map(ColumnInfo::name).toList();
    }

    public List<Integer> columnJdbcTypes() {
        return columns.stream().map(ColumnInfo::jdbcType).toList();
    }
}
