package com.polygres.wire.core;

import java.io.Serializable;
import java.util.List;

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
