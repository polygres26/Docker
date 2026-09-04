package com.sayonora.wire.core;

import java.io.Serializable;
import java.util.List;

public record ExecutionResult(
        boolean isQuery,
        List<ColumnInfo> columns,
        List<List<Object>> rows,
        long updateCount,
        long generatedKey) implements Serializable {

    public static ExecutionResult ofUpdate(long updateCount) {
        return new ExecutionResult(false, List.of(), List.of(), updateCount, 0);
    }

    /** {@code generatedKey} is the backend's own auto-generated primary key for this statement
     * (e.g. a MySQL AUTO_INCREMENT column, surfaced to the client as MySQL's OK-packet
     * last-insert-id field / {@code Statement.getGeneratedKeys()}) -- 0 when the statement had
     * none (not an insert, or the table has no auto-generated column). */
    public static ExecutionResult ofUpdate(long updateCount, long generatedKey) {
        return new ExecutionResult(false, List.of(), List.of(), updateCount, generatedKey);
    }

    public static ExecutionResult ofQuery(List<ColumnInfo> columns, List<List<Object>> rows) {
        return new ExecutionResult(true, columns, rows, 0, 0);
    }

    public List<String> columnNames() {
        return columns.stream().map(ColumnInfo::name).toList();
    }

    public List<Integer> columnJdbcTypes() {
        return columns.stream().map(ColumnInfo::jdbcType).toList();
    }

    public List<String> columnTypeNames() {
        return columns.stream().map(ColumnInfo::typeName).toList();
    }
}
