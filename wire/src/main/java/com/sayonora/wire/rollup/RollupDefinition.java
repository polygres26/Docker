package com.sayonora.wire.rollup;

import java.util.List;

public record RollupDefinition(
        String name,
        String backendName,
        String sourceTable,
        List<String> groupByColumns,
        List<String> aggregations,
        int refreshIntervalMinutes,
        int maxStalenessMinutes) {

    private static final String TABLE_PREFIX = "warp_rollup_";

    public String rollupTableName() {
        return TABLE_PREFIX + name;
    }

    public String definingSql() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", groupByColumns));
        if (!aggregations.isEmpty()) {
            sql.append(", ").append(String.join(", ", aggregations));
        }
        sql.append(" FROM ").append(sourceTable);
        sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        return sql.toString();
    }

    public String createTableSql() {
        return "CREATE TABLE " + rollupTableName() + " AS " + definingSql();
    }

    public String dropTableSql() {
        return "DROP TABLE IF EXISTS " + rollupTableName();
    }
}
