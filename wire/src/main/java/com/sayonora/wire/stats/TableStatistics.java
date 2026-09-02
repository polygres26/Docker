package com.sayonora.wire.stats;

import java.io.Serializable;
import java.util.Map;

public record TableStatistics(
        String qualifiedTableName,
        long rowCount,
        Map<String, Long> columnDistinctCounts,
        long collectedAtMillis) implements Serializable {

    public TableStatistics(String qualifiedTableName, long rowCount, long collectedAtMillis) {
        this(qualifiedTableName, rowCount, Map.of(), collectedAtMillis);
    }
}
