package com.polygres.wire.dynamowire;

public record TableSchema(
        String tableName,
        String partitionKeyName,
        String partitionKeyType,
        String sortKeyName,
        String sortKeyType,
        String status,
        long creationTimeEpochMillis) {

    public boolean hasSortKey() {
        return sortKeyName != null;
    }
}
