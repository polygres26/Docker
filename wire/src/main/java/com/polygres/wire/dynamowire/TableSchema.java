package com.polygres.wire.dynamowire;

/**
 * A DynamoDB table's key schema, as captured at {@code CreateTable} time and persisted in the
 * {@code _dynamo_tables} catalog table (see {@link PgItemStore}). {@code sortKeyName} is null for
 * tables with a partition-key-only schema.
 */
public record TableSchema(
        String tableName,
        String partitionKeyName,
        String partitionKeyType,   // "S" or "N" (B is rejected at CreateTable — see PgItemStore)
        String sortKeyName,        // nullable
        String sortKeyType,        // nullable, "S" or "N"
        String status,
        long creationTimeEpochMillis) {

    public boolean hasSortKey() {
        return sortKeyName != null;
    }
}
