package com.sayonora.warp.dynamowire;

import java.util.Map;

/** DynamoDB's input validation for items and keys (sizes, key types, secondary-index keys). */
final class ItemValidator {

    static final int MAX_ITEM_SIZE = 400 * 1024;
    static final int MAX_PARTITION_KEY_BYTES = 2048;
    static final int MAX_SORT_KEY_BYTES = 1024;

    private ItemValidator() {}

    /** A full item for PutItem / the result of an UpdateItem. */
    static void validateItem(TableSchema s, Map<String, AttributeValue> item, boolean fromUpdate) {
        for (String name : item.keySet()) {
            if (name.isEmpty()) {
                throw DynamoException.validation("One or more parameter values are not valid. Empty attribute name");
            }
        }
        checkKeyAttr(s.partitionKeyName(), s.partitionKeyType(), item.get(s.partitionKeyName()), true);
        if (s.hasSortKey()) {
            checkKeyAttr(s.sortKeyName(), s.sortKeyType(), item.get(s.sortKeyName()), false);
        }
        for (TableSchema.IndexDef idx : s.meta().indexes()) {
            for (TableSchema.KeyAttr k : idx.allKeys()) {
                if (k.name().equals(s.partitionKeyName()) || k.name().equals(s.sortKeyName())) continue;
                AttributeValue v = item.get(k.name());
                if (v == null) continue; // sparse index: item simply is not indexed
                if (!k.type().equals(v.type.name())) {
                    throw DynamoException.validation("One or more parameter values were invalid: Type mismatch for Index Key "
                            + k.name() + " Expected: " + k.type() + " Actual: " + v.type + " IndexName: " + idx.name());
                }
                if (isEmpty(v)) {
                    throw DynamoException.validation("One or more parameter values are not valid. A value specified for a "
                            + "secondary index key is not supported. The AttributeValue for a key attribute cannot contain an "
                            + "empty string value. IndexName: " + idx.name() + ", IndexKey: " + k.name());
                }
            }
        }
        int size = AttributeValue.itemSize(item);
        if (size > MAX_ITEM_SIZE) {
            throw DynamoException.validation(fromUpdate ? "Item size to update has exceeded the maximum allowed size"
                    : "Item size has exceeded the maximum allowed size");
        }
    }

    private static boolean isEmpty(AttributeValue v) {
        return (v.type == AttributeValue.Type.S && v.scalar.isEmpty())
                || (v.type == AttributeValue.Type.B && v.bytes().length == 0);
    }

    private static void checkKeyAttr(String name, String type, AttributeValue v, boolean partition) {
        if (v == null) {
            throw DynamoException.validation("One of the required keys was not given a value");
        }
        if (!type.equals(v.type.name())) {
            throw DynamoException.validation("One or more parameter values were invalid: Type mismatch for key " + name
                    + " expected: " + type + " actual: " + v.type);
        }
        if (isEmpty(v)) {
            throw DynamoException.validation("One or more parameter values are not valid. The AttributeValue for a key "
                    + "attribute cannot contain an empty string value. Key: " + name);
        }
        int bytes = v.type == AttributeValue.Type.S ? AttributeValue.utf8(v.scalar)
                : v.type == AttributeValue.Type.B ? v.bytes().length : 0;
        if (partition && bytes > MAX_PARTITION_KEY_BYTES) {
            throw DynamoException.validation("One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of"
                    + MAX_PARTITION_KEY_BYTES + " bytes");
        }
        if (!partition && bytes > MAX_SORT_KEY_BYTES) {
            throw DynamoException.validation("One or more parameter values were invalid: Aggregated size of all range keys has exceeded the size limit of "
                    + MAX_SORT_KEY_BYTES + " bytes");
        }
    }

    /** A Key map (GetItem / DeleteItem / UpdateItem / batch keys): exactly the table's key attributes. */
    static void validateKey(TableSchema s, Map<String, AttributeValue> key) {
        int expected = s.hasSortKey() ? 2 : 1;
        if (key.size() != expected || !key.containsKey(s.partitionKeyName())
                || (s.hasSortKey() && !key.containsKey(s.sortKeyName()))) {
            throw DynamoException.validation("The provided key element does not match the schema");
        }
        checkKeyAttr(s.partitionKeyName(), s.partitionKeyType(), key.get(s.partitionKeyName()), true);
        if (s.hasSortKey()) checkKeyAttr(s.sortKeyName(), s.sortKeyType(), key.get(s.sortKeyName()), false);
    }

    /** The key of an item. */
    static Map<String, AttributeValue> keyOf(TableSchema s, Map<String, AttributeValue> item) {
        Map<String, AttributeValue> k = new java.util.LinkedHashMap<>();
        k.put(s.partitionKeyName(), item.get(s.partitionKeyName()));
        if (s.hasSortKey()) k.put(s.sortKeyName(), item.get(s.sortKeyName()));
        return k;
    }

    // ---------------------------------------------------------------------- names

    private static final java.util.regex.Pattern NAME_CHARS = java.util.regex.Pattern.compile("[a-zA-Z0-9_.\\-]+");
    private static final String NAME_MESSAGE = "Invalid table/index name.  Table/index names must be between 3 and 255 "
            + "characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'";

    static void validateTableName(String name) {
        if (name == null) {
            throw DynamoException.validation("1 validation error detected: Value null at 'tableName' failed to satisfy "
                    + "constraint: Member must not be null");
        }
        if (name.length() < 3 || name.length() > 255 || !NAME_CHARS.matcher(name).matches()) {
            throw DynamoException.validation(NAME_MESSAGE);
        }
    }

    static void validateIndexName(String name) {
        if (name == null || name.length() < 3 || name.length() > 255 || !NAME_CHARS.matcher(name).matches()) {
            throw DynamoException.validation(NAME_MESSAGE);
        }
    }
}
