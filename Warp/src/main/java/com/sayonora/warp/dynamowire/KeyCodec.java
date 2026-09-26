package com.sayonora.warp.dynamowire;

import java.util.Map;

/** How key attribute values are stored in the item table's {@code pk_value}/{@code sk_value} columns. */
final class KeyCodec {

    private KeyCodec() {}

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** S: the string; N: the normalised number text; B: lower-case hex (order preserving under byte
     * order, unlike base64). */
    static String token(AttributeValue v) {
        return switch (v.type) {
            case B -> hex(v.bytes());
            default -> v.scalar;
        };
    }

    static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            out[2 * i] = HEX[(bytes[i] >> 4) & 0xF];
            out[2 * i + 1] = HEX[bytes[i] & 0xF];
        }
        return new String(out);
    }

    static String partitionToken(TableSchema s, Map<String, AttributeValue> item) {
        AttributeValue v = item.get(s.partitionKeyName());
        if (v == null) {
            throw DynamoException.validation("One of the required keys was not given a value");
        }
        return token(v);
    }

    static String sortToken(TableSchema s, Map<String, AttributeValue> item) {
        if (!s.hasSortKey()) return "";
        AttributeValue v = item.get(s.sortKeyName());
        if (v == null) {
            throw DynamoException.validation("One of the required keys was not given a value");
        }
        return token(v);
    }
}
