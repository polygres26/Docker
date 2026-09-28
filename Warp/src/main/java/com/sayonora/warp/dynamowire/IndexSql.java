package com.sayonora.warp.dynamowire;

import java.math.BigDecimal;

/**
 * SQL fragments over the stored item JSON for secondary-index key attributes. The same text is used
 * in {@code CREATE INDEX} and in queries so Postgres can match the expression indexes.
 */
final class IndexSql {

    private IndexSql() {}

    /** A string literal safe to inline (attribute names come from index definitions). */
    static String lit(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    private static String typeKey(TableSchema.KeyAttr k) {
        return k.type();
    }

    /** The ordered/comparable value of an index key attribute. */
    static String keyExpr(TableSchema.KeyAttr k) {
        String raw = "item->" + lit(k.name()) + "->>'" + typeKey(k) + "'";
        return switch (k.type()) {
            case "N" -> "((" + raw + ")::numeric)";
            case "B" -> "(encode(decode(" + raw + ", 'base64'), 'hex') COLLATE \"C\")";
            default -> "((" + raw + ") COLLATE \"C\")";
        };
    }

    /** Predicate: the item has this key attribute (with the declared type), i.e. it is in the index. */
    static String presence(TableSchema.KeyAttr k) {
        return "(item->" + lit(k.name()) + "->>'" + typeKey(k) + "') IS NOT NULL";
    }

    static boolean numeric(TableSchema.KeyAttr k) {
        return "N".equals(k.type());
    }

    /** The bind value for {@link #keyExpr}. */
    static Object bind(TableSchema.KeyAttr k, AttributeValue v) {
        return switch (k.type()) {
            case "N" -> new BigDecimal(v.scalar);
            case "B" -> KeyCodec.hex(v.bytes());
            default -> v.scalar;
        };
    }
}
