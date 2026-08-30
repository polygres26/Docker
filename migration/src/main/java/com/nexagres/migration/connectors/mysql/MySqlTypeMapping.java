package com.nexagres.migration.connectors.mysql;

/**
 * MySQL {@code information_schema.columns.data_type} -> target Postgres column type. Deliberately
 * lossy/simplified in a few places, documented here rather than silently guessed:
 * <ul>
 *   <li>{@code VARCHAR}/{@code CHAR} lose their length constraint -- mapped to plain {@code text}.
 *   Avoids ever failing an insert because a replicated value is longer than the source's own
 *   declared limit would suggest (a real, if rare, occurrence with multi-byte charsets), at the
 *   cost of not enforcing that limit on the target.
 *   <li>{@code TINYINT} (including {@code TINYINT(1)}, MySQL's conventional "boolean") maps to
 *   {@code smallint}, not {@code boolean} -- guessing boolean-vs-integer from a display width is
 *   unreliable (a real {@code TINYINT(1)} counter column exists in the wild), so this stays
 *   unambiguous rather than silently wrong for either case.
 *   <li>{@code ENUM}/{@code SET} map to {@code text} -- their value constraint isn't replicated,
 *   only their data.
 * </ul>
 */
final class MySqlTypeMapping {

    private MySqlTypeMapping() {
    }

    static String toPostgresType(String mysqlDataType) {
        return switch (mysqlDataType.toLowerCase()) {
            case "tinyint", "smallint", "year" -> "smallint";
            case "mediumint", "int", "integer" -> "integer";
            case "bigint" -> "bigint";
            case "decimal", "numeric" -> "numeric";
            case "float" -> "real";
            case "double", "double precision" -> "double precision";
            case "date" -> "date";
            case "datetime", "timestamp" -> "timestamp";
            case "time" -> "time";
            case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set" -> "text";
            case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bit" -> "bytea";
            case "json" -> "jsonb";
            default -> "text"; // an unrecognized type is safer stored as text than rejected outright
        };
    }

    static boolean isBinary(String postgresType) {
        return "bytea".equals(postgresType);
    }
}
