package com.sayonora.migration.connectors.oracle;

/**
 * Oracle {@code all_tab_columns.data_type} -> target Postgres column type. Documented
 * simplifications, same principle as {@code MySqlTypeMapping}/{@code MsSqlTypeMapping}:
 * <ul>
 *   <li>{@code VARCHAR2}/{@code CHAR}/{@code NVARCHAR2}/{@code NCHAR}/{@code CLOB}/{@code NCLOB}
 *   lose their length constraint -- mapped to plain {@code text}.
 *   <li>{@code NUMBER} (with or without declared precision/scale) maps to {@code numeric} --
 *   Oracle's {@code NUMBER} is a single arbitrary-precision type covering everything from a
 *   single-digit flag to a 38-digit value; Postgres {@code numeric} with no declared precision/
 *   scale is the equivalent arbitrary-precision type, so nothing is lost here (unlike the
 *   MySQL/SQL Server mappings' length-constraint simplifications).
 *   <li>{@code DATE} maps to {@code timestamp}, not {@code date} -- Oracle's {@code DATE} always
 *   carries a time component (unlike ANSI SQL {@code DATE}), so {@code timestamp} is the correct
 *   match, not a simplification.
 *   <li>{@code RAW}/{@code LONG RAW}/{@code BLOB} map to {@code bytea}.
 * </ul>
 */
final class OracleTypeMapping {

    private OracleTypeMapping() {
    }

    static String toPostgresType(String oracleDataType) {
        String type = oracleDataType.toUpperCase();
        if (type.startsWith("TIMESTAMP")) {
            return type.contains("WITH TIME ZONE") ? "timestamptz" : "timestamp";
        }
        return switch (type) {
            case "NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE" -> "numeric";
            case "DATE" -> "timestamp";
            case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG" -> "text";
            case "RAW", "LONG RAW", "BLOB" -> "bytea";
            default -> "text"; // an unrecognized type is safer stored as text than rejected outright
        };
    }

    static boolean isBinary(String postgresType) {
        return "bytea".equals(postgresType);
    }
}
