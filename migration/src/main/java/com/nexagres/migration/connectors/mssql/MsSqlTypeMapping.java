package com.nexagres.migration.connectors.mssql;

/**
 * SQL Server {@code information_schema.columns.data_type} -> target Postgres column type.
 * Documented simplifications, same principle as {@code MySqlTypeMapping}:
 * <ul>
 *   <li>{@code char}/{@code varchar}/{@code nchar}/{@code nvarchar}/{@code text}/{@code ntext}
 *   lose their length constraint -- mapped to plain {@code text}.
 *   <li>{@code bit} maps to real {@code boolean} (unlike MySQL's ambiguous {@code TINYINT(1)}, SQL
 *   Server's {@code bit} genuinely IS a boolean type -- 0/1/NULL only, no ambiguity to preserve).
 *   <li>{@code uniqueidentifier} maps to Postgres's native {@code uuid} type.
 *   <li>{@code money}/{@code smallmoney} map to {@code numeric} -- the currency semantics aren't
 *   replicated, only the numeric value.
 *   <li>{@code xml} maps to {@code text} -- not parsed/validated as XML on the target.
 * </ul>
 */
final class MsSqlTypeMapping {

    private MsSqlTypeMapping() {
    }

    static String toPostgresType(String sqlServerDataType) {
        return switch (sqlServerDataType.toLowerCase()) {
            case "tinyint", "smallint" -> "smallint";
            case "int" -> "integer";
            case "bigint" -> "bigint";
            case "decimal", "numeric", "money", "smallmoney" -> "numeric";
            case "float" -> "double precision";
            case "real" -> "real";
            case "bit" -> "boolean";
            case "date" -> "date";
            case "datetime", "datetime2", "smalldatetime" -> "timestamp";
            case "datetimeoffset" -> "timestamptz";
            case "time" -> "time";
            case "char", "varchar", "nchar", "nvarchar", "text", "ntext", "xml" -> "text";
            case "binary", "varbinary", "image" -> "bytea";
            case "uniqueidentifier" -> "uuid";
            default -> "text"; // an unrecognized type is safer stored as text than rejected outright
        };
    }

    static boolean isBinary(String postgresType) {
        return "bytea".equals(postgresType);
    }
}
