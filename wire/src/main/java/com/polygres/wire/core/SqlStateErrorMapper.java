package com.polygres.wire.core;

import java.util.Map;

/**
 * Maps a Postgres {@code SQLState} (every PolyWire frontend proxies onto a Postgres backend, so
 * this is always the SQLState actually present on the {@link java.sql.SQLException}) to each
 * non-Postgres wire protocol's own real native error number, so a client sees a differentiated,
 * meaningful error instead of the same generic code for every distinct backend failure.
 *
 * <p>Before this class existed, {@code orawire} always answered {@code ORA-00942} for every
 * backend failure, {@code mywire} always answered MySQL {@code 1105}
 * ({@code ER_UNKNOWN_ERROR}), and {@code mssqlwire} always answered SQL Server {@code 50000}
 * (a generic user-defined error) — differentiated only by message text, never by the client's own
 * native error-number field. That made a permission error, a lock timeout, and a genuinely missing
 * object indistinguishable to any client code that branches on error number rather than parsing
 * message text.
 *
 * <p>Deliberately a small, hand-curated table of the SQLStates PolyWire actually exercises live
 * (undefined_table/column, syntax error, unique/not-null/foreign-key violation, duplicate table) —
 * not an exhaustive port of every Postgres errcode. Any SQLState not in the table below falls back
 * to that protocol's original generic default (Oracle 942 / MySQL 1105 / SQL Server 50000) — an
 * explicit, documented fallback, not a silent gap.
 *
 * <p>{@code pgwire} needs no entry here: its client already speaks native Postgres wire-protocol
 * error responses, SQLState included, directly from the backend.
 */
public final class SqlStateErrorMapper {

    /** orawire's pre-existing generic fallback (every backend failure, before this class). */
    public static final int ORACLE_DEFAULT = 942;
    /** mywire's pre-existing generic fallback ({@code ER_UNKNOWN_ERROR}). */
    public static final int MYSQL_DEFAULT = 1105;
    /** mssqlwire's pre-existing generic fallback (generic user-defined TDS error). */
    public static final int SQL_SERVER_DEFAULT = 50000;

    private record NativeErrors(int oracle, int mysql, int sqlServer) {
    }

    private static final Map<String, NativeErrors> TABLE = Map.ofEntries(
            // 42P01 undefined_table -> ORA-00942, ER_NO_SUCH_TABLE, "Invalid object name"
            Map.entry("42P01", new NativeErrors(942, 1146, 208)),
            // 42703 undefined_column -> ORA-00904, ER_BAD_FIELD_ERROR, "Invalid column name"
            Map.entry("42703", new NativeErrors(904, 1054, 207)),
            // 42601 syntax_error -> ORA-00900, ER_PARSE_ERROR, "Incorrect syntax"
            Map.entry("42601", new NativeErrors(900, 1064, 102)),
            // 23505 unique_violation -> ORA-00001, ER_DUP_ENTRY, unique-index violation
            Map.entry("23505", new NativeErrors(1, 1062, 2627)),
            // 23502 not_null_violation -> ORA-01400, ER_BAD_NULL_ERROR, not-null violation
            Map.entry("23502", new NativeErrors(1400, 1048, 515)),
            // 23503 foreign_key_violation -> ORA-02291, ER_NO_REFERENCED_ROW_2, FK violation
            Map.entry("23503", new NativeErrors(2291, 1452, 547)),
            // 42P07 duplicate_table -> ORA-00955, ER_TABLE_EXISTS_ERROR, duplicate object
            Map.entry("42P07", new NativeErrors(955, 1050, 2714)));

    private SqlStateErrorMapper() {
    }

    public static int toOracleError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? ORACLE_DEFAULT : n.oracle();
    }

    public static int toMySqlError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? MYSQL_DEFAULT : n.mysql();
    }

    public static int toSqlServerError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? SQL_SERVER_DEFAULT : n.sqlServer();
    }
}
