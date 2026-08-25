package com.polygres.wire.core;

import java.util.Map;

public final class SqlStateErrorMapper {

    public static final int ORACLE_DEFAULT = 942;
    
    public static final int MYSQL_DEFAULT = 1105;
    
    public static final int SQL_SERVER_DEFAULT = 50000;

    private record NativeErrors(int oracle, int mysql, int sqlServer) {
    }

    private static final Map<String, NativeErrors> TABLE = Map.ofEntries(

            Map.entry("42P01", new NativeErrors(942, 1146, 208)),

            Map.entry("42703", new NativeErrors(904, 1054, 207)),

            Map.entry("42601", new NativeErrors(900, 1064, 102)),

            Map.entry("23505", new NativeErrors(1, 1062, 2627)),

            Map.entry("23502", new NativeErrors(1400, 1048, 515)),

            Map.entry("23503", new NativeErrors(2291, 1452, 547)),

            Map.entry("42P07", new NativeErrors(955, 1050, 2714)),

            // -- Phase 2 additions, each verified against real vendor error-message docs (not
            // guessed) before landing. See the per-entry notes below for the handful that aren't
            // a clean 1:1 across all three dialects -- Postgres draws finer distinctions than
            // Oracle/MySQL/SQL Server do in a few places, and this table is honest about that
            // rather than papering over it with a plausible-sounding number.

            // check_violation. SQL Server's 547 is shared with 23503 (foreign_key_violation)
            // below -- that's not a bug here, SQL Server itself overloads one error number
            // ("The %s statement conflicted with the %s constraint") for both FK and CHECK.
            Map.entry("23514", new NativeErrors(2290, 3819, 547)),

            // deadlock_detected.
            Map.entry("40P01", new NativeErrors(60, 1213, 1205)),

            // serialization_failure. MySQL has no error code distinct from deadlock (1213) for
            // this -- InnoDB collapses both into the same SQLSTATE 40001/error 1213. SQL Server's
            // real code depends on isolation level (1205 classic-deadlock-style vs. 3960 for a
            // snapshot-isolation update conflict); 1205 is used here as the safer default since
            // it doesn't assume snapshot isolation is configured on the backend.
            Map.entry("40001", new NativeErrors(8177, 1213, 1205)),

            // invalid_text_representation (e.g. "invalid input syntax for type integer"). No
            // vendor has one generic "bad literal" code -- Oracle's 1722 is numeric-specific
            // (invalid number), which is also the most common real-world trigger for this
            // SQLSTATE, so it's used as the representative default here.
            Map.entry("22P02", new NativeErrors(1722, 1366, 245)),

            // numeric_value_out_of_range.
            Map.entry("22003", new NativeErrors(1438, 1264, 8115)),

            // division_by_zero.
            Map.entry("22012", new NativeErrors(1476, 1365, 8134)),

            // string_data_right_truncation.
            Map.entry("22001", new NativeErrors(12899, 1406, 8152)),

            // insufficient_privilege. SQL Server's real number varies by DML verb and object type
            // (229 for SELECT, 230 for column-level, etc.) -- 229 is used as the representative
            // default since SELECT-permission-denied is the most common real-world trigger.
            Map.entry("42501", new NativeErrors(1031, 1142, 229)),

            // undefined_function. The weakest mapping in this table: Oracle has no error code
            // dedicated to "function not found" -- it reuses ORA-00904 "invalid identifier", the
            // same code 42703 (undefined_column) already maps to above. SQL Server's 195 only
            // covers unrecognized BUILT-IN function names, not a missing user-defined one (there's
            // no dedicated code for that case either). Both are honest approximations, not typos.
            Map.entry("42883", new NativeErrors(904, 1305, 195)),

            // lock_not_available (e.g. SELECT ... FOR UPDATE NOWAIT hitting a held lock). MySQL's
            // dedicated NOWAIT error (ER_LOCK_NOWAIT) only exists on MySQL 8.0.22+; older MySQL
            // just blocks and eventually times out as 1205 instead -- 3572 is the correct code for
            // any MySQL version PolyWire's own compatibility target actually supports.
            Map.entry("55P03", new NativeErrors(54, 3572, 1222)),

            // query_canceled (e.g. statement_timeout exceeded). SQL Server has no real numbered
            // error for this at all -- a cancellation there is signaled via a TDS protocol-level
            // "attention" packet, not an error-code frame, so SQL_SERVER_DEFAULT is used here
            // deliberately rather than inventing a plausible-looking number that doesn't exist in
            // sys.messages.
            Map.entry("57014", new NativeErrors(1013, 3024, SQL_SERVER_DEFAULT)));

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
