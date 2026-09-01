package com.nexagres.wire.core;

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
            // any MySQL version Warp's own compatibility target actually supports.
            Map.entry("55P03", new NativeErrors(54, 3572, 1222)),

            // query_canceled (e.g. statement_timeout exceeded). SQL Server has no real numbered
            // error for this at all -- a cancellation there is signaled via a TDS protocol-level
            // "attention" packet, not an error-code frame, so SQL_SERVER_DEFAULT is used here
            // deliberately rather than inventing a plausible-looking number that doesn't exist in
            // sys.messages.
            Map.entry("57014", new NativeErrors(1013, 3024, SQL_SERVER_DEFAULT)),

            // -- Gap-closing pass against a customer-supplied coverage checklist (see PR/commit
            // notes), each again verified before landing, not guessed.

            // ambiguous_column (e.g. SELECT id FROM parent, child where both have an "id" column).
            // SQL Server's 209 is strongly corroborated across multiple independent sources but
            // Microsoft's own sys.messages catalog page for it 404'd during verification -- flagged
            // here rather than silently treated as equally certain as the primary-source-confirmed
            // entries elsewhere in this table.
            Map.entry("42702", new NativeErrors(918, 1052, 209)),

            // too_many_connections (Postgres refuses a new connection once max_connections is
            // exhausted: "sorry, too many clients already"). SQL Server has no single documented
            // sys.messages number for this the way Oracle/MySQL do -- same treatment as 57014,
            // SQL_SERVER_DEFAULT used deliberately rather than a fabricated number.
            Map.entry("53300", new NativeErrors(18, 1040, SQL_SERVER_DEFAULT)),

            // connection_failure (the backend Postgres connection drops mid-session -- a network
            // blip, Postgres restarting, etc.). Postgres itself defines 08006 as the SQLSTATE for
            // this (verified against Postgres's own errcodes.txt). DialectErrorMessages renders
            // this as fixed dialect-native text (ORA-03113/"Lost connection...") regardless of
            // whatever varying wording the underlying JDBC driver actually attached ("connection
            // reset", "An I/O error occurred", etc.) -- unlike the identifier-bearing templates
            // elsewhere in this file, this one needs no extractor, so that variability doesn't
            // matter. SQL Server has no single canonical number for a mid-session transport
            // failure either (severity-20 "transport-level error" messages aren't consistently
            // numbered across versions), so SQL_SERVER_DEFAULT is used deliberately here too.
            Map.entry("08006", new NativeErrors(3113, 2013, SQL_SERVER_DEFAULT)),

            // admin_shutdown -- the SQLSTATE Postgres itself actually sends ("FATAL: terminating
            // connection due to administrator command") when its own connection is genuinely
            // killed: a graceful backend restart/shutdown, an operator running pg_terminate_backend,
            // a container stop. Confirmed live, not assumed: a real Oracle/MySQL client mid-session
            // against a backend stopped out from under it (RealPostgres#stop(), 3/3 repeated runs)
            // consistently gets exactly this SQLSTATE, not the more generic 08006 above -- Postgres
            // is specific about WHY the connection ended, and this is the code that actually shows
            // up for a driver's disconnect-and-reconnect logic to key off (ORA-03113 is the
            // canonical example: Oracle drivers check for it specifically to decide whether to
            // transparently reconnect rather than surface the error to the application).
            Map.entry("57P01", new NativeErrors(3113, 2013, SQL_SERVER_DEFAULT)),

            // sqlclient_unable_to_establish_sqlconnection -- distinct from 08006/57P01 above (an
            // already-open connection dying) in that this is a NEW connection attempt failing to
            // even establish, once the backend is genuinely down. Confirmed live while verifying
            // mongowire's own connection-lost coverage: a client with automatic retry (MongoDB's
            // retryable reads, on by default) hits this SQLSTATE specifically on its retry attempt,
            // after the first attempt already got 57P01 -- HikariCP's own pool-exhaustion exception
            // carries this code once it can no longer open a fresh physical connection at all.
            Map.entry("08001", new NativeErrors(3113, 2013, SQL_SERVER_DEFAULT)));

    // foreign_key_violation (23503) is the one SQLSTATE in this table where Postgres genuinely
    // collapses a real distinction Oracle and MySQL both keep: an INSERT/UPDATE whose new row
    // references a missing parent ("insert or update on table ... violates foreign key
    // constraint ...") is a DIFFERENT native error than a DELETE/UPDATE blocked because a child
    // row still references the row being removed ("update or delete on table ... violates
    // foreign key constraint ... on table ..."). SQL Server doesn't distinguish the two at the
    // CODE level (547 either way -- only its message wording differs, handled entirely in
    // DialectErrorMessages), so only the Oracle/MySQL codes below actually change on this check.
    // Deliberately NOT anchored with "^" -- a real caught SQLException's getMessage() carries
    // Postgres's own "ERROR: " prefix (confirmed against a live orawire session: the actual text
    // is "ERROR: update or delete on table ..."), so an anchored match against the absolute start
    // of the string would never fire. Caught by OracleJdbcIntegrationTest's real end-to-end test
    // against an actual wire session, not by the narrower unit tests, which used hand-written
    // strings without that prefix and so didn't catch this.
    private static final java.util.regex.Pattern FK_VIOLATION_DELETE_SIDE =
            java.util.regex.Pattern.compile("update or delete on table");

    private static boolean isForeignKeyDeleteSide(String sqlState, String message) {
        return "23503".equals(sqlState) && message != null && FK_VIOLATION_DELETE_SIDE.matcher(message).find();
    }

    private SqlStateErrorMapper() {
    }

    public static int toOracleError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? ORACLE_DEFAULT : n.oracle();
    }

    /** As {@link #toOracleError(String)}, but distinguishes {@code 23503}'s two real directions
     * (see {@link #isForeignKeyDeleteSide}) using the real Postgres message text -- ORA-02292
     * ("child record found") for the delete-side, ORA-02291 ("parent key not found", same as the
     * single-arg overload) otherwise. Every other SQLSTATE behaves identically to the single-arg
     * overload; prefer this overload wherever the real message text is available. */
    public static int toOracleError(String sqlState, String message) {
        return isForeignKeyDeleteSide(sqlState, message) ? 2292 : toOracleError(sqlState);
    }

    public static int toMySqlError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? MYSQL_DEFAULT : n.mysql();
    }

    /** As {@link #toMySqlError(String)}, distinguishing {@code 23503}'s direction the same way
     * {@link #toOracleError(String, String)} does -- 1451 (ER_ROW_IS_REFERENCED_2, "Cannot delete
     * or update a parent row") for the delete-side, 1452 (same as the single-arg overload)
     * otherwise. */
    public static int toMySqlError(String sqlState, String message) {
        return isForeignKeyDeleteSide(sqlState, message) ? 1451 : toMySqlError(sqlState);
    }

    public static int toSqlServerError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? SQL_SERVER_DEFAULT : n.sqlServer();
    }
}
