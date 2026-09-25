package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Locks in every SQLSTATE -> native-error-code mapping in {@link SqlStateErrorMapper} against the
 * real, verified numbers documented inline in that class -- each was checked against actual
 * vendor error-message references (Oracle's official error docs, MySQL's Server Error Reference,
 * SQL Server's sys.messages) before landing, not guessed. This test exists so a future edit that
 * accidentally transposes two numbers, or "simplifies" one of the deliberately-approximate
 * entries (42883, 40001, 57014), fails loudly instead of silently shipping a wrong error code to
 * a real client.
 */
class SqlStateErrorMapperTest {

    // sqlState, oracle, mysql, sqlServer
    @ParameterizedTest
    @CsvSource({
            "42P01, 942,   1146, 208",
            "42703, 904,   1054, 207",
            "42601, 900,   1064, 102",
            "23505, 1,     1062, 2627",
            "23502, 1400,  1048, 515",
            "23503, 2291,  1452, 547",
            "42P07, 955,   1050, 2714",
            "23514, 2290,  3819, 547",
            "40P01, 60,    1213, 1205",
            "40001, 8177,  1213, 1205",
            "22P02, 1722,  1366, 245",
            "22003, 1438,  1264, 8115",
            "22012, 1476,  1365, 8134",
            "22001, 12899, 1406, 8152",
            "42501, 1031,  1142, 229",
            "42883, 904,   1305, 195",
            "55P03, 54,    3572, 1222",
            "57014, 1013,  3024, 50000",
            "42702, 918,   1052, 209",
            "53300, 18,    1040, 50000",
            "08006, 3113,  2013, 50000",
    })
    void mapsEachKnownSqlStateToTheVerifiedNativeCodeForAllThreeDialects(
            String sqlState, int oracle, int mysql, int sqlServer) {
        assertEquals(oracle, SqlStateErrorMapper.toOracleError(sqlState), "Oracle code for " + sqlState);
        assertEquals(mysql, SqlStateErrorMapper.toMySqlError(sqlState), "MySQL code for " + sqlState);
        assertEquals(sqlServer, SqlStateErrorMapper.toSqlServerError(sqlState), "SQL Server code for " + sqlState);
    }

    @Test
    void foreignKeyViolationDefaultsToTheInsertSideCodeWithNoMessageArgument() {
        // Same as the single-arg 23503 row above -- the 2-arg overload with a null/unhelpful
        // message must fall back to the same insert-side default, not silently pick a direction.
        assertEquals(2291, SqlStateErrorMapper.toOracleError("23503", null));
        assertEquals(1452, SqlStateErrorMapper.toMySqlError("23503", null));
        assertEquals(2291, SqlStateErrorMapper.toOracleError("23503", "some unrelated message"));
        assertEquals(1452, SqlStateErrorMapper.toMySqlError("23503", "some unrelated message"));
    }

    @Test
    void foreignKeyViolationOnTheDeleteSideGetsTheRealChildRecordFoundCode() {
        // Real captured Postgres wording for a DELETE blocked by an existing child row.
        String deleteSideMessage = "update or delete on table \"parent\" violates foreign key "
                + "constraint \"child_id_fkey\" on table \"child\"";
        assertEquals(2292, SqlStateErrorMapper.toOracleError("23503", deleteSideMessage),
                "Oracle: ORA-02292 (child record found), not ORA-02291 (parent key not found)");
        assertEquals(1451, SqlStateErrorMapper.toMySqlError("23503", deleteSideMessage),
                "MySQL: ER_ROW_IS_REFERENCED_2 (1451), not ER_NO_REFERENCED_ROW_2 (1452)");
        // SQL Server genuinely does NOT distinguish direction at the numeric-code level -- 547
        // either way (only the message wording differs, in DialectErrorMessages).
        assertEquals(547, SqlStateErrorMapper.toSqlServerError("23503"));
    }

    @Test
    void foreignKeyViolationDeleteSideDetectionSurvivesTheRealErrorPrefix() {
        // Regression test for a real bug an end-to-end test caught: a genuinely caught
        // SQLException's getMessage() carries Postgres's own "ERROR: " prefix (confirmed against
        // a live orawire session), which broke an earlier "^update or delete on table" ANCHORED
        // check -- the anchor required the match at position 0, which the prefix always defeated.
        // This uses the message exactly as a real caught exception has it, not the cleaned-up
        // string the test above uses.
        String realCaughtMessage = "ERROR: update or delete on table \"ojdbc_fk_parent\" violates "
                + "foreign key constraint \"ojdbc_fk_child_id_fkey\" on table \"ojdbc_fk_child\"";
        assertEquals(2292, SqlStateErrorMapper.toOracleError("23503", realCaughtMessage));
        assertEquals(1451, SqlStateErrorMapper.toMySqlError("23503", realCaughtMessage));
    }

    @Test
    void foreignKeyViolationOnAnUnrelatedSqlStateIgnoresTheMessageShape() {
        // The delete-side detection is scoped to sqlState 23503 specifically -- a message that
        // happens to start with "update or delete on table" under a DIFFERENT SQLSTATE must not
        // accidentally trigger it.
        String deleteSideShapedMessage = "update or delete on table \"x\" violates foreign key "
                + "constraint \"y\" on table \"z\"";
        assertEquals(SqlStateErrorMapper.toOracleError("40P01"),
                SqlStateErrorMapper.toOracleError("40P01", deleteSideShapedMessage));
    }

    @Test
    void anUnmappedSqlStateFallsBackToEachDialectsDefault() {
        assertEquals(SqlStateErrorMapper.ORACLE_DEFAULT, SqlStateErrorMapper.toOracleError("XX000"));
        assertEquals(SqlStateErrorMapper.MYSQL_DEFAULT, SqlStateErrorMapper.toMySqlError("XX000"));
        assertEquals(SqlStateErrorMapper.SQL_SERVER_DEFAULT, SqlStateErrorMapper.toSqlServerError("XX000"));
    }

    @Test
    void aNullSqlStateFallsBackToEachDialectsDefault() {
        assertEquals(SqlStateErrorMapper.ORACLE_DEFAULT, SqlStateErrorMapper.toOracleError(null));
        assertEquals(SqlStateErrorMapper.MYSQL_DEFAULT, SqlStateErrorMapper.toMySqlError(null));
        assertEquals(SqlStateErrorMapper.SQL_SERVER_DEFAULT, SqlStateErrorMapper.toSqlServerError(null));
    }
}
