package com.polygres.wire.core;

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
    })
    void mapsEachKnownSqlStateToTheVerifiedNativeCodeForAllThreeDialects(
            String sqlState, int oracle, int mysql, int sqlServer) {
        assertEquals(oracle, SqlStateErrorMapper.toOracleError(sqlState), "Oracle code for " + sqlState);
        assertEquals(mysql, SqlStateErrorMapper.toMySqlError(sqlState), "MySQL code for " + sqlState);
        assertEquals(sqlServer, SqlStateErrorMapper.toSqlServerError(sqlState), "SQL Server code for " + sqlState);
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
