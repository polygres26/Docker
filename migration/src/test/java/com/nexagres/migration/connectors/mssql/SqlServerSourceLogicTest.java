package com.nexagres.migration.connectors.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure logic, no infrastructure needed -- proves {@link SqlServerSource}'s LSN comparison, hex
 * round-trip, and NULL-handling SQL-fragment generation, none of which need a real SQL Server
 * connection to verify. Written specifically because this connector's CDC path could not be
 * verified end to end against any real engine in this environment (see {@code
 * RealAzureSqlEdge}'s own javadoc) -- these pure-logic pieces get real, if narrower, coverage
 * instead of none at all.
 */
class SqlServerSourceLogicTest {

    @Test
    void typeMappingCoversCommonSqlServerTypes() {
        assertEquals("smallint", MsSqlTypeMapping.toPostgresType("tinyint"));
        assertEquals("integer", MsSqlTypeMapping.toPostgresType("int"));
        assertEquals("bigint", MsSqlTypeMapping.toPostgresType("bigint"));
        assertEquals("numeric", MsSqlTypeMapping.toPostgresType("decimal"));
        assertEquals("boolean", MsSqlTypeMapping.toPostgresType("bit"));
        assertEquals("timestamp", MsSqlTypeMapping.toPostgresType("datetime2"));
        assertEquals("timestamptz", MsSqlTypeMapping.toPostgresType("datetimeoffset"));
        assertEquals("text", MsSqlTypeMapping.toPostgresType("nvarchar"));
        assertEquals("bytea", MsSqlTypeMapping.toPostgresType("varbinary"));
        assertEquals("uuid", MsSqlTypeMapping.toPostgresType("uniqueidentifier"));
        assertTrue(MsSqlTypeMapping.isBinary("bytea"));
        assertEquals(false, MsSqlTypeMapping.isBinary("text"));
    }

    @Test
    void hexRoundTripsAnArbitraryLsn() {
        byte[] lsn = { 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x01, (byte) 0xFF, 0x00, 0x01 };
        String hex = SqlServerSource.hex(lsn);
        byte[] roundTripped = SqlServerSource.unhex(hex);
        assertEquals(lsn.length, roundTripped.length);
        for (int i = 0; i < lsn.length; i++) {
            assertEquals(lsn[i], roundTripped[i]);
        }
    }

    @Test
    void lsnComparisonIsUnsignedLexicographic() {
        byte[] smaller = { 0x00, 0x00, 0x00, 0x01 };
        byte[] larger = { 0x00, 0x00, 0x00, (byte) 0xFF }; // 0xFF as a signed byte is negative --
        // the comparison MUST treat it as the larger unsigned value (255), not a negative one, or
        // LSN ordering would be silently wrong for any LSN byte >= 0x80.
        assertTrue(SqlServerSource.compareLsn(smaller, larger) < 0);
        assertTrue(SqlServerSource.compareLsn(larger, smaller) > 0);
        assertEquals(0, SqlServerSource.compareLsn(smaller, smaller));
    }

    @Test
    void nullValueBecomesALiteralNotABindParam() {
        List<String> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        String fragment = SqlServerSource.appendValuePlaceholder(sql, params, null, "text");
        assertEquals("NULL", fragment);
        assertEquals("NULL", sql.toString());
        assertTrue(params.isEmpty(), "a null value must never add a bind param -- see this class's own javadoc for why");
    }

    @Test
    void nonNullValueBecomesATypedPlaceholderWithExactlyOneParam() {
        List<String> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        SqlServerSource.appendValuePlaceholder(sql, params, 42, "integer");
        assertEquals("?::integer", sql.toString());
        assertEquals(List.of("42"), params);
    }

    @Test
    void binaryValueIsHexEncodedAndDecodedViaSql() {
        List<String> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        byte[] raw = { 1, 2, 3 };
        SqlServerSource.appendValuePlaceholder(sql, params, raw, "bytea");
        assertEquals("decode(?, 'hex')", sql.toString());
        assertEquals(List.of(SqlServerSource.hex(raw)), params);
    }
}
