package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real .NET-equivalent bulk-copy client (mssql-jdbc's {@code
 * SQLServerBulkCopy} -- the pure-Java implementation of the same protocol {@code SqlBulkCopy}
 * uses) can bulk-load rows through mssqlwire -- from the "scope the remaining large items" list:
 * every {@code TDS7_BULK_LOAD_BCP} packet was previously unhandled, falling to a generic
 * "unsupported message type" error that also desynced the connection for the row-data packets
 * that would have followed.
 *
 * <p>Two real, previously-missing prerequisites were needed before BCP could even start (both
 * found live, not assumed): {@code SET FMTONLY ON <select>} (SQLServerBulkCopy's own
 * "describe this table's columns without running the query" probe, sent as a literal
 * {@code sp_executesql N'...'} call with no leading EXEC keyword) needed a real column-metadata
 * answer, and a {@code sys.columns} lookup (collation/computed-ness per column) needed answering
 * from {@code information_schema.columns} since the optional pg_sqlserver emulation extension
 * isn't installed in this test environment. See {@code MssqlWireSessionHandler}'s own javadoc on
 * each for the exact scope/limits of those two fixes.
 */
class MssqlBulkCopyIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private Connection connect() throws Exception {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";databaseName=" + postgres.database();
        return DriverManager.getConnection(url);
    }

    @Test
    void bulkCopyFromAResultSetRoundTripsRealRowsIntoARealTable() throws Exception {
        try (Connection conn = connect(); Statement setup = conn.createStatement()) {
            setup.execute("CREATE TABLE mssql_bcp_src (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            setup.execute("CREATE TABLE mssql_bcp_dst (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            setup.execute("INSERT INTO mssql_bcp_src (id, name) VALUES (1, 'alice')");
            setup.execute("INSERT INTO mssql_bcp_src (id, name) VALUES (2, 'bob')");

            try (Statement srcStmt = conn.createStatement();
                    ResultSet source = srcStmt.executeQuery("SELECT id, name FROM mssql_bcp_src");
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                bulkCopy.setDestinationTableName("mssql_bcp_dst");
                bulkCopy.writeToServer(source);
            }

            try (Statement verify = conn.createStatement();
                    ResultSet rs = verify.executeQuery("SELECT id, name FROM mssql_bcp_dst ORDER BY id")) {
                assertTrue(rs.next(), "the first bulk-loaded row must exist");
                assertEquals(1, rs.getInt(1));
                assertEquals("alice", rs.getString(2));
                assertTrue(rs.next(), "the second bulk-loaded row must exist");
                assertEquals(2, rs.getInt(1));
                assertEquals("bob", rs.getString(2));
                assertTrue(!rs.next(), "no extra rows should have been inserted");
            }

            setup.execute("DROP TABLE mssql_bcp_dst");
            setup.execute("DROP TABLE mssql_bcp_src");
        }
    }
}
