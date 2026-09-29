package com.sayonora.warp.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.RealAzureSqlEdge;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real bug, live-verified: a {@code PreparedStatement} with ZERO {@code '?'} bind parameters
 * (e.g. {@code "SELECT payload FROM rtt_bench WHERE id = 1"}), reused across multiple
 * {@code executeQuery()} calls on the SAME statement object, failed on the 2nd+ execution with
 * {@code com.microsoft.sqlserver.jdbc.SQLServerException: sp_executesql call missing a string
 * @stmt parameter}.
 *
 * <p>Root cause, confirmed live: real T-SQL {@code sp_executesql} syntax makes {@code @params}
 * optional, and mssql-jdbc exploits that -- for a statement with no bind parameters, later
 * executions of the same {@code PreparedStatement} object send {@code sp_executesql} with only a
 * single RPC parameter ({@code @stmt}), omitting {@code @params} entirely. {@code
 * MssqlWireSessionHandler#handleRpc} used to require at least 2 RPC parameters for any {@code
 * sp_executesql} call, so it rejected this perfectly valid 1-parameter shape outright.
 *
 * <p>Three executions over the same {@code PreparedStatement}, in both ADAPT (dialect-translated
 * to Postgres) and BRIDGE (verbatim SQL against a pooled real SQL Server backend) mode -- Relay
 * mode is a raw byte proxy to a real backend and was never exposed to this bug.
 */
class MssqlZeroParamPreparedStatementReuseIntegrationTest {

    private static WarpProcess.Builder mssqlwireWarp(RealAzureSqlEdge sqlServer, RealPostgres postgres,
            String backendMode, String database) {
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(),
                        postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_MSSQLWIRE_BACKEND_MODE", backendMode)
                .env("WARP_MSSQL_HOST", sqlServer.host())
                .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                .env("WARP_MSSQL_DATABASE", database)
                .env("WARP_MSSQL_USER", sqlServer.username())
                .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                .env("WARP_OTEL_ENDPOINT", "disabled");
    }

    @Test
    @Timeout(180)
    void adaptModeReusingAZeroParamPreparedStatementThreeTimesWorks() throws Exception {
        try (RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (WarpProcess warp = mssqlwireWarp(sqlServer, postgres, "adapt", "master").start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                        + ";encrypt=false;trustServerCertificate=true";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement setup = conn.createStatement()) {
                    setup.execute("CREATE TABLE rtt_bench (id INT PRIMARY KEY, payload VARCHAR(50))");
                    setup.execute("INSERT INTO rtt_bench VALUES (1, 'via-adapt')");
                }

                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "SELECT payload FROM rtt_bench WHERE id = 1")) {
                    for (int i = 1; i <= 3; i++) {
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next(), "execution " + i + " must return a row");
                            assertEquals("via-adapt", rs.getString(1), "execution " + i + " must return the right row");
                        }
                    }
                }
            }
        }
    }

    @Test
    @Timeout(180)
    void bridgeModeReusingAZeroParamPreparedStatementThreeTimesWorks() throws Exception {
        try (RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
                RealPostgres postgres = RealPostgres.start()) {

            sqlServer.createDatabase("bridgedb");
            String bridgeDbUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                    + ";databaseName=bridgedb;encrypt=false;trustServerCertificate=true";
            try (Connection setup = DriverManager.getConnection(bridgeDbUrl, sqlServer.username(), sqlServer.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE rtt_bench (id INT PRIMARY KEY, payload VARCHAR(50))");
                stmt.execute("INSERT INTO rtt_bench VALUES (1, 'via-bridge')");
            }

            try (WarpProcess warp = mssqlwireWarp(sqlServer, postgres, "bridge", "bridgedb").start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                        + ";encrypt=false;trustServerCertificate=true";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "SELECT payload FROM rtt_bench WHERE id = 1")) {
                    for (int i = 1; i <= 3; i++) {
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next(), "execution " + i + " must return a row");
                            assertEquals("via-bridge", rs.getString(1), "execution " + i + " must return the right row");
                        }
                    }
                }
            }
        }
    }
}
