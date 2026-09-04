package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that reusing ONE {@code PreparedStatement} many times -- the entire point of using
 * one -- works through mssqlwire's native-backend mode, closing a real compatibility gap found
 * live while testing multi-engine concurrent traffic this session: a real mssql-jdbc client
 * reusing a {@code PreparedStatement} for a plain 3-column INSERT failed with {@code "RPC
 * parameter \"null\" is BY_REF (output parameter) -- not supported"} on its SECOND execution.
 *
 * <p>Root cause, confirmed live: mssql-jdbc only sends {@code sp_executesql} for the first couple
 * of executions of a given {@code PreparedStatement}. Once "hot," it switches to real
 * server-side statement caching -- confirmed live here as a single {@code sp_prepexec} call (not
 * the separate {@code sp_prepare}+{@code sp_execute} pair some other drivers use, both also
 * supported now) -- which carries a real {@code @handle OUTPUT} parameter {@code
 * RpcRequestReader} used to refuse outright rather than decode. {@code MssqlWireSessionHandler}
 * now handles all four real RPC shapes (see its own {@code SP_*_PROC_ID} constants): {@code
 * sp_executesql}, {@code sp_prepare}, {@code sp_execute}, {@code sp_prepexec}, and {@code
 * sp_unprepare}.
 *
 * <p>Ten executions, not two -- enough to be confident this isn't just "the first switch works,"
 * covering sp_executesql calls, the sp_prepexec handoff, and several sp_execute calls reusing
 * that same handle afterward.
 */
class MssqlPreparedStatementReuseIntegrationTest {

    @Test
    void reusingOnePreparedStatementTenTimesWorksAcrossTheSpExecutesqlToSpPrepexecHandoff() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start()) {
            sqlServer.createDatabase("workdb");
            String directUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                    + ";databaseName=workdb;encrypt=false;trustServerCertificate=true";
            try (Connection c = DriverManager.getConnection(directUrl, sqlServer.username(), sqlServer.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE reuse_it (id INT PRIMARY KEY, label VARCHAR(50))");
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                    .env("WARP_MSSQLWIRE_BACKEND", "sqlserver")
                    .env("WARP_MSSQL_HOST", sqlServer.host())
                    .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                    .env("WARP_MSSQL_DATABASE", "workdb")
                    .env("WARP_MSSQL_USER", sqlServer.username())
                    .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";databaseName=workdb;encrypt=false;"
                        + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
                try (Connection conn = DriverManager.getConnection(url);
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO reuse_it (id, label) VALUES (?, ?)")) {
                    for (int i = 1; i <= 10; i++) {
                        ps.setInt(1, i);
                        ps.setString(2, "row-" + i);
                        assertEquals(1, ps.executeUpdate(), "row " + i + " must insert exactly one row");
                    }
                }
            }

            try (Connection c = DriverManager.getConnection(directUrl, sqlServer.username(), sqlServer.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM reuse_it")) {
                rs.next();
                assertEquals(10, rs.getInt(1), "all 10 reused-PreparedStatement inserts must have "
                        + "landed on the real SQL Server backend");
            }
        }
    }
}
