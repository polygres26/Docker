package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unlike every other mssqlwire test (which proves T-SQL dialect translation into a Postgres
 * backend), this proves {@code WARP_MSSQLWIRE_BACKEND=sqlserver} -- the new native-backend mode,
 * mirroring {@code WARP_ORACLE_BACKEND_MODE=native}/{@code WARP_MYWIRE_BACKEND=mysql} -- actually
 * reaches a REAL SQL Server instance, not a translated-into-Postgres one. Proof: create a table
 * and insert a row directly against the real backend (bypassing Warp entirely), then read it back
 * through a real mssql-jdbc client connected to Warp -- only possible if the native-mode
 * connection genuinely points at the same real SQL Server database, not Warp's own configured
 * Postgres.
 */
class MssqlNativeBackendIntegrationTest {

    private RealPostgres postgres;
    private RealAzureSqlEdge sqlServer;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (sqlServer != null) sqlServer.close();
        if (postgres != null) postgres.close();
    }

    @Test
    void aClientQueryInNativeModeReadsFromTheRealSqlServerBackendNotPostgres() throws Exception {
        // A real Postgres still has to exist -- Warp's own control-plane tables (warp_config,
        // warp_firewall_rules, etc.) always live there regardless of any wire protocol's own
        // backend mode; native mode only changes what a CLIENT QUERY executes against.
        postgres = RealPostgres.start();

        sqlServer = RealAzureSqlEdge.start();
        String dbUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(dbUrl, sqlServer.username(), sqlServer.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE dbo.native_test (id INT PRIMARY KEY, label NVARCHAR(50))");
            st.execute("INSERT INTO dbo.native_test (id, label) VALUES (1, 'real-sql-server-row')");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_MSSQLWIRE_BACKEND", "sqlserver")
                .env("WARP_MSSQL_HOST", sqlServer.host())
                .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                .env("WARP_MSSQL_DATABASE", "master")
                .env("WARP_MSSQL_USER", sqlServer.username())
                .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        // Login credentials here authenticate against WARP's own front door (WARP_AUTH_MODE's
        // default, matching every other mssqlwire test's convention of using the configured
        // Postgres user/password) -- unrelated to which real database backend the query ends up
        // running against once past login. Using the real SQL Server "sa" credentials here would
        // authenticate against the wrong thing (confirmed live: "login failed for user 'sa'").
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        try (Connection conn = DriverManager.getConnection(url);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT label FROM dbo.native_test WHERE id = 1")) {
            assertTrue(rs.next(), "expected the row written directly to the real SQL Server backend");
            assertEquals("real-sql-server-row", rs.getString(1),
                    "the value must come from the real SQL Server table -- getting no row or a "
                            + "different value would mean native mode is still hitting Postgres");
        }
    }
}
