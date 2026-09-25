package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
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

        // Proof this session's statements now run through the shared pipeline instead of the old
        // JdbcBackendExecutor-direct bypass: a firewall DENY rule against a real SQL Server-bound
        // statement rejects it, exactly as it would for the default dialect-translated Postgres
        // path. warp_firewall_rules lives in Warp's own control-plane Postgres (the same real
        // `postgres` this test already started, per the class javadoc), and FirewallRuleStore's
        // insert trigger NOTIFYs the running Warp process to reload live -- poll briefly since
        // that reload is asynchronous.
        try (Connection controlPlane = DriverManager.getConnection(
                "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                postgres.username(), postgres.password());
                Statement st = controlPlane.createStatement()) {
            // sql_pattern (matched via Pattern#find against the raw SQL text), not table_pattern
            // (matched via SqlTableReferences' schema-qualified FROM/JOIN extraction with anchored
            // ^...$ matching) -- the query below is "FROM dbo.native_test" and a plain
            // table_pattern of "native_test" wouldn't match "dbo.native_test" as extracted (find()
            // does not anchor past the "dbo." prefix). sql_pattern's plain substring search is
            // simpler and dialect-qualification-proof for this test's purpose.
            st.execute("INSERT INTO warp_firewall_rules (priority, action, statement_type, table_pattern, "
                    + "sql_pattern, enabled, description) VALUES (100, 'deny', 'SELECT', NULL, "
                    + "'native_test', true, 'block reads of native_test')");
        }

        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        SQLException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection conn = DriverManager.getConnection(url);
                    Statement st = conn.createStatement()) {
                st.executeQuery("SELECT label FROM dbo.native_test WHERE id = 1");
                Thread.sleep(200);
            } catch (SQLException e) {
                lastFailure = e;
                break;
            }
        }
        if (lastFailure == null) {
            fail("expected the firewall DENY rule (loaded live via warp_firewall_rules) to reject a "
                    + "native-backend-mode SELECT against native_test within 10s -- native mode is still "
                    + "bypassing FirewallStage");
        }
    }
}
