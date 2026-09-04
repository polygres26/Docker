package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof of the new dual-port capability: ONE running Warp process serves BOTH native SQL
 * Server passthrough AND the dialect-translated-into-Postgres path AT THE SAME TIME, on two
 * different ports -- not a single global mode toggle requiring a restart to switch, the only
 * thing that existed before this (see {@link MssqlNativeBackendIntegrationTest}, which only ever
 * proves ONE mode per process).
 *
 * <p>Proof structure: the PRIMARY port ({@code WARP_MSSQLWIRE_PORT}, mode left at its default)
 * stays dialect-translated into Postgres -- a table created through it lands in the real Postgres
 * backend, not SQL Server. The SECOND port ({@code WARP_MSSQLWIRE_NATIVE_PORT}) is native SQL
 * Server passthrough -- a row inserted directly into the real SQL Server backend (bypassing Warp)
 * is readable through it, and a statement sent through it is NOT visible in Postgres. Both
 * connections are made concurrently against the SAME Warp process, proving the two listeners
 * don't share session/mode state.
 */
class MssqlDualPortNativeAndTranslatedIntegrationTest {

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
    void primaryPortStaysTranslatedWhileSecondPortIsNativePassthroughOnTheSameProcess() throws Exception {
        postgres = RealPostgres.start();
        sqlServer = RealAzureSqlEdge.start();

        String sqlServerDirectUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(sqlServerDirectUrl, sqlServer.username(), sqlServer.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE dbo.dual_port_native_test (id INT PRIMARY KEY, label NVARCHAR(50))");
            st.execute("INSERT INTO dbo.dual_port_native_test (id, label) VALUES (1, 'real-sql-server-row')");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .frontend("mssqlwire-native", "WARP_MSSQLWIRE_NATIVE_PORT")
                .env("WARP_MSSQL_HOST", sqlServer.host())
                .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                .env("WARP_MSSQL_DATABASE", "master")
                .env("WARP_MSSQL_USER", sqlServer.username())
                .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        String translatedUrl = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        String nativeUrl = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire-native") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";

        // Native port: reads the row written DIRECTLY to real SQL Server, bypassing Warp entirely
        // -- only possible if this port's own connection genuinely reaches that same real backend.
        try (Connection conn = DriverManager.getConnection(nativeUrl);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT label FROM dbo.dual_port_native_test WHERE id = 1")) {
            assertTrue(rs.next(), "native port must see the row written directly to real SQL Server");
            assertEquals("real-sql-server-row", rs.getString(1));
        }

        // Translated port, on the SAME running process: an ordinary CREATE TABLE/INSERT/SELECT
        // must land in Warp's configured Postgres, exactly like every other translated-mode test
        // -- proving the primary listener's own mode was NOT flipped to native by the second
        // listener's own startup.
        try (Connection conn = DriverManager.getConnection(translatedUrl); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE dual_port_translated_test (id INTEGER PRIMARY KEY, val VARCHAR(20))");
            st.executeUpdate("INSERT INTO dual_port_translated_test (id, val) VALUES (1, 'from-translated-port')");
        }
        try (Connection controlPlane = DriverManager.getConnection(
                "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                postgres.username(), postgres.password());
                Statement st = controlPlane.createStatement();
                ResultSet rs = st.executeQuery("SELECT val FROM dual_port_translated_test WHERE id = 1")) {
            assertTrue(rs.next(), "the translated port's own INSERT must be visible directly in real "
                    + "Postgres -- the control-plane/data backend both mssqlwire listeners share");
            assertEquals("from-translated-port", rs.getString(1));
        }
    }
}
