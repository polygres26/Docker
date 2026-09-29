package com.sayonora.warp.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.warp.testsupport.RealAzureSqlEdge;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * mssqlwire's version of {@code com.sayonora.warp.mywire.MySqlRelayBridgeIntegrationTest} -- first
 * live test of mssqlwire's new three-way {@code WARP_MSSQLWIRE_BACKEND_MODE} split (EMULATE,
 * pre-existing; RELAY and BRIDGE, new -- see {@code ServerOptions.MssqlBackendMode}). Uses {@link
 * RealAzureSqlEdge} (a real, ARM64-native SQL-Server-compatible engine) as the target real backend
 * -- see that class's own javadoc for why a real {@code mcr.microsoft.com/mssql/server} image can't
 * be used on this host instead.
 */
class MssqlRelayBridgeIntegrationTest {

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
    void adaptModeDialectTranslatesToTheConfiguredPostgresBackend() throws Exception {
        try (RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (WarpProcess warp = mssqlwireWarp(sqlServer, postgres, "emulate", "master").start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                        + ";encrypt=false;trustServerCertificate=true";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {
                    st.execute("CREATE TABLE adapt_it (id INT PRIMARY KEY, val VARCHAR(50))");
                    st.execute("INSERT INTO adapt_it VALUES (1, 'via-emulate')");
                    try (ResultSet rs = st.executeQuery("SELECT val FROM adapt_it WHERE id = 1")) {
                        assertEquals(true, rs.next(),
                                "EMULATE mode must dialect-translate the client's T-SQL and execute it "
                                        + "against the configured Postgres backend");
                        assertEquals("via-emulate", rs.getString(1));
                    }
                }
            }
        }
    }

    @Test
    @Timeout(180)
    void relayModeProxiesRawBytesToARealSqlServerBackend() throws Exception {
        try (RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
                RealPostgres postgres = RealPostgres.start()) {

            sqlServer.createDatabase("relaydb");
            String relayDbUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                    + ";databaseName=relaydb;encrypt=false;trustServerCertificate=true";
            try (Connection setup = DriverManager.getConnection(relayDbUrl, sqlServer.username(), sqlServer.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE relay_it (id INT PRIMARY KEY, val VARCHAR(50))");
                stmt.execute("INSERT INTO relay_it VALUES (1, 'via-relay')");
            }

            try (WarpProcess warp = mssqlwireWarp(sqlServer, postgres, "relay", "relaydb").start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                        + ";databaseName=relaydb;encrypt=false;trustServerCertificate=true";
                try (Connection conn = DriverManager.getConnection(url, sqlServer.username(), sqlServer.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT val FROM relay_it WHERE id = 1")) {
                    assertEquals(true, rs.next(), "relay must reach the real SQL Server backend and return its row");
                    assertEquals("via-relay", rs.getString(1));
                }
            }
        }
    }

    @Test
    @Timeout(180)
    void bridgeModeExecutesVerbatimSqlAgainstAPooledSqlServerBackend() throws Exception {
        try (RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
                RealPostgres postgres = RealPostgres.start()) {

            sqlServer.createDatabase("bridgedb");
            String bridgeDbUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                    + ";databaseName=bridgedb;encrypt=false;trustServerCertificate=true";
            try (Connection setup = DriverManager.getConnection(bridgeDbUrl, sqlServer.username(), sqlServer.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE bridge_it (id INT PRIMARY KEY, val VARCHAR(50))");
                stmt.execute("INSERT INTO bridge_it VALUES (1, 'via-bridge')");
            }

            try (WarpProcess warp = mssqlwireWarp(sqlServer, postgres, "bridge", "bridgedb").start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                        + ";encrypt=false;trustServerCertificate=true";
                // Bridge mode's client-facing login is a SEPARATE credential from its backend pool
                // (see MssqlBridgePool's own javadoc): the client authenticates against Warp's own
                // CredentialStore, seeded by WarpProcess.Builder#pgBackend from WARP_AUTH_USER/
                // PASSWORD = the Postgres creds, NOT the real SQL Server WARP_MSSQL_USER/PASSWORD
                // the pool itself uses to reach the real backend.
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT val FROM bridge_it WHERE id = 1")) {
                    assertEquals(true, rs.next(),
                            "bridge mode must parse the client's real TDS protocol and execute the "
                                    + "verbatim SQL against a real, pooled SQL Server backend connection");
                    assertEquals("via-bridge", rs.getString(1));
                }
            }
        }
    }
}
