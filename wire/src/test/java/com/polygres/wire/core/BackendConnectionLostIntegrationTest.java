package com.polygres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.wire.testsupport.PolyWireProcess;
import com.polygres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * A real driver expects a specific "the connection is dead, reconnect" error, not a generic
 * failure -- Oracle's ORA-03113 ("end-of-file on communication channel") is the canonical example
 * every Oracle driver checks for to decide whether to reconnect rather than surface the error to
 * the application. This proves PolyWire delivers that signal correctly for the three dialects that
 * have a real numbered code for it, using a GENUINE backend outage ({@link RealPostgres#stop()},
 * not a mock or a config flag) while a client session is already open and mid-use -- the exact
 * scenario a driver's reconnect logic exists for.
 *
 * <p>SQL Server is deliberately not covered by an assertion on the client-visible error CODE here
 * (see {@code SqlStateErrorMapper}'s own comment on {@code 08006}): there is no single documented
 * sys.messages number for a dropped connection the way Oracle/MySQL have one, and a real SQL
 * Server client normally detects this via its own transport-level exception (a broken/reset
 * socket), not a numbered error read from the wire -- so there is no "real" SQL Server code this
 * test could assert without inventing one, consistent with this codebase's discipline elsewhere.
 */
class BackendConnectionLostIntegrationTest {

    @Test
    void oracleClientSeesTheRealOra03113WhenTheBackendConnectionIsGenuinelyLost() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("orawire", "POLYWIRE_ORAWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            String url = "jdbc:oracle:thin:@//localhost:" + polywire.port("orawire") + "/anything";
            try (Connection conn = DriverManager.getConnection(url, primary.username(), primary.password())) {
                try (Statement warmup = conn.createStatement()) {
                    warmup.execute("SELECT 1 FROM DUAL");
                }

                primary.stop();
                try {
                    SQLException lost = assertThrows(SQLException.class,
                            () -> {
                                try (Statement st = conn.createStatement()) {
                                    st.execute("SELECT 1 FROM DUAL");
                                }
                            },
                            "a statement against a genuinely dead backend connection must fail");
                    assertEquals(3113, lost.getErrorCode(),
                            "must be the real ORA-03113 (end-of-file on communication channel), "
                                    + "the code Oracle drivers check to decide whether to reconnect");
                    assertNotNull(lost.getMessage());
                } finally {
                    primary.resume();
                }
            }
        }
    }

    @Test
    void mySqlClientSeesTheRealLostConnectionCodeWhenTheBackendConnectionIsGenuinelyLost() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("mywire", "POLYWIRE_MYWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            String url = "jdbc:mysql://localhost:" + polywire.port("mywire") + "/postgres"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
            try (Connection conn = DriverManager.getConnection(url, primary.username(), primary.password())) {
                try (Statement warmup = conn.createStatement()) {
                    warmup.execute("SELECT 1");
                }

                primary.stop();
                try {
                    SQLException lost = assertThrows(SQLException.class,
                            () -> {
                                try (Statement st = conn.createStatement()) {
                                    st.execute("SELECT 1");
                                }
                            },
                            "a statement against a genuinely dead backend connection must fail");
                    assertEquals(2013, lost.getErrorCode(),
                            "must be the real MySQL 2013 (CR_SERVER_LOST -- lost connection during "
                                    + "query), the code MySQL drivers check to decide whether to reconnect");
                    assertNotNull(lost.getMessage());
                } finally {
                    primary.resume();
                }
            }
        }
    }

    /** pgwire is a real Postgres wire-protocol passthrough (see {@code PgWireSessionHandler}) --
     * a real Postgres client already knows how to handle SQLSTATE {@code 57P01} (admin_shutdown)
     * on its own, since that's genuinely Postgres's own error vocabulary, not an emulated one. No
     * dialect-native translation is needed or wanted here; this just confirms the passthrough
     * carries the real SQLSTATE through unmangled, the same way {@code SqlStateErrorMapper}'s
     * other entries carry it through translated for the three emulated dialects. */
    @Test
    void postgresClientSeesTheRealPostgresSqlstateWhenTheBackendConnectionIsGenuinelyLost() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            String url = "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres";
            try (Connection conn = DriverManager.getConnection(url, primary.username(), primary.password())) {
                try (Statement warmup = conn.createStatement()) {
                    warmup.execute("SELECT 1");
                }

                primary.stop();
                try {
                    SQLException lost = assertThrows(SQLException.class,
                            () -> {
                                try (Statement st = conn.createStatement()) {
                                    st.execute("SELECT 1");
                                }
                            },
                            "a statement against a genuinely dead backend connection must fail");
                    assertEquals("57P01", lost.getSQLState(),
                            "a real Postgres client should see Postgres's own real SQLSTATE unchanged");
                } finally {
                    primary.resume();
                }
            }
        }
    }
}
