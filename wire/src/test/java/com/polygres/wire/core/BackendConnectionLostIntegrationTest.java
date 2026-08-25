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
import java.util.concurrent.CompletableFuture;
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

    /** The OTHER direction: what does a real client see if PolyWire ITSELF dies mid-session, not
     * the backend Postgres? There's nothing running server-side once {@link PolyWireProcess#kill}
     * returns to send a graceful in-protocol error frame the way the other tests in this class
     * prove for a backend outage -- so this deliberately does NOT assert a specific SQLSTATE/error
     * code the way those do. A real client's own transport-level disconnect detection is what
     * actually fires here (the same detection every real driver already relies on for its real
     * target server dying outright), and this test's job is just confirming that detection
     * genuinely fires -- the statement fails, the client doesn't hang forever waiting on a
     * response that's never coming. */
    @Test
    void oracleClientDetectsPolyWireItselfDyingMidSession() throws Exception {
        try (RealPostgres primary = RealPostgres.start()) {
            PolyWireProcess polywire = PolyWireProcess.builder()
                    .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                    .frontend("orawire", "POLYWIRE_ORAWIRE_PORT")
                    .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                    .start();

            String url = "jdbc:oracle:thin:@//localhost:" + polywire.port("orawire") + "/anything";
            try (Connection conn = DriverManager.getConnection(url, primary.username(), primary.password())) {
                try (Statement warmup = conn.createStatement()) {
                    warmup.execute("SELECT 1 FROM DUAL");
                }

                polywire.kill();

                assertThrows(SQLException.class,
                        () -> {
                            try (Statement st = conn.createStatement()) {
                                st.execute("SELECT 1 FROM DUAL");
                            }
                        },
                        "a statement against a genuinely dead PolyWire process must fail, not hang -- "
                                + "the client's own transport-level disconnect detection, since there's "
                                + "no server left running to send a graceful error frame");
            }
        }
    }

    /** Your second question, precisely: what happens when a client is actively mid-fetch (a query
     * already IN FLIGHT, not the next statement issued after already knowing the backend is dead)
     * and Postgres dies WHILE that call is blocked waiting on it? Real timing, not simulated: a
     * background thread starts a genuinely slow query (pg_sleep), the main thread stops Postgres
     * shortly after it's actually running server-side (confirmed via pg_stat_activity, not a fixed
     * sleep guess), and the already-blocked query call is what's asserted on -- proving the SAME
     * 57P01-\>ORA-03113 signal reaches an in-flight call, not just a subsequently-issued one. */
    @Test
    void anInFlightQueryGetsTheSameOra03113WhenPostgresDiesMidExecution() throws Exception {
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

                CompletableFuture<SQLException> inFlightFailure = CompletableFuture.supplyAsync(() -> {
                    try (Statement st = conn.createStatement()) {
                        st.execute("SELECT pg_sleep(30) FROM DUAL");
                        return null;
                    } catch (SQLException e) {
                        return e;
                    }
                });

                // Confirm the query is genuinely running server-side (not just "sent") before
                // pulling the rug out -- via a SEPARATE admin connection straight to Postgres, not
                // through PolyWire, so this check can't itself be affected by what we're about to
                // do to the backend.
                try (Connection admin = DriverManager.getConnection(primary.jdbcUrl(), primary.username(), primary.password())) {
                    long deadline = System.currentTimeMillis() + 10_000;
                    boolean running = false;
                    while (System.currentTimeMillis() < deadline && !running) {
                        try (Statement check = admin.createStatement();
                                var rs = check.executeQuery(
                                        "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep%'")) {
                            running = rs.next() && rs.getInt(1) > 0;
                        }
                        if (!running) {
                            Thread.sleep(100);
                        }
                    }
                    assertTrue(running, "the pg_sleep query must actually be running server-side before we stop Postgres");
                }

                primary.stop();
                try {
                    SQLException lost = inFlightFailure.get(15, java.util.concurrent.TimeUnit.SECONDS);
                    assertNotNull(lost, "the already-in-flight query must fail, not silently return/hang");
                    assertEquals(3113, lost.getErrorCode(),
                            "an in-flight query gets the same real ORA-03113 as a subsequently-issued "
                                    + "one -- the driver's reconnect logic doesn't care which shape of "
                                    + "call was interrupted");
                } finally {
                    primary.resume();
                }
            }
        }
    }
}
