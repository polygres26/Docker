package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of read-write splitting (POLYWIRE_READ_ROUTING_ENABLED): a real pgwire client
 * against a real Main subprocess, with two independent real Postgres containers standing in for
 * primary and standby (no actual streaming replication between them -- each is seeded with its
 * own distinguishing marker row, so which one served a query is unambiguous from the result).
 *
 * <p>This is the gap identified when comparing PolyWire against PgBouncer/OJP/ShardingSphere:
 * PolyWire already had primary/standby failover wiring (PgConnections) but no way to
 * deliberately route a read-only, non-transactional statement to the standby to offload the
 * primary -- a core ShardingSphere feature. See RoutingBackendExecutor#executeOnFreshConnection
 * and PgConnections#openForRead for the implementation this test verifies.
 */
class ReadRoutingIntegrationTest {

    private static RealPostgres primary;
    private static RealPostgres standby;
    private static PolyWireProcess polywire;

    @BeforeAll
    static void startInfra() throws Exception {
        primary = RealPostgres.start();
        standby = RealPostgres.start();

        try (Connection c = DriverManager.getConnection(primary.jdbcUrl(), primary.username(), primary.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE which_host AS SELECT 'PRIMARY' AS host");
            st.execute("CREATE TABLE rw_split_check (id INT)");
        }
        try (Connection c = DriverManager.getConnection(standby.jdbcUrl(), standby.username(), standby.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE which_host AS SELECT 'STANDBY' AS host");
        }

        polywire = PolyWireProcess.builder()
                .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                .env("POLYWIRE_STANDBY_HOST", standby.host())
                .env("POLYWIRE_STANDBY_PORT", String.valueOf(standby.port()))
                .env("POLYWIRE_READ_ROUTING_ENABLED", "true")
                .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (polywire != null) polywire.close();
        if (standby != null) standby.close();
        if (primary != null) primary.close();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, primary.username(), primary.password());
    }

    @Test
    void plainAutocommitReadIsRoutedToStandby() throws SQLException {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT host FROM which_host")) {
            assertEquals(true, rs.next());
            assertEquals("STANDBY", rs.getString("host"),
                    "a plain, non-transactional SELECT with read routing enabled must be served by the standby");
        }
    }

    @Test
    void writeAlwaysGoesToPrimaryRegardlessOfReadRouting() throws SQLException {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO rw_split_check (id) VALUES (1)");
        }
        try (Connection direct = DriverManager.getConnection(primary.jdbcUrl(), primary.username(), primary.password());
                Statement st = direct.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM rw_split_check")) {
            assertEquals(true, rs.next());
            assertEquals(1, rs.getInt(1), "the INSERT must have landed on the primary, not the standby");
        }
    }

    @Test
    void readInsideAnExplicitTransactionStaysOnThePrimary() throws SQLException {
        // Read-write splitting a query mid-transaction would break read-after-write consistency
        // within that transaction -- must never happen regardless of the statement's own kind.
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT host FROM which_host")) {
                assertEquals(true, rs.next());
                assertEquals("PRIMARY", rs.getString("host"),
                        "a SELECT inside an explicit transaction must stay on the primary, never the standby");
            }
            conn.commit();
        }
    }
}
