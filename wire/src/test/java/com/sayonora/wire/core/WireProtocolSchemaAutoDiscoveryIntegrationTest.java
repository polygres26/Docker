package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Real proof that schema auto-discovery federation ({@link SchemaAutoDiscoveryStage}) works for a
 * genuine wire-protocol client -- a real pgjdbc connection through pgwire, no MCP involved at all
 * -- not just MCP's own {@code query_federated} tool. Directly answers the gap raised: "how will a
 * real Postgres/Oracle driver get a federated answer for data outside the backend it's connected
 * to?" The same {@link SchemaAutoDiscovery} resolution core and the same {@link
 * SchemaFederationStage#executeWithMounts} Calcite planning core MCP's tool uses now also run as a
 * real {@link PipelineStage}, in the exact pipeline slot {@link SchemaFederationStage} itself
 * occupies, so a plain, unqualified cross-backend JOIN federates transparently for any protocol.
 */
class WireProtocolSchemaAutoDiscoveryIntegrationTest {

    private RealPostgres ordersDb;
    private RealPostgres customersDb;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (customersDb != null) customersDb.close();
        if (ordersDb != null) ordersDb.close();
    }

    @Test
    void aRealPgjdbcClientFederatesAPlainUnqualifiedCrossBackendJoinWithNoSchemaRules() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)");
            st.execute("INSERT INTO orders VALUES (1, 100, 50), (2, 101, 75)");
        }
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (100, 'alice'), (101, 'bob')");
        }

        String backends = "default=" + ordersDb.jdbcUrl() + "|" + ordersDb.username() + "|" + ordersDb.password()
                + ";customers_backend=" + customersDb.jdbcUrl() + "|" + customersDb.username() + "|" + customersDb.password();
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                // Deliberately NO WARP_ROUTER_SCHEMA_RULES -- the entire point of this test.
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        try (Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres",
                        ordersDb.username(), ordersDb.password());
                Statement st = conn.createStatement();
                // Plain, unqualified table names -- exactly what a real app's own ORM-generated SQL
                // would send, no awareness of Warp's backend topology at all.
                ResultSet rs = st.executeQuery(
                        "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.id")) {
            assertTrue(rs.next(), "the first joined row must exist");
            assertEquals("alice", rs.getString(1));
            assertTrue(rs.next(), "the second joined row must exist");
            assertEquals("bob", rs.getString(1));
        }
    }

    @Test
    void aRealPgjdbcClientGetsARealErrorForAnAmbiguousTableNameRatherThanWrongData() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)");
        }
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)");
        }

        String backends = "default=" + ordersDb.jdbcUrl() + "|" + ordersDb.username() + "|" + ordersDb.password()
                + ";customers_backend=" + customersDb.jdbcUrl() + "|" + customersDb.username() + "|" + customersDb.password();
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres",
                ordersDb.username(), ordersDb.password())) {
            SQLException e = assertThrows(SQLException.class, () -> {
                try (Statement st = conn.createStatement()) {
                    st.executeQuery("SELECT * FROM widgets");
                }
            });
            assertTrue(e.getMessage().contains("ambiguous"),
                    "an ambiguous table name must surface as a real error to a real client, not silently "
                            + "return one backend's data -- got: " + e.getMessage());
        }
    }
}
