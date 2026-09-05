package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof that {@code query_federated} -- a new MCP tool exposing Warp's existing Calcite-based
 * {@code SchemaFederationStage} cross-backend JOIN planner, previously only reachable from a real
 * wire-protocol client -- lets an MCP agent JOIN two tables that live on two DIFFERENT backends
 * (real {@code WARP_ROUTER_SCHEMA_RULES}-routed shards) in one call, and that the tool's own note
 * correctly reports the configured shard group either way.
 */
class QueryFederatedIntegrationTest {

    private RealPostgres ordersDb;
    private RealPostgres customersDb;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (customersDb != null) customersDb.close();
        if (ordersDb != null) ordersDb.close();
    }

    private static HttpResponse<String> mcpCall(int mcpPort, String toolName, String sql) throws Exception {
        JsonObject arguments = new JsonObject();
        arguments.addProperty("sql", sql);
        JsonObject params = new JsonObject();
        params.addProperty("name", toolName);
        params.add("arguments", arguments);
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", 1);
        req.addProperty("method", "tools/call");
        req.add("params", params);
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + mcpPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()))
                .build();
        return HttpClient.newHttpClient().send(httpReq, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void joinsTwoRealSchemaRoutedBackendsInOneFederatedCall() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        // SchemaFederationStage mounts each backend's REAL Postgres schema at the schema-rule's own
        // name (BackendDriverRegistry.realCatalogSchemaName is a no-op for Postgres) -- so
        // "orders_db"/"customers_db" have to be real schemas on their respective backends, not just
        // virtual routing labels, for a query literally referencing "orders_db.orders" to resolve.
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA orders_db");
            st.execute("CREATE TABLE orders_db.orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)");
            st.execute("INSERT INTO orders_db.orders VALUES (1, 100, 50), (2, 101, 75)");
        }
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA customers_db");
            st.execute("CREATE TABLE customers_db.customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers_db.customers VALUES (100, 'alice'), (101, 'bob')");
        }

        // "default" is required whenever WARP_BACKENDS is set explicitly -- sqswire (and other
        // single-backend subsystems) always resolve against BackendRegistry.DEFAULT_BACKEND_NAME
        // regardless of routing rules, and only get it for free when WARP_BACKENDS is left unset.
        String backends = "default=" + ordersDb.jdbcUrl() + "|" + ordersDb.username() + "|" + ordersDb.password()
                + ";customers_backend=" + customersDb.jdbcUrl() + "|" + customersDb.username() + "|" + customersDb.password();
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_ROUTER_SCHEMA_RULES", "orders_db:default,customers_db:customers_backend")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
        int port = warp.port("mcp");

        HttpResponse<String> response = mcpCall(port, "query_federated",
                "SELECT c.name, o.amount FROM orders_db.orders o JOIN customers_db.customers c "
                        + "ON o.customer_id = c.id ORDER BY o.id");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("alice") && response.body().contains("bob"),
                "a real cross-backend JOIN through query_federated must return rows from BOTH real "
                        + "backends in one call -- got: " + response.body());
        assertTrue(response.body().contains("schema-federation rule(s)") && response.body().contains("orders_db"),
                "the tool's note should report the real configured schema-federation rules -- got: " + response.body());
    }

    @Test
    void reportsNoShardGroupConfiguredWhenThereIsNone() throws Exception {
        ordersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE plain_table (id INTEGER PRIMARY KEY)");
            st.execute("INSERT INTO plain_table VALUES (1)");
        }
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        HttpResponse<String> response = mcpCall(warp.port("mcp"), "query_federated", "SELECT id FROM plain_table");
        assertTrue(response.body().contains("No shard group or schema-federation rule is configured"),
                "with neither mechanism configured, the note must say so plainly -- got: " + response.body());
    }

    /** No WARP_ROUTER_SCHEMA_RULES at all -- plain, bare table names, resolved purely by real
     * schema auto-discovery across every registered backend (BackendCatalogDiscovery), per the
     * gap raised directly: federation shouldn't require an operator to pre-declare schema aliases
     * the way sharding does, especially for an MCP/NL-to-SQL caller that has no reason to know any
     * such naming scheme. */
    @Test
    void autoDiscoversAndFederatesAcrossTwoBackendsWithNoSchemaRulesConfigured() throws Exception {
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
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                // Deliberately NO WARP_ROUTER_SCHEMA_RULES -- that's the entire point of this test.
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        HttpResponse<String> response = mcpCall(warp.port("mcp"), "query_federated",
                "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.id");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("alice") && response.body().contains("bob"),
                "a real cross-backend JOIN via plain, unqualified table names must return rows from "
                        + "BOTH real backends purely via auto-discovery -- got: " + response.body());
        assertTrue(response.body().contains("Auto-discovered and federated"),
                "the tool's note should say auto-discovery is what federated this query -- got: " + response.body());
    }

    /** A table name that exists on two different backends is a real ambiguity -- proves it's
     * refused with a clear error rather than silently picking one. */
    @Test
    void refusesAnAmbiguousTableNameFoundOnTwoBackends() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)");
        }
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            // Same table name, a DIFFERENT real backend -- the real ambiguity this test proves is refused.
            st.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)");
        }

        String backends = "default=" + ordersDb.jdbcUrl() + "|" + ordersDb.username() + "|" + ordersDb.password()
                + ";customers_backend=" + customersDb.jdbcUrl() + "|" + customersDb.username() + "|" + customersDb.password();
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        HttpResponse<String> response = mcpCall(warp.port("mcp"), "query_federated", "SELECT * FROM widgets");
        assertTrue(response.body().contains("more than one backend"),
                "an ambiguous table name must be refused with a clear error, not silently resolved -- got: "
                        + response.body());
    }
}
