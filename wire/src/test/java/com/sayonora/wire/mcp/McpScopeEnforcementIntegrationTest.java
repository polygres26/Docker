package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * Real proof that {@code WARP_MCP_SCOPE} is an ENFORCED boundary, not just a listing preference --
 * the exact gap raised directly: a {@code scope} argument only constrains what an agent chooses to
 * ask for, nothing stops it from calling {@code execute_sql} against a backend outside that scope
 * anyway. A DATABASE-scoped endpoint must have literally no path to any other backend, even via a
 * bare, unqualified query_federated call that would otherwise auto-discover and federate across
 * everything registered.
 */
class McpScopeEnforcementIntegrationTest {

    private RealPostgres backendA;
    private RealPostgres backendB;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (backendB != null) backendB.close();
        if (backendA != null) backendA.close();
    }

    private static HttpResponse<String> mcpCall(int mcpPort, String toolName, JsonObject arguments) throws Exception {
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

    private static HttpResponse<String> sqlCall(int mcpPort, String toolName, String sql) throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("sql", sql);
        return mcpCall(mcpPort, toolName, args);
    }

    @Test
    void aDatabaseScopedEndpointHasNoPathToAnyOtherBackendEvenViaQueryFederated() throws Exception {
        backendA = RealPostgres.start();
        backendB = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(backendA.jdbcUrl(), backendA.username(), backendA.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (1, 'alice')");
        }
        try (Connection c = DriverManager.getConnection(backendB.jdbcUrl(), backendB.username(), backendB.password());
                Statement st = c.createStatement()) {
            // A table with the SAME name on the OTHER backend -- if scope enforcement leaked, a
            // bare "SELECT * FROM secrets" could accidentally (or an agent could deliberately)
            // resolve here instead.
            st.execute("CREATE TABLE secrets (id INTEGER PRIMARY KEY, value VARCHAR(50))");
            st.execute("INSERT INTO secrets VALUES (1, 'do-not-leak')");
        }

        String backends = "default=" + backendA.jdbcUrl() + "|" + backendA.username() + "|" + backendA.password()
                + ";backend_b=" + backendB.jdbcUrl() + "|" + backendB.username() + "|" + backendB.password();
        warp = WarpProcess.builder()
                .pgBackend(backendA.host(), backendA.port(), backendA.database(), backendA.username(), backendA.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_MCP_SCOPE", "db:default")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
        int port = warp.port("mcp");

        // inspect_schema: sees only "default"'s own table, never "secrets".
        HttpResponse<String> schema = mcpCall(port, "inspect_schema", new JsonObject());
        assertTrue(schema.body().contains("customers"), "must see its own scoped backend's table");
        assertFalse(schema.body().contains("secrets"), "must never see a table on a backend outside scope");

        // query_federated with a BARE reference to the other backend's table: no federation path
        // exists (auto-discovery is scoped too), so this must fail as an ordinary "no such table"
        // error against backendA -- NOT silently return backendB's row.
        HttpResponse<String> leaked = sqlCall(port, "query_federated", "SELECT value FROM secrets");
        assertFalse(leaked.body().contains("do-not-leak"),
                "a DATABASE-scoped endpoint must never be able to read a table on another backend -- got: "
                        + leaked.body());

        // execute_sql against its own scoped backend still works normally.
        HttpResponse<String> ok = sqlCall(port, "execute_sql", "SELECT name FROM customers WHERE id = 1");
        assertTrue(ok.body().contains("alice"), "the scoped backend's own data must still be reachable");
    }

    @Test
    void aGroupScopedEndpointsInspectSchemaAndQueryFederatedSeeOnlyItsOwnGroup() throws Exception {
        backendA = RealPostgres.start();
        backendB = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(backendA.jdbcUrl(), backendA.username(), backendA.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)");
            st.execute("INSERT INTO orders VALUES (1, 100, 50)");
        }
        try (Connection c = DriverManager.getConnection(backendB.jdbcUrl(), backendB.username(), backendB.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (100, 'alice')");
        }

        String backends = "default=" + backendA.jdbcUrl() + "|" + backendA.username() + "|" + backendA.password()
                + ";backend_b=" + backendB.jdbcUrl() + "|" + backendB.username() + "|" + backendB.password();
        warp = WarpProcess.builder()
                .pgBackend(backendA.host(), backendA.port(), backendA.database(), backendA.username(), backendA.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_BACKEND_GROUPS", "team_alpha=default,backend_b")
                .env("WARP_MCP_SCOPE", "group:team_alpha")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
        int port = warp.port("mcp");

        HttpResponse<String> schema = mcpCall(port, "inspect_schema", new JsonObject());
        assertTrue(schema.body().contains("orders") && schema.body().contains("customers"),
                "a group-scoped endpoint must see every member of its own group -- got: " + schema.body());

        HttpResponse<String> federated = sqlCall(port, "query_federated",
                "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id");
        assertTrue(federated.body().contains("alice"),
                "auto-discovery must still federate correctly WITHIN the endpoint's own group -- got: "
                        + federated.body());
    }
}
