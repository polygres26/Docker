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
 * Real proof that {@code inspect_schema}'s {@code scope} argument actually reaches every backend
 * it claims to -- the exact gap raised directly: an agent exploring the database before writing a
 * query previously could only ever see the ONE backend its session happened to be connected to,
 * even though schema auto-discovery (query_federated) resolves against every registered backend.
 * Three real Postgres backends, split across a real WARP_BACKEND_GROUPS group and one outside it,
 * proves {@code scope=current} is unchanged, {@code scope=group} sees exactly its own group's
 * members (not the excluded one), and {@code scope=all} sees everything.
 */
class InspectSchemaScopeIntegrationTest {

    private RealPostgres backendA;
    private RealPostgres backendB;
    private RealPostgres backendC;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (backendC != null) backendC.close();
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

    @Test
    void currentGroupAndAllScopesEachSeeExactlyWhatTheyShould() throws Exception {
        backendA = RealPostgres.start();
        backendB = RealPostgres.start();
        backendC = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(backendA.jdbcUrl(), backendA.username(), backendA.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE table_a (id INTEGER PRIMARY KEY)");
        }
        try (Connection c = DriverManager.getConnection(backendB.jdbcUrl(), backendB.username(), backendB.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE table_b (id INTEGER PRIMARY KEY)");
        }
        try (Connection c = DriverManager.getConnection(backendC.jdbcUrl(), backendC.username(), backendC.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE table_c (id INTEGER PRIMARY KEY)");
        }

        String backends = "default=" + backendA.jdbcUrl() + "|" + backendA.username() + "|" + backendA.password()
                + ";backend_b=" + backendB.jdbcUrl() + "|" + backendB.username() + "|" + backendB.password()
                + ";backend_c=" + backendC.jdbcUrl() + "|" + backendC.username() + "|" + backendC.password();
        warp = WarpProcess.builder()
                .pgBackend(backendA.host(), backendA.port(), backendA.database(), backendA.username(), backendA.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                // "default" and "backend_b" form a real named group; "backend_c" is deliberately
                // left OUT of it, to prove scope=group doesn't just show everything.
                .env("WARP_BACKEND_GROUPS", "team_alpha=default,backend_b")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
        int port = warp.port("mcp");

        // scope=current (default, unchanged): only the session's own backend (backendA/"default").
        HttpResponse<String> current = mcpCall(port, "inspect_schema", new JsonObject());
        assertTrue(current.body().contains("table_a"), "current scope must see its own backend's table");
        assertFalse(current.body().contains("table_b") || current.body().contains("table_c"),
                "current scope must NOT see other backends -- got: " + current.body());

        // scope=group: exactly team_alpha's members (default + backend_b), NOT backend_c.
        JsonObject groupArgs = new JsonObject();
        groupArgs.addProperty("scope", "group");
        groupArgs.addProperty("group", "team_alpha");
        HttpResponse<String> group = mcpCall(port, "inspect_schema", groupArgs);
        assertTrue(group.body().contains("table_a") && group.body().contains("table_b"),
                "group scope must see every member of its own group -- got: " + group.body());
        assertFalse(group.body().contains("table_c"),
                "group scope must NOT see a backend outside the named group -- got: " + group.body());

        // scope=all: every registered backend, including the one outside any declared group.
        JsonObject allArgs = new JsonObject();
        allArgs.addProperty("scope", "all");
        HttpResponse<String> all = mcpCall(port, "inspect_schema", allArgs);
        assertTrue(all.body().contains("table_a") && all.body().contains("table_b") && all.body().contains("table_c"),
                "all scope must see every registered backend -- got: " + all.body());
    }

    @Test
    void anUnknownScopeIsRefusedWithAClearError() throws Exception {
        backendA = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(backendA.host(), backendA.port(), backendA.database(), backendA.username(), backendA.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        JsonObject args = new JsonObject();
        args.addProperty("scope", "bogus");
        HttpResponse<String> response = mcpCall(warp.port("mcp"), "inspect_schema", args);
        assertTrue(response.body().contains("unknown scope"),
                "an unrecognized scope must be refused clearly -- got: " + response.body());
    }
}
