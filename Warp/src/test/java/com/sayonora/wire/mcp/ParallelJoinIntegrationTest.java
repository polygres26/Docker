package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real, end-to-end proof of Phase 0 of the "Warp-native parallel execution engine" design (the
 * {@code jazzy-wishing-balloon} plan): a two-backend federated JOIN, run once through {@code
 * WARP_PARALLEL_JOIN_ENABLED=true} (multi-threaded hash join, {@link
 * com.sayonora.wire.core.ParallelJoinPlanner}/{@link com.sayonora.wire.core.ParallelJoinExecutor})
 * and once with it left unset (today's unchanged sequential Calcite {@code RelRunner} path), and
 * asserts the two runs return the SAME set of joined rows -- the strongest correctness guarantee
 * available, since the parallel path must always agree with the already-trusted sequential one.
 */
class ParallelJoinIntegrationTest {

    private RealPostgres ordersDb;
    private RealPostgres customersDb;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (customersDb != null) customersDb.close();
        if (ordersDb != null) ordersDb.close();
    }

    private static HttpResponse<String> mcpCall(int mcpPort, String sql) throws Exception {
        JsonObject arguments = new JsonObject();
        arguments.addProperty("sql", sql);
        JsonObject params = new JsonObject();
        params.addProperty("name", "query_federated");
        params.add("arguments", arguments);
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", 1);
        req.addProperty("method", "tools/call");
        req.add("params", params);
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + mcpPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()))
                .build();
        return HttpClient.newHttpClient().send(httpReq, HttpResponse.BodyHandlers.ofString());
    }

    /** {@code query_federated}'s own response shape (see {@code WarpMcpServer#toolCallResult}):
     * {@code {result: {content: [{type:"text", text: "<JSON array of row-maps, itself a string>"}]}}}.
     * Parses the inner JSON-in-a-string row array and returns each row as its own canonical,
     * sorted-key JSON string, in sorted order -- so two runs can be compared as an unordered
     * multiset. The parallel path's own row order is never guaranteed to match the sequential
     * path's (partitions are concatenated in no particular order), which is expected and fine;
     * only the SET of rows must agree. */
    private static List<String> sortedRowStrings(String responseBody) {
        Gson gson = new Gson();
        JsonObject root = gson.fromJson(responseBody, JsonObject.class);
        JsonArray content = root.getAsJsonObject("result").getAsJsonArray("content");
        // toolCallResult(result, note) prepends a human-readable note as its OWN content item
        // before the row-data item -- find the one whose text is actually a JSON array, rather
        // than assuming a fixed index.
        String rowsJson = null;
        for (int i = 0; i < content.size(); i++) {
            String text = content.get(i).getAsJsonObject().get("text").getAsString();
            if (text.stripLeading().startsWith("[")) {
                rowsJson = text;
                break;
            }
        }
        if (rowsJson == null) {
            throw new AssertionError("no content item's text looked like a JSON row array -- got: " + responseBody);
        }
        JsonArray rows = gson.fromJson(rowsJson, JsonArray.class);
        List<String> canonicalRows = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonObject row = rows.get(i).getAsJsonObject();
            List<String> keys = new ArrayList<>(row.keySet());
            keys.sort(String::compareTo);
            StringBuilder sb = new StringBuilder();
            for (String key : keys) {
                sb.append(key).append('=').append(row.get(key)).append(';');
            }
            canonicalRows.add(sb.toString());
        }
        canonicalRows.sort(String::compareTo);
        return canonicalRows;
    }

    @Test
    void parallelEngineAgreesWithTheSequentialPathOnAReal200RowJoin() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        int customerCount = 200;
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            for (int i = 0; i < customerCount; i++) {
                st.execute("INSERT INTO customers VALUES (" + i + ", 'customer_" + i + "')");
            }
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)");
            // Two orders per customer -- a real one-to-many join, not a trivial one-to-one lookup.
            for (int i = 0; i < customerCount; i++) {
                st.execute("INSERT INTO orders VALUES (" + (i * 2) + ", " + i + ", " + (i + 1) + ")");
                st.execute("INSERT INTO orders VALUES (" + (i * 2 + 1) + ", " + i + ", " + (i + 2) + ")");
            }
        }

        String sql = "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id";
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
                .env("WARP_PARALLEL_JOIN_ENABLED", "true")
                .env("WARP_PARALLEL_JOIN_THREADS", "4")
                // This test's dataset (400 joined rows) is realistic for a fast test run but well
                // under WARP_PARALLEL_JOIN_MIN_ROWS's own default (10,000) -- explicitly zero the
                // threshold so the parallel path actually activates here rather than silently
                // falling back, which would make this test pass without exercising anything.
                .env("WARP_PARALLEL_JOIN_MIN_ROWS", "0")
                .start();
        HttpResponse<String> parallelResponse = mcpCall(warp.port("mcp"), sql);
        assertEquals(200, parallelResponse.statusCode());
        List<String> parallelRows = sortedRowStrings(parallelResponse.body());
        warp.close();
        warp = null;

        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                // Deliberately no WARP_PARALLEL_JOIN_ENABLED -- today's unchanged sequential path.
                .start();
        HttpResponse<String> sequentialResponse = mcpCall(warp.port("mcp"), sql);
        assertEquals(200, sequentialResponse.statusCode());
        List<String> sequentialRows = sortedRowStrings(sequentialResponse.body());

        assertEquals(customerCount * 2, sequentialRows.size(), "sanity check on the sequential path's own row count");
        assertEquals(sequentialRows, parallelRows,
                "the parallel join engine must return exactly the same set of rows as the sequential path");
    }

    /** A 3-backend-style shape (no clean single 2-leaf equi-join -- here, no join at all) must fall
     * back to the sequential path cleanly even with the flag on, since {@link
     * com.sayonora.wire.core.ParallelJoinPlanner} only ever activates for a validated 2-backend
     * equi-join shape. */
    @Test
    void fallsBackCleanlyForAPlainSingleBackendQueryEvenWithTheFlagOn() throws Exception {
        ordersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE plain_table (id INTEGER PRIMARY KEY)");
            st.execute("INSERT INTO plain_table VALUES (1), (2), (3)");
        }
        warp = WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .env("WARP_PARALLEL_JOIN_ENABLED", "true")
                .start();

        HttpResponse<String> response = mcpCall(warp.port("mcp"), "SELECT id FROM plain_table");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"id\""), "a plain, non-federated query must still work "
                + "unchanged with the flag on -- got: " + response.body());
    }

    /** {@code WARP_PARALLEL_JOIN_MIN_ROWS}'s own default (10,000) must gate a real, but small,
     * federated join back to the sequential path even with the flag on -- correctness (not
     * activation) is what's proven here, since a caller can't observe from the response alone
     * which path actually ran. */
    @Test
    void aSmallJoinStaysCorrectUnderTheDefaultMinRowsThreshold() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)");
            st.execute("INSERT INTO orders VALUES (1, 1, 50), (2, 2, 75)");
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
                .env("WARP_PARALLEL_JOIN_ENABLED", "true")
                // Deliberately NOT overriding WARP_PARALLEL_JOIN_MIN_ROWS -- its own real default
                // (10,000) must gate this 2-row join back to the sequential path on its own.
                .start();

        HttpResponse<String> response = mcpCall(warp.port("mcp"),
                "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("alice") && response.body().contains("bob"),
                "a small federated join must still return correct results whichever path the "
                        + "MIN_ROWS threshold routes it to -- got: " + response.body());
    }
}
