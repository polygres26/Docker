package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a {@code GROUP BY} aggregation directly above a federated JOIN is both handled
 * correctly by the parallel join engine AND actually exercised by it -- captures the coordinator's
 * own log output the same way the {@code ORDER BY}/{@code LIMIT} and remote-dispatch tests already
 * do, since a correct result alone can't distinguish "our aggregation ran" from "it fell back and
 * Calcite aggregated it sequentially instead."
 */
class ParallelJoinAggregationIntegrationTest {

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

    private static JsonArray rows(String responseBody) {
        Gson gson = new Gson();
        JsonObject root = gson.fromJson(responseBody, JsonObject.class);
        JsonArray content = root.getAsJsonObject("result").getAsJsonArray("content");
        for (int i = 0; i < content.size(); i++) {
            String text = content.get(i).getAsJsonObject().get("text").getAsString();
            if (text.stripLeading().startsWith("[")) {
                return gson.fromJson(text, JsonArray.class);
            }
        }
        throw new AssertionError("no row-array content item found -- got: " + responseBody);
    }

    @Test
    void groupByOverAFederatedJoinComputesSumCountAvgMinMaxCorrectly() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)");
            // alice: orders of 10, 20, 30 -- sum=60, count=3, avg=20, min=10, max=30
            st.execute("INSERT INTO orders VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30)");
            // bob: orders of 5, 15 -- sum=20, count=2, avg=10, min=5, max=15
            st.execute("INSERT INTO orders VALUES (4, 2, 5), (5, 2, 15)");
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
                .env("WARP_PARALLEL_JOIN_MIN_ROWS", "0")
                .env("WARP_PARALLEL_JOIN_THREADS", "4")
                .start();

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        HttpResponse<String> response;
        try {
            System.setOut(new PrintStream(new TeeOutputStream(originalOut, captured), true, StandardCharsets.UTF_8));
            response = mcpCall(warp.port("mcp"),
                    "SELECT c.name, SUM(o.amount) AS total, COUNT(*) AS cnt, AVG(o.amount) AS avg_amount, "
                            + "MIN(o.amount) AS min_amount, MAX(o.amount) AS max_amount "
                            + "FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.name");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        assertTrue(log.contains("executed via the parallel join engine"),
                "the parallel engine's own aggregation handling must have actually engaged -- captured log: " + log);

        JsonArray rows = rows(response.body());
        assertEquals(2, rows.size());
        Map<String, JsonObject> byName = new HashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonObject row = rows.get(i).getAsJsonObject();
            byName.put(row.get("name").getAsString(), row);
        }

        JsonObject alice = byName.get("alice");
        assertEquals(60, alice.get("total").getAsInt());
        assertEquals(3, alice.get("cnt").getAsInt());
        assertEquals(20.0, alice.get("avg_amount").getAsDouble(), 0.001);
        assertEquals(10, alice.get("min_amount").getAsInt());
        assertEquals(30, alice.get("max_amount").getAsInt());

        JsonObject bob = byName.get("bob");
        assertEquals(20, bob.get("total").getAsInt());
        assertEquals(2, bob.get("cnt").getAsInt());
        assertEquals(10.0, bob.get("avg_amount").getAsDouble(), 0.001);
        assertEquals(5, bob.get("min_amount").getAsInt());
        assertEquals(15, bob.get("max_amount").getAsInt());
    }

    /** Real proof of {@code AggregateSpec.outputLayout}'s own reason for existing: nothing requires
     * a {@code SELECT} list to put group keys before aggregates, or in {@code GROUP BY}'s own
     * declared order -- Calcite inserts a reordering {@code Project} above the {@code Aggregate}
     * for exactly this shape, which a naive "group keys first, then aggregates" assumption would
     * get wrong. */
    @Test
    void aggregatesListedBeforeTheGroupKeyInTheSelectListAreStillOrderedCorrectly() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)");
            st.execute("INSERT INTO orders VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5)");
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
                .env("WARP_PARALLEL_JOIN_MIN_ROWS", "0")
                .start();

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        HttpResponse<String> response;
        try {
            System.setOut(new PrintStream(new TeeOutputStream(originalOut, captured), true, StandardCharsets.UTF_8));
            // The aggregate (SUM) is listed BEFORE the group key (name) -- the opposite of
            // GROUP BY's own declared order and of EnumerableAggregate's natural row-type order.
            response = mcpCall(warp.port("mcp"),
                    "SELECT SUM(o.amount) AS total, c.name FROM orders o JOIN customers c "
                            + "ON o.customer_id = c.id GROUP BY c.name");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        assertTrue(log.contains("executed via the parallel join engine"),
                "the parallel engine's own aggregation handling must have actually engaged -- captured log: " + log);

        JsonArray rows = rows(response.body());
        assertEquals(2, rows.size());
        Map<String, Integer> totalByName = new HashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonObject row = rows.get(i).getAsJsonObject();
            totalByName.put(row.get("name").getAsString(), row.get("total").getAsInt());
        }
        assertEquals(30, totalByName.get("alice"));
        assertEquals(5, totalByName.get("bob"));
    }

    private static final class TeeOutputStream extends java.io.OutputStream {
        private final java.io.OutputStream first;
        private final java.io.OutputStream second;

        TeeOutputStream(java.io.OutputStream first, java.io.OutputStream second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void write(int b) throws java.io.IOException {
            first.write(b);
            second.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws java.io.IOException {
            first.write(b, off, len);
            second.write(b, off, len);
        }

        @Override
        public void flush() throws java.io.IOException {
            first.flush();
            second.flush();
        }
    }
}
