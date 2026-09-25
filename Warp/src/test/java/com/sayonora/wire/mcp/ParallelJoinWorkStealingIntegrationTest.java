package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * Real proof that Phase 2's work-stealing local execution model still produces a CORRECT result
 * under a genuinely skewed key distribution -- one join key accounts for the overwhelming majority
 * of rows, everything else is a long tail of singleton keys. Under the old one-thread-per-partition
 * model this shape would leave whichever partition happened to own the hot key as the sole
 * bottleneck while every other partition's thread finished instantly and sat idle; under work
 * stealing, idle threads pick up other partitions' remaining batches instead. This test can't
 * directly observe which thread processed which row, so it proves the thing that actually matters:
 * every row is still accounted for exactly once, with real multi-partition parallelism enabled.
 */
class ParallelJoinWorkStealingIntegrationTest {

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

    private static int rowCount(String responseBody) {
        Gson gson = new Gson();
        JsonObject root = gson.fromJson(responseBody, JsonObject.class);
        JsonArray content = root.getAsJsonObject("result").getAsJsonArray("content");
        String rowsJson = null;
        for (int i = 0; i < content.size(); i++) {
            String text = content.get(i).getAsJsonObject().get("text").getAsString();
            if (text.stripLeading().startsWith("[")) {
                rowsJson = text;
                break;
            }
        }
        return gson.fromJson(rowsJson, JsonArray.class).size();
    }

    @Test
    void aHeavilySkewedJoinStillProducesEveryRowExactlyOnceUnderWorkStealing() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        int hotCustomerOrderCount = 3000;
        int longTailCustomerCount = 300;

        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (0, 'hot_customer')");
            for (int i = 1; i <= longTailCustomerCount; i++) {
                st.execute("INSERT INTO customers VALUES (" + i + ", 'customer_" + i + "')");
            }
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount NUMERIC)");
            // The hot key: one customer with thousands of orders -- a real skew, not a toy case.
            List<String> values = new ArrayList<>();
            int orderId = 0;
            for (int i = 0; i < hotCustomerOrderCount; i++) {
                values.add("(" + (orderId++) + ", 0, 1)");
            }
            for (int i = 1; i <= longTailCustomerCount; i++) {
                values.add("(" + (orderId++) + ", " + i + ", 1)");
            }
            st.execute("INSERT INTO orders VALUES " + String.join(",", values));
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
                // Several local partitions -- most will finish almost instantly (the long tail),
                // one will own the hot key's huge share. Work stealing is what lets the idle
                // partitions' threads help drain that one partition's backlog instead of the whole
                // query waiting on a single dedicated thread.
                .env("WARP_PARALLEL_JOIN_THREADS", "8")
                .start();

        HttpResponse<String> response = mcpCall(warp.port("mcp"),
                "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id");
        assertEquals(200, response.statusCode());
        assertEquals(hotCustomerOrderCount + longTailCustomerCount, rowCount(response.body()),
                "every order must be joined exactly once, including every one of the hot key's rows");
    }
}
