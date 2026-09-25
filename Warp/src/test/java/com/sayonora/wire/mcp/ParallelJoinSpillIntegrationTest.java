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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a build-side partition exceeding {@code WARP_PARALLEL_JOIN_SPILL_THRESHOLD_ROWS}
 * actually spills to disk (via {@link com.sayonora.wire.core.PartitionSpill}) AND still produces a
 * fully correct join -- a correct result alone can't distinguish "spilling actually engaged" from
 * "the threshold never triggered," so this uses the same captured-log engagement proof established
 * for every other feature in this engine. The threshold is set absurdly low (far below any real
 * partition's natural row count) specifically to force every partition to spill deterministically,
 * regardless of how the build side happens to hash-partition.
 */
class ParallelJoinSpillIntegrationTest {

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
        for (int i = 0; i < content.size(); i++) {
            String text = content.get(i).getAsJsonObject().get("text").getAsString();
            if (text.stripLeading().startsWith("[")) {
                return gson.fromJson(text, JsonArray.class).size();
            }
        }
        throw new AssertionError("no row-array content item found -- got: " + responseBody);
    }

    @Test
    void aBuildSidePartitionExceedingTheSpillThresholdStillJoinsEveryRowCorrectly() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        int customerCount = 40; // the BUILD side (smaller table) -- comfortably over the threshold below
        int ordersPerCustomer = 3;

        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            List<String> values = new ArrayList<>();
            for (int i = 1; i <= customerCount; i++) {
                values.add("(" + i + ", 'customer_" + i + "')");
            }
            st.execute("INSERT INTO customers VALUES " + String.join(",", values));
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)");
            List<String> values = new ArrayList<>();
            int orderId = 0;
            for (int customerId = 1; customerId <= customerCount; customerId++) {
                for (int j = 0; j < ordersPerCustomer; j++) {
                    values.add("(" + (orderId++) + ", " + customerId + ", " + (customerId * 10 + j) + ")");
                }
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
                .env("WARP_PARALLEL_JOIN_THREADS", "2")
                // Absurdly low on purpose -- forces every build partition (~20 customers each, with
                // 2 threads) to cross the threshold and spill deterministically, regardless of
                // exactly how customers happen to hash-partition.
                .env("WARP_PARALLEL_JOIN_SPILL_THRESHOLD_ROWS", "3")
                .start();

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        HttpResponse<String> response;
        try {
            System.setOut(new PrintStream(new TeeOutputStream(originalOut, captured), true, StandardCharsets.UTF_8));
            response = mcpCall(warp.port("mcp"),
                    "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        assertTrue(log.contains("exceeded the spill threshold"),
                "at least one partition must have actually spilled to disk -- captured log: " + log);
        assertEquals(customerCount * ordersPerCustomer, rowCount(response.body()),
                "every order must still be joined exactly once, including rows served from a spilled "
                        + "build-side partition");
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
