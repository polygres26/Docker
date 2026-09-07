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
 * Real proof that the parallel join engine's dynamic filter (this session's own follow-up to the
 * Warp-vs-Trino gap analysis) actually pushes the build side's own real keys into the probe side's
 * extracted SQL, and that the join is still fully correct once it does -- captured-log proof, the
 * same technique established for every other feature in this engine, since a correct result alone
 * can't distinguish "the filter actually got pushed down" from "it silently skipped and the ordinary
 * Bloom filter alone happened to still produce the right answer" (which it would, correctness-wise --
 * this feature is a pure I/O optimization, not a correctness one).
 */
class ParallelJoinDynamicFilterIntegrationTest {

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
    void theProbeSideIsFilteredByTheBuildSideSRealKeysAndTheJoinStaysCorrect() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        int matchingCustomerCount = 5;
        int nonMatchingCustomerCount = 50; // orders referencing NONE of these -- proves the filter cuts real rows
        int ordersPerCustomer = 4;

        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            List<String> values = new ArrayList<>();
            for (int i = 1; i <= matchingCustomerCount; i++) {
                values.add("(" + i + ", 'customer_" + i + "')");
            }
            st.execute("INSERT INTO customers VALUES " + String.join(",", values));
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)");
            List<String> values = new ArrayList<>();
            int orderId = 0;
            // Orders for the matching customers (must survive the join)...
            for (int customerId = 1; customerId <= matchingCustomerCount; customerId++) {
                for (int j = 0; j < ordersPerCustomer; j++) {
                    values.add("(" + (orderId++) + ", " + customerId + ", " + (customerId * 10 + j) + ")");
                }
            }
            // ...and orders for customer ids that don't exist in `customers` at all -- these must be
            // excluded by the join regardless, but a dynamic filter should ALSO stop them from ever
            // being fetched by the probe scan in the first place.
            for (int customerId = 1000; customerId < 1000 + nonMatchingCustomerCount; customerId++) {
                values.add("(" + (orderId++) + ", " + customerId + ", 999)");
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
                .start();

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        HttpResponse<String> response;
        try {
            System.setOut(new PrintStream(new TeeOutputStream(originalOut, captured), true, StandardCharsets.UTF_8));
            // customers is the smaller (build) side; orders (the probe side) is where the dynamic
            // filter should land.
            response = mcpCall(warp.port("mcp"),
                    "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        assertTrue(log.contains("dynamic filter pushed") && log.contains(matchingCustomerCount + " build-side key"),
                "the dynamic filter must have actually pushed the build side's real key set into the "
                        + "probe side's own SQL -- captured log: " + log);
        assertEquals(matchingCustomerCount * ordersPerCustomer, rowCount(response.body()),
                "the join result must still be fully correct -- only the matching customers' orders");
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
