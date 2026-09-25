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
 * Real proof that a bounded {@code ORDER BY ... LIMIT} above a federated JOIN is both handled
 * correctly by the parallel join engine AND actually exercised by it (not silently falling back to
 * the sequential path and only happening to produce a correct result via Calcite's own execution
 * instead) -- captures the coordinator's own log output the same way
 * {@code ParallelJoinRemoteDispatchIntegrationTest} already does, since result correctness alone
 * can't distinguish "our new Sort handling ran" from "it fell back and Calcite sorted it."
 */
class ParallelJoinOrderByLimitIntegrationTest {

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

    private static List<Integer> amountsInOrder(String responseBody) {
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
        JsonArray rows = gson.fromJson(rowsJson, JsonArray.class);
        List<Integer> amounts = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            amounts.add(rows.get(i).getAsJsonObject().get("amount").getAsInt());
        }
        return amounts;
    }

    @Test
    void orderByDescLimitReturnsExactlyTheTopRowsInDescendingOrder() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            for (int i = 1; i <= 20; i++) {
                st.execute("INSERT INTO customers VALUES (" + i + ", 'customer_" + i + "')");
            }
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)");
            for (int i = 1; i <= 20; i++) {
                // amount = i * 10, so the known top-3 by amount descending are 200, 190, 180.
                st.execute("INSERT INTO orders VALUES (" + i + ", " + i + ", " + (i * 10) + ")");
            }
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
                    "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id "
                            + "ORDER BY o.amount DESC LIMIT 3");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        assertTrue(log.contains("executed via the parallel join engine"),
                "the parallel engine's own ORDER BY/LIMIT handling must have actually engaged -- captured log: " + log);
        List<Integer> amounts = amountsInOrder(response.body());
        assertEquals(List.of(200, 190, 180), amounts,
                "must return exactly the top 3 amounts, in descending order -- got: " + amounts);
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
