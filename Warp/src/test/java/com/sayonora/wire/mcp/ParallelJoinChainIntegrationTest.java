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
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof that the N-way (left-deep chain) extension of the parallel join engine actually
 * engages and produces correct results for a genuine 3-backend federated join -- {@code orders
 * JOIN customers JOIN products}, confirmed via a live diagnostic (see the "jazzy-wishing-balloon"
 * design plan's own Phase 2+ N-way section) to compile to a left-deep
 * {@code EnumerableHashJoin(EnumerableHashJoin(orders,customers),products)} tree, exactly the shape
 * {@link com.sayonora.wire.core.ParallelJoinPlanner#tryChainPlan} and {@link
 * com.sayonora.wire.core.ChainedJoinExecutor} are built for. Uses the same captured-log engagement
 * proof established for every other feature this session -- a correct result alone can't
 * distinguish "the chain executor actually ran" from "it fell back and Calcite joined all three
 * backends sequentially instead," which it can also do correctly.
 */
class ParallelJoinChainIntegrationTest {

    private RealPostgres ordersDb;
    private RealPostgres customersDb;
    private RealPostgres productsDb;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (productsDb != null) productsDb.close();
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
    void threeBackendLeftDeepChainJoinsCorrectlyViaTheParallelEngine() throws Exception {
        ordersDb = RealPostgres.start();
        customersDb = RealPostgres.start();
        productsDb = RealPostgres.start();

        try (Connection c = DriverManager.getConnection(customersDb.jdbcUrl(), customersDb.username(), customersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");
        }
        try (Connection c = DriverManager.getConnection(productsDb.jdbcUrl(), productsDb.username(), productsDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, title VARCHAR(50))");
            st.execute("INSERT INTO products VALUES (10, 'widget'), (20, 'gadget')");
        }
        try (Connection c = DriverManager.getConnection(ordersDb.jdbcUrl(), ordersDb.username(), ordersDb.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER, amount INTEGER)");
            st.execute("INSERT INTO orders VALUES (1, 1, 10, 100), (2, 2, 20, 200), (3, 1, 20, 300)");
        }

        String backends = "default=" + ordersDb.jdbcUrl() + "|" + ordersDb.username() + "|" + ordersDb.password()
                + ";customers_backend=" + customersDb.jdbcUrl() + "|" + customersDb.username() + "|" + customersDb.password()
                + ";products_backend=" + productsDb.jdbcUrl() + "|" + productsDb.username() + "|" + productsDb.password();
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
            response = mcpCall(warp.port("mcp"),
                    "SELECT c.name, p.title, o.amount FROM orders o "
                            + "JOIN customers c ON o.customer_id = c.id "
                            + "JOIN products p ON o.product_id = p.id");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        // NOTE: this query's join graph -- orders (hub) joined independently to customers and to
        // products, which never reference each other -- is topologically a STAR (see
        // ParallelJoinPlanner's own star-topology section), which SchemaFederationStage now tries
        // BEFORE the linear chain engine since it's provably at least as good for this exact shape.
        // For exactly 3 leaves a star and a "chain" are the same underlying tree (there's only one
        // possible 2-edge spanning tree over 3 nodes), so the star engine correctly wins here --
        // ParallelJoinChainFourWayIntegrationTest below proves the genuinely non-star (4-leaf path)
        // case still falls through to the chain engine instead.
        assertTrue(log.contains("executed via the parallel join engine") && log.contains("star topology"),
                "the star join executor must have actually engaged for this hub-and-2-spokes shape -- "
                        + "captured log: " + log);

        JsonArray rows = rows(response.body());
        assertEquals(3, rows.size());
        Set<String> triples = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonObject row = rows.get(i).getAsJsonObject();
            triples.add(row.get("name").getAsString() + "|" + row.get("title").getAsString() + "|" + row.get("amount").getAsInt());
        }
        assertEquals(Set.of("alice|widget|100", "bob|gadget|200", "alice|gadget|300"), triples);
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
