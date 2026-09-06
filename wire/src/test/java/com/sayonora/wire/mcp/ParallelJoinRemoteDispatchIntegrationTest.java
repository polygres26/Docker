package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real, end-to-end proof of Phase 1b: TWO separate real Warp processes -- a coordinator and a peer
 * -- sharing the same config-primary Postgres (so {@code NodeRegistry}'s heartbeat rows are
 * mutually visible), with the coordinator dispatching a real federated-join partition to the peer
 * over mutual TLS and getting back correctly joined rows. Deliberately forces the whole join onto
 * the single live peer ({@code WARP_PARALLEL_JOIN_THREADS=1} + exactly one live peer means {@code
 * remoteCount == 1 == threadCount}) so a successful result can only mean the remote path actually
 * ran -- a silent fall-back-to-local would still produce a correct RESULT, so this test also
 * captures the coordinator's own log output and asserts it shows a successful remote dispatch, not
 * the "falling back to local execution" warning {@code ParallelJoinExecutor} logs on failure.
 */
class ParallelJoinRemoteDispatchIntegrationTest {

    private RealPostgres ordersDb;
    private RealPostgres customersDb;
    private WarpProcess peerNode;
    private WarpProcess coordinatorNode;

    @AfterEach
    void stopInfra() {
        if (coordinatorNode != null) coordinatorNode.close();
        if (peerNode != null) peerNode.close();
        if (customersDb != null) customersDb.close();
        if (ordersDb != null) ordersDb.close();
    }

    /** Every OTHER Warp integration test in this codebase runs exactly one {@code WarpProcess} at
     * a time, so none of them notice that most of Warp's listener ports (gRPC, pgwire, mywire,
     * orawire, mssqlwire, mongowire, boltwire, dynamowire, sqswire, oswire, influxwire, HTTP/HTTPS,
     * TLS variants) default to FIXED port numbers, bound unconditionally on every startup
     * regardless of which {@code frontend()} a test happens to care about -- only {@code
     * WARP_METRICS_PORT} is randomized unconditionally, by {@code WarpProcess} itself. This test is
     * the first to run TWO real Warp processes on the same host simultaneously, which means every
     * one of those fixed ports must be explicitly randomized for at least one of the two instances
     * or the second process's startup collides on a bind -- a real, newly-discovered gap in the
     * shared test harness's assumptions, not something to route around silently. */
    private static WarpProcess.Builder withEveryFixedPortRandomized(WarpProcess.Builder builder) throws java.io.IOException {
        String[] envVars = {
                "WARP_GRPC_PORT", "WARP_GRPC_TLS_PORT", "WARP_TLS_PORT",
                "WARP_PGWIRE_PORT", "WARP_MYWIRE_PORT", "WARP_ORAWIRE_PORT", "WARP_MSSQLWIRE_PORT",
                "WARP_MONGOWIRE_PORT", "WARP_BOLTWIRE_PORT", "WARP_DYNAMOWIRE_PORT",
                "WARP_SQSWIRE_PORT", "WARP_OSWIRE_PORT", "WARP_INFLUXWIRE_PORT", "WARP_A2A_PORT",
                "WARP_HTTP_PORT", "WARP_HTTPS_PORT",
        };
        for (String envVar : envVars) {
            builder = builder.env(envVar, String.valueOf(findFreePort()));
        }
        return builder;
    }

    /** Real gRPC/Netty TLS enforces hostname verification against the certificate's CN by default
     * -- {@code WarpPeerGrpcServer.openPeerChannel} dials {@code NodeRow.host()}, which is
     * whatever {@code NodeRegistry.resolveHost()} advertised (this machine's real hostname, e.g.
     * {@code Kumars-MacBook-Pro.local}, not the literal string {@code "localhost"}). A test cert
     * minted with {@code CN=localhost} would fail the handshake the instant the coordinator dials
     * the peer by its REAL advertised host -- found live, not hypothetical -- so the CN here must
     * match exactly what {@link com.sayonora.wire.config.NodeRegistry#resolveHost()} returns. */
    private static Path generateSelfSignedKeystore(Path dir, String password) throws Exception {
        String hostname = com.sayonora.wire.config.NodeRegistry.resolveHost();
        Path keystorePath = dir.resolve("peer-dispatch-test.p12");
        Process keytool = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", "warp-peer-dispatch-test",
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "30",
                "-keystore", keystorePath.toString(), "-storetype", "PKCS12",
                "-storepass", password, "-keypass", password,
                "-dname", "CN=" + hostname + ", OU=warp test, O=warp, L=Test, ST=Test, C=US")
                .redirectErrorStream(true)
                .start();
        boolean finished = keytool.waitFor(30, TimeUnit.SECONDS);
        if (!finished || keytool.exitValue() != 0) {
            throw new IllegalStateException("keytool failed: " + new String(keytool.getInputStream().readAllBytes()));
        }
        return keystorePath;
    }

    private static int findFreePort() throws java.io.IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
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

    private static List<String> sortedRowStrings(String responseBody) {
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
    void aFederatedJoinPartitionActuallyRunsOnARealRemotePeer() throws Exception {
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

        String password = "test-password";
        Path tempDir = Files.createTempDirectory("warp-peer-dispatch-test");
        Path keystorePath = generateSelfSignedKeystore(tempDir, password);

        // Real, found-live gotcha: WARP_BACKENDS (and the rest of Warp's config) is centrally
        // versioned in warp_config on the shared config-primary Postgres -- whichever process
        // starts FIRST publishes it, and every later process ADOPTS that published version rather
        // than re-deriving its own from its own env vars. Starting the COORDINATOR first means its
        // real WARP_BACKENDS becomes the one published version; the peer inheriting it too is
        // harmless (it never receives an execute_sql/query_federated call in this test, so it never
        // acts on WARP_BACKENDS at all).
        String backends = "default=" + ordersDb.jdbcUrl() + "|" + ordersDb.username() + "|" + ordersDb.password()
                + ";customers_backend=" + customersDb.jdbcUrl() + "|" + customersDb.username() + "|" + customersDb.password();
        coordinatorNode = withEveryFixedPortRandomized(WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .env("WARP_PARALLEL_JOIN_ENABLED", "true")
                .env("WARP_PARALLEL_JOIN_MIN_ROWS", "0")
                // Exactly one partition + exactly one live peer -- the ENTIRE join is forced onto
                // the peer (remoteCount = min(peerCount=1, threadCount=1) = 1), so a correct result
                // can only mean the remote path genuinely ran.
                .env("WARP_PARALLEL_JOIN_THREADS", "1")
                .env("WARP_PARALLEL_JOIN_REMOTE_ENABLED", "true")
                .env("WARP_PEER_TLS_KEYSTORE", keystorePath.toString())
                .env("WARP_PEER_TLS_KEYSTORE_PASSWORD", password))
                .start();

        // The PEER node never sees ordersDb/customersDb at all -- Phase 1a's whole design point is
        // that a peer only ever matches rows it's handed, never touches an original backend. It
        // inherits the coordinator's already-published WARP_BACKENDS (see above) but never acts on
        // it, since it's never sent an execute_sql/query_federated call in this test.
        peerNode = withEveryFixedPortRandomized(WarpProcess.builder()
                .pgBackend(ordersDb.host(), ordersDb.port(), ordersDb.database(), ordersDb.username(), ordersDb.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .frontend("peerGrpc", "WARP_PEER_GRPC_PORT")
                .env("WARP_PEER_TLS_KEYSTORE", keystorePath.toString())
                .env("WARP_PEER_TLS_KEYSTORE_PASSWORD", password)
                .env("WARP_OTEL_ENDPOINT", "disabled"))
                .start();

        // The heartbeat that lets the coordinator discover the peer runs every ~10s -- give it a
        // real moment to land before the query, rather than racing the very first cycle.
        Thread.sleep(11_000);

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        HttpResponse<String> response;
        try {
            System.setOut(new PrintStream(new TeeOutputStream(originalOut, captured), true, StandardCharsets.UTF_8));
            response = mcpCall(coordinatorNode.port("mcp"),
                    "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id");
        } finally {
            System.out.flush();
            System.setOut(originalOut);
        }
        String log = captured.toString(StandardCharsets.UTF_8);

        assertEquals(200, response.statusCode());
        List<String> rows = sortedRowStrings(response.body());
        assertEquals(2, rows.size());
        assertTrue(response.body().contains("alice") && response.body().contains("bob"),
                "the join must still return correct rows via the remote path -- got: " + response.body());
        assertTrue(log.contains("executed via the parallel join engine"),
                "the parallel engine must have actually engaged for this query");
        assertFalse(log.contains("falling back to local execution for this one partition"),
                "with exactly one live peer and one partition, the join must have run remotely, "
                        + "not silently fallen back -- captured log: " + log);
    }

    /** Tees every write to two underlying streams -- lets the test capture the child processes'
     * drained stdout ({@code WarpProcess} prints it via {@code System.out}) while still letting it
     * reach the real console/surefire output. */
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
