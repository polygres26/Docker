package com.nexagres.wire.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the workload capture/replay feature: two real {@code Main} subprocesses,
 * each with {@code POLYWIRE_CAPTURE_ENABLED=true} and its own in-memory
 * {@link WorkloadCaptureBuffer}, both registered in the same {@code polywire_nodes} table (same
 * backing Postgres), and a real {@code WorkloadReplayer} subprocess that discovers both, pulls
 * both instances' {@code /api/capture}, and merges by wall-clock -- proving cross-instance
 * ordering is real, not just each instance's own local order.
 */
class WorkloadCaptureIntegrationTest {

    private static final String ADMIN_TOKEN = "test-admin-token";

    private static RealPostgres postgres;
    private static PolyWireProcess instanceA;
    private static PolyWireProcess instanceB;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        instanceA = PolyWireProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                .env("POLYWIRE_CAPTURE_ENABLED", "true")
                .env("POLYWIRE_ADMIN_TOKEN", ADMIN_TOKEN)
                // Two full embedded-Ignite JVMs on one box (each defaulting to local static
                // discovery) is unrelated overhead/contention this test doesn't need -- capture
                // doesn't touch the KV caches that are the only reason Main starts Ignite at all.
                .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                .start();
        instanceB = PolyWireProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                .env("POLYWIRE_CAPTURE_ENABLED", "true")
                .env("POLYWIRE_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                // Every other protocol's listen port defaults to a fixed value (PolyWireProcess's
                // .frontend() only randomizes pgwire, the one this test actually drives) -- a
                // second Main process on the same box needs every one of them moved off instance
                // A's, or it fails to bind at startup.
                .env("POLYWIRE_MYWIRE_PORT", "13307")
                .env("POLYWIRE_MSSQLWIRE_PORT", "14334")
                .env("POLYWIRE_ORAWIRE_PORT", "11522")
                .env("POLYWIRE_GRPC_PORT", "7071")
                .env("POLYWIRE_MONGOWIRE_PORT", "27018")
                .env("POLYWIRE_DYNAMOWIRE_PORT", "18001")
                .env("POLYWIRE_SQSWIRE_PORT", "9325")
                .env("POLYWIRE_OSWIRE_PORT", "9201")
                .env("POLYWIRE_MCP_PORT", "18011")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (instanceA != null) {
            instanceA.close();
        }
        if (instanceB != null) {
            instanceB.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    private Connection connectThroughPolyWire(PolyWireProcess instance) throws SQLException {
        String url = "jdbc:postgresql://localhost:" + instance.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    private Connection connectDirectToPostgres() throws SQLException {
        return DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
    }

    @Test
    void capturedEntriesAreReadableFromEachInstancesOwnAdminApi() throws Exception {
        try (Connection conn = connectThroughPolyWire(instanceA); Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
            stmt.execute("SELECT 2");
        }

        String body = httpGet(instanceA.metricsPort(), "/api/capture?since=0&limit=100");
        assertTrue(body.contains("\"sqlText\":\"SELECT 1\""), "expected instance A's capture buffer to contain its own traffic: " + body);
        assertTrue(body.contains("\"sqlText\":\"SELECT 2\""), "expected instance A's capture buffer to contain its own traffic: " + body);
    }

    @Test
    void replayerMergesTwoInstancesByWallClockIntoOneGlobalOrder() throws Exception {
        try (Connection direct = connectDirectToPostgres(); Statement setup = direct.createStatement()) {
            setup.execute("CREATE TABLE IF NOT EXISTS replay_merge_check (id SERIAL PRIMARY KEY, n INT)");
            setup.execute("DELETE FROM replay_merge_check");
        }

        // Instance A gets n=1..5 first, then (after a real pause so the wall-clock gap is
        // unambiguous even across two separate hosts/processes) instance B gets n=6..10. If the
        // replayer really merges by wall-clock across instances -- not by discovery order, not by
        // per-instance grouping -- the replayed sequence must come out 1..10, interleavable in
        // principle but strictly increasing here since A fully precedes B in time.
        try (Connection conn = connectThroughPolyWire(instanceA); Statement stmt = conn.createStatement()) {
            for (int n = 1; n <= 5; n++) {
                stmt.execute("INSERT INTO replay_merge_check (n) VALUES (" + n + ")");
            }
        }
        Thread.sleep(500);
        try (Connection conn = connectThroughPolyWire(instanceB); Statement stmt = conn.createStatement()) {
            for (int n = 6; n <= 10; n++) {
                stmt.execute("INSERT INTO replay_merge_check (n) VALUES (" + n + ")");
            }
        }

        try (Connection direct = connectDirectToPostgres(); Statement st = direct.createStatement()) {
            // Clear so the post-replay id order can only have come from the replay itself.
            st.execute("DELETE FROM replay_merge_check");
        }

        runReplayer();

        List<Integer> replayedOrder = new ArrayList<>();
        try (Connection direct = connectDirectToPostgres();
                Statement st = direct.createStatement();
                ResultSet rs = st.executeQuery("SELECT n FROM replay_merge_check ORDER BY id")) {
            while (rs.next()) {
                replayedOrder.add(rs.getInt("n"));
            }
        }

        assertTrue(replayedOrder.size() >= 10, "expected at least the 10 captured inserts to replay: " + replayedOrder);
        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), replayedOrder.subList(0, 10),
                "WorkloadReplayer must merge both instances' captured statements into one global "
                        + "wall-clock order, not replay each instance's traffic as a separate block");
    }

    private static String httpGet(int port, String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + port + path).toURL().openConnection();
        conn.setRequestProperty("Authorization", "Bearer " + ADMIN_TOKEN);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private void runReplayer() throws Exception {
        // Same --add-opens PolyWireProcess uses for Main -- embedded Ignite (on the classpath as
        // a dependency, its JDBC driver auto-registered via ServiceLoader) reflectively opens
        // several java.base packages during static init, which the module system blocks by
        // default from Java 17 onward. WorkloadReplayer never touches Ignite itself, but
        // DriverManager eagerly loads every registered driver -- including Ignite's -- the moment
        // any JDBC connection is opened, so it hits the same restriction Main does.
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin/java");
        command.add("--add-opens=java.base/jdk.internal.access=ALL-UNNAMED");
        command.add("--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED");
        command.add("--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.lang.reflect=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.io=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.util=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.util.concurrent=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.math=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.time=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.text=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.net=ALL-UNNAMED");
        command.add("--add-opens=java.sql/java.sql=ALL-UNNAMED");
        command.add("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("com.nexagres.wire.capture.WorkloadReplayer");

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.environment().put("POLYWIRE_HOST", postgres.host());
        pb.environment().put("POLYWIRE_PORT", String.valueOf(postgres.port()));
        pb.environment().put("POLYWIRE_DATABASE", postgres.database());
        pb.environment().put("POLYWIRE_USER", postgres.username());
        pb.environment().put("POLYWIRE_PASSWORD", postgres.password());
        pb.environment().put("POLYWIRE_ADMIN_TOKEN", ADMIN_TOKEN);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        boolean finished = process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(finished, "WorkloadReplayer did not finish in time. Output so far:\n" + output);
        assertEquals(0, process.exitValue(), "WorkloadReplayer exited non-zero. Output:\n" + output);
    }
}
