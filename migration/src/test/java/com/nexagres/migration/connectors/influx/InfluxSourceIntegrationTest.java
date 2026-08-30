package com.nexagres.migration.connectors.influx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.PolywireGrpcSink;
import com.nexagres.migration.testsupport.PolyWireProcess;
import com.nexagres.migration.testsupport.RealPostgres;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof, real infrastructure throughout, using this session's own established
 * approach: a real running Polywire instance fronting influxwire stands in as a genuine InfluxDB
 * v1 HTTP-API source, using plain HTTP writes (the real line protocol) -- no external InfluxDB
 * needed. Two separate Polywire instances: a source (influxwire only) and a target (grpc).
 *
 * <p>Proves: (1) a pre-existing backlog of points replicates with tags/fields correctly split
 * per this connector's explicit tag-key declaration; (2) a LIVE point, written after the
 * coordinator is already running, also replicates -- the real timestamp-cursor CDC mechanism,
 * not just a one-time snapshot.
 */
class InfluxSourceIntegrationTest {

    private static void waitUntil(Duration timeout, Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private static void writeLine(HttpClient http, int port, String database, String line) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/write?db=" + database))
                .POST(HttpRequest.BodyPublishers.ofString(line, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("write failed (" + resp.statusCode() + "): " + resp.body());
        }
    }

    private static Long targetRowCount(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"polywire_influx_readings\"");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (java.sql.SQLException e) {
            if ("42P01".equals(e.getSQLState())) {
                return null;
            }
            throw e;
        }
    }

    @Test
    void backlogAndLivePointsReplicateWithTagsAndFieldsCorrectlySplit() throws Exception {
        try (RealPostgres sourcePostgres = RealPostgres.start();
                PolyWireProcess sourcePolywire = PolyWireProcess.builder()
                        .pgBackend(sourcePostgres.host(), sourcePostgres.port(), sourcePostgres.database(), sourcePostgres.username(), sourcePostgres.password())
                        .frontend("influxwire", "POLYWIRE_INFLUXWIRE_PORT")
                        .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                        .frontend("mywire", "POLYWIRE_MYWIRE_PORT")
                        .frontend("mssqlwire", "POLYWIRE_MSSQLWIRE_PORT")
                        .frontend("mongowire", "POLYWIRE_MONGOWIRE_PORT")
                        .frontend("boltwire", "POLYWIRE_BOLTWIRE_PORT")
                        .frontend("orawire", "POLYWIRE_ORAWIRE_PORT")
                        .frontend("dynamowire", "POLYWIRE_DYNAMOWIRE_PORT")
                        .frontend("sqswire", "POLYWIRE_SQSWIRE_PORT")
                        .frontend("oswire", "POLYWIRE_OSWIRE_PORT")
                        .frontend("mcp", "POLYWIRE_MCP_PORT")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start();
                RealPostgres targetPostgres = RealPostgres.start();
                PolyWireProcess targetPolywire = PolyWireProcess.builder()
                        .pgBackend(targetPostgres.host(), targetPostgres.port(), targetPostgres.database(), targetPostgres.username(), targetPostgres.password())
                        .frontend("grpc", "POLYWIRE_GRPC_PORT")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            HttpClient http = HttpClient.newHttpClient();
            int sourcePort = sourcePolywire.port("influxwire");
            // Real InfluxDB line protocol: measurement,tag=val field=val timestamp (nanoseconds).
            long baseNanos = System.currentTimeMillis() * 1_000_000L;
            for (int i = 0; i < 5; i++) {
                writeLine(http, sourcePort, "mydb", "readings,sensor=temp-" + i + " value=" + (20.0 + i) + " " + (baseNanos + i * 1_000_000_000L));
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password());
            checkpoints.ensureSchema();

            InfluxSource source = new InfluxSource("localhost", sourcePort, "mydb", "readings", Set.of("sensor"));
            PolywireGrpcSink sink = new PolywireGrpcSink("localhost", targetPolywire.port("grpc"), targetPostgres.username(), targetPostgres.password());
            Coordinator coordinator = new Coordinator(source, sink, checkpoints, 1);
            Thread coordinatorThread = new Thread(() -> {
                try {
                    coordinator.run();
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-coordinator");
            coordinatorThread.start();
            try {
                // Proof #1: the pre-existing backlog (5 points) fully replicates.
                waitUntil(Duration.ofSeconds(20), () -> {
                    Long count = targetRowCount(targetPostgres);
                    return count != null && count == 5;
                });
                try (Connection conn = DriverManager.getConnection(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "SELECT tags, fields FROM \"polywire_influx_readings\" WHERE tags->>'sensor' = 'temp-2'");
                        ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("temp-2", com.google.gson.JsonParser.parseString(rs.getString("tags")).getAsJsonObject().get("sensor").getAsString());
                    assertEquals(22.0, com.google.gson.JsonParser.parseString(rs.getString("fields")).getAsJsonObject().get("value").getAsDouble());
                }

                // Proof #2: a LIVE point, written after the coordinator is already running,
                // replicates too -- the real timestamp-cursor CDC mechanism, not just a snapshot.
                // Must be LATER than every backlog point (the last was at baseNanos + 4s) -- the
                // timestamp-cursor design only ever moves forward (see InfluxSource's own
                // javadoc), so an earlier "live" point would be a genuinely backdated write this
                // connector is honestly documented as not handling, not a bug to work around here.
                writeLine(http, sourcePort, "mydb", "readings,sensor=temp-live value=99.9 " + (baseNanos + 10_000_000_000L));
                waitUntil(Duration.ofSeconds(20), () -> {
                    Long count = targetRowCount(targetPostgres);
                    return count != null && count == 6;
                });
            } finally {
                source.close();
                coordinatorThread.interrupt();
                coordinatorThread.join(Duration.ofSeconds(10).toMillis());
                sink.close();
            }
        }
    }
}
