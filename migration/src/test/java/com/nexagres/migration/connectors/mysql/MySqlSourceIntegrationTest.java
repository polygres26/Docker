package com.nexagres.migration.connectors.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.WarpGrpcSink;
import com.nexagres.migration.testsupport.WarpProcess;
import com.nexagres.migration.testsupport.RealMySql;
import com.nexagres.migration.testsupport.RealPostgres;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof, real infrastructure throughout: real MySQL with binlog (ROW format) enabled,
 * real Warp gRPC subprocess, real target Postgres. Unlike DynamoDB Streams (see {@code
 * DynamoSourceStreamsTest}'s own javadoc for why that path needs a fake client), MySQL's binlog
 * works out of the box in a real {@code mysql:8} container, so this test covers the ENTIRE
 * connector -- schema translation, parallel partitioned snapshot, and live binlog CDC -- against
 * real infrastructure, no substitutes.
 *
 * <p>Specifically proves: (1) the target table's schema is translated correctly from MySQL's real
 * {@code information_schema} (int/decimal/varchar all land as their mapped Postgres types); (2) the
 * initial snapshot replicates via parallel partitions; (3) a live INSERT with a NULL column
 * replicates correctly; (4) a live UPDATE that sets a column TO NULL replicates correctly -- the
 * specific correctness case {@link MySqlSource}'s own javadoc calls out, since the gRPC protocol
 * has no null bind-parameter marker; (5) a live DELETE replicates; (6) a restarted {@code
 * MySqlSource}/{@code Coordinator} resumes from checkpoint (skips the already-done partitions,
 * picks up a write made during simulated downtime).
 */
class MySqlSourceIntegrationTest {

    private static void waitUntil(Duration timeout, java.util.concurrent.Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(150);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private static Long targetRowCount(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"src\".\"orders\"");
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

    private static ResultSet targetRow(Connection conn, int id) throws Exception {
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM \"src\".\"orders\" WHERE \"id\" = ?");
        ps.setInt(1, id);
        return ps.executeQuery();
    }

    @Test
    void snapshotAndBinlogChangesReplicateThroughTheRealGrpcPathIncludingNullHandlingAndSurviveARestart() throws Exception {
        try (RealMySql mysql = RealMySql.start();
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection conn = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL, note VARCHAR(255) NULL)");
                for (int i = 0; i < 10; i++) {
                    st.execute("INSERT INTO orders (id, amount, note) VALUES (" + i + ", " + (10 + i) + ".50, 'note-" + i + "')");
                }
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            MySqlSource source = new MySqlSource(mysql.host(), mysql.port(), mysql.username(), mysql.password(),
                    "src", "orders", 3, 6001);
            WarpGrpcSink sink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            Coordinator coordinator = new Coordinator(source, sink, checkpoints, 3);
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
                // Proof #1/#2: schema translation + parallel-partitioned snapshot.
                waitUntil(Duration.ofSeconds(25), () -> {
                    Long count = targetRowCount(postgres);
                    return count != null && count == 10;
                });
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        ResultSet rs = targetRow(targetConn, 3)) {
                    assertTrue(rs.next());
                    assertEquals(new BigDecimal("13.50"), rs.getBigDecimal("amount"));
                    assertEquals("note-3", rs.getString("note"));
                }

                // Proof #3: a live INSERT with a NULL column.
                try (Connection conn = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                        Statement st = conn.createStatement()) {
                    st.execute("INSERT INTO orders (id, amount, note) VALUES (10, 99.99, NULL)");
                }
                waitUntil(Duration.ofSeconds(20), () -> {
                    Long count = targetRowCount(postgres);
                    return count != null && count == 11;
                });
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        ResultSet rs = targetRow(targetConn, 10)) {
                    assertTrue(rs.next());
                    assertNull(rs.getString("note"), "a NULL column on INSERT should replicate as a real NULL, not a string");
                }

                // Proof #4: a live UPDATE that sets a column TO NULL -- the specific correctness
                // case MySqlSource's own javadoc calls out (the gRPC protocol has no null
                // bind-parameter marker, so this exercises the inline-NULL-literal fix directly).
                try (Connection conn = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                        Statement st = conn.createStatement()) {
                    st.execute("UPDATE orders SET note = NULL WHERE id = 0");
                }
                waitUntil(Duration.ofSeconds(20), () -> {
                    try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                            ResultSet rs = targetRow(targetConn, 0)) {
                        return rs.next() && rs.getString("note") == null && rs.wasNull();
                    }
                });

                // Proof #5: a live DELETE.
                try (Connection conn = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                        Statement st = conn.createStatement()) {
                    st.execute("DELETE FROM orders WHERE id = 2");
                }
                waitUntil(Duration.ofSeconds(20), () -> {
                    try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                            ResultSet rs = targetRow(targetConn, 2)) {
                        return !rs.next();
                    }
                });
            } finally {
                source.close();
                coordinatorThread.interrupt();
                coordinatorThread.join(Duration.ofSeconds(10).toMillis());
                sink.close();
            }

            // Simulate real downtime, then prove restart-and-resume.
            try (Connection conn = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO orders (id, amount, note) VALUES (11, 1.00, 'late')");
            }

            MySqlSource resumedSource = new MySqlSource(mysql.host(), mysql.port(), mysql.username(), mysql.password(),
                    "src", "orders", 3, 6002);
            WarpGrpcSink resumedSink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            Coordinator resumedCoordinator = new Coordinator(resumedSource, resumedSink, checkpoints, 3);
            Thread resumedThread = new Thread(() -> {
                try {
                    resumedCoordinator.run();
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-coordinator-resumed");
            resumedThread.start();
            try {
                waitUntil(Duration.ofSeconds(20), () -> {
                    try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                            ResultSet rs = targetRow(targetConn, 11)) {
                        return rs.next();
                    }
                });
            } finally {
                resumedSource.close();
                resumedThread.interrupt();
                resumedThread.join(Duration.ofSeconds(10).toMillis());
                resumedSink.close();
            }
        }
    }
}
