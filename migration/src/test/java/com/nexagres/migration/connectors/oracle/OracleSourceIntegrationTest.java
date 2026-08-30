package com.nexagres.migration.connectors.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.PolywireGrpcSink;
import com.nexagres.migration.testsupport.PolyWireProcess;
import com.nexagres.migration.testsupport.RealOracle;
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
 * End-to-end proof, real infrastructure throughout: real Oracle Database Free (LogMiner works
 * natively on this host, unlike SQL Server CDC -- see {@code RealOracle}'s own javadoc), real
 * Polywire gRPC subprocess, real target Postgres. Unlike {@code SqlServerSourceSnapshotIntegrationTest}
 * (which could only cover the snapshot path), this covers the ENTIRE connector -- schema
 * translation, parallel {@code ORA_HASH}-partitioned snapshot, AND live LogMiner-based CDC
 * (insert/update/delete, including a NULL-valued column) -- against real infrastructure, no
 * substitutes.
 *
 * <p>Database-level (MINIMAL) supplemental logging is pre-enabled here via the privileged {@code
 * system} connection, simulating what a real DBA does once -- {@link OracleSource} itself only
 * handles TABLE-level (ALL COLUMNS) supplemental logging, which the schema owner can grant to
 * their own tables without DBA privilege (see {@code OracleSource#ensureSupplementalLogging}'s own
 * javadoc for why database-level logging needs a privileged connection this connector cannot
 * assume it has).
 */
class OracleSourceIntegrationTest {

    private static void waitUntil(Duration timeout, java.util.concurrent.Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private static Long targetRowCount(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"MIGTEST\".\"ORDERS\"");
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
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM \"MIGTEST\".\"ORDERS\" WHERE \"ID\" = ?");
        ps.setBigDecimal(1, new BigDecimal(id));
        return ps.executeQuery();
    }

    @Test
    void snapshotAndLogMinerChangesReplicateThroughTheRealGrpcPath() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "POLYWIRE_GRPC_PORT")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            oracle.createSchema("migtest", "MigPass123");
            // Must run against the CDB ROOT service, not the PDB -- see RealOracle#rootJdbcUrl's
            // own javadoc for why (a real ORA-01031 confirmed live against the PDB service, even
            // as system). Simulates a DBA's one-time setup.
            try (Connection conn = DriverManager.getConnection(oracle.rootJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement st = conn.createStatement()) {
                st.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
            }

            String schemaJdbcUrl = oracle.jdbcUrl();
            try (Connection conn = DriverManager.getConnection(schemaJdbcUrl, "migtest", "MigPass123");
                    Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE orders (id NUMBER PRIMARY KEY, amount NUMBER(10,2) NOT NULL, note VARCHAR2(255) NULL)");
                for (int i = 0; i < 10; i++) {
                    String noteValue = i == 4 ? "NULL" : "'note-" + i + "'";
                    st.execute("INSERT INTO orders (id, amount, note) VALUES (" + i + ", " + (10 + i) + ".50, " + noteValue + ")");
                }
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            OracleSource source = new OracleSource(oracle.host(), oracle.port(), oracle.serviceName(),
                    "migtest", "MigPass123", "migtest", "orders", 3);
            PolywireGrpcSink sink = new PolywireGrpcSink("localhost", polywire.port("grpc"), postgres.username(), postgres.password());
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
                // Proof #1/#2: schema translation + parallel ORA_HASH-partitioned snapshot.
                waitUntil(Duration.ofSeconds(40), () -> {
                    Long count = targetRowCount(postgres);
                    return count != null && count == 10;
                });
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        ResultSet rs = targetRow(targetConn, 3)) {
                    assertTrue(rs.next());
                    assertEquals(0, new BigDecimal("13.50").compareTo(rs.getBigDecimal("amount")),
                            "numeric with no declared scale doesn't pad trailing zeros -- compare by value, not by scale");
                    assertEquals("note-3", rs.getString("note"));
                }
                // A NULL source column should land as a real NULL, not text "NULL".
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        ResultSet rs = targetRow(targetConn, 4)) {
                    assertTrue(rs.next());
                    assertNull(rs.getString("note"));
                }

                // Proof #3: a live LogMiner-replicated INSERT.
                try (Connection conn = DriverManager.getConnection(schemaJdbcUrl, "migtest", "MigPass123");
                        Statement st = conn.createStatement()) {
                    st.execute("INSERT INTO orders (id, amount, note) VALUES (10, 99.99, 'late')");
                }
                waitUntil(Duration.ofSeconds(30), () -> {
                    Long count = targetRowCount(postgres);
                    return count != null && count == 11;
                });

                // Proof #4: a live LogMiner-replicated UPDATE (only the changed column's redo).
                try (Connection conn = DriverManager.getConnection(schemaJdbcUrl, "migtest", "MigPass123");
                        Statement st = conn.createStatement()) {
                    st.execute("UPDATE orders SET amount = 500.00 WHERE id = 0");
                }
                waitUntil(Duration.ofSeconds(30), () -> {
                    try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                            ResultSet rs = targetRow(targetConn, 0)) {
                        return rs.next() && new BigDecimal("500.00").compareTo(rs.getBigDecimal("amount")) == 0;
                    }
                });

                // Proof #5: a live LogMiner-replicated DELETE.
                try (Connection conn = DriverManager.getConnection(schemaJdbcUrl, "migtest", "MigPass123");
                        Statement st = conn.createStatement()) {
                    st.execute("DELETE FROM orders WHERE id = 2");
                }
                waitUntil(Duration.ofSeconds(30), () -> {
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
        }
    }
}
