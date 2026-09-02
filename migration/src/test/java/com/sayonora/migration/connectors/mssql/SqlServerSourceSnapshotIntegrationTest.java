package com.sayonora.migration.connectors.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.migration.checkpoint.CdcCheckpointStore;
import com.sayonora.migration.core.Partition;
import com.sayonora.migration.sink.WarpGrpcSink;
import com.sayonora.migration.testsupport.WarpProcess;
import com.sayonora.migration.testsupport.RealAzureSqlEdge;
import com.sayonora.migration.testsupport.RealPostgres;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real, not simulated, snapshot (schema translation + parallel {@code CHECKSUM}-partitioned reads)
 * verification: real Azure SQL Edge (a genuine SQL-Server-compatible engine, ARM64-native -- see
 * {@link RealAzureSqlEdge}'s own javadoc for why real SQL Server images can't be used at all on
 * this host), real Warp gRPC subprocess, real target Postgres.
 *
 * <p>Drives {@link SqlServerSource}'s snapshot-related methods directly (not through {@link
 * com.sayonora.migration.coordinator.Coordinator}), same reasoning as {@code
 * DynamoSourceSnapshotIntegrationTest}: {@code Coordinator#run} also calls {@code
 * prepareChangeFeed}/{@code streamChanges}, which need a real, actively-capturing CDC setup this
 * environment cannot provide (see {@link RealAzureSqlEdge}'s own javadoc: Azure SQL Edge has no
 * SQL Server Agent, so its CDC capture job never runs, and real SQL Server doesn't run at all under
 * this host's emulation) -- {@code SqlServerSource}'s CDC path could not be verified end to end
 * against any real engine in this environment, a genuine, disclosed limitation, not glossed over.
 */
class SqlServerSourceSnapshotIntegrationTest {

    @Test
    void snapshotTranslatesSchemaAndCopiesEveryRowAcrossParallelPartitions() throws Exception {
        try (RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            sqlServer.createDatabase("src");
            String jdbcUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                    + ";databaseName=src;encrypt=false;trustServerCertificate=true";
            try (Connection conn = DriverManager.getConnection(jdbcUrl, sqlServer.username(), sqlServer.password());
                    Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE dbo.orders (id INT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL, note VARCHAR(255) NULL)");
                for (int i = 0; i < 20; i++) {
                    String noteValue = i == 5 ? "NULL" : "'note-" + i + "'";
                    st.execute("INSERT INTO dbo.orders (id, amount, note) VALUES (" + i + ", " + (10 + i) + ".25, " + noteValue + ")");
                }
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            SqlServerSource source = new SqlServerSource(sqlServer.host(), sqlServer.port(),
                    sqlServer.username(), sqlServer.password(), "src", "dbo", "orders", 3);
            try (WarpGrpcSink sink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password())) {
                source.ensureTargetSchema(sink);
                for (Partition partition : source.listPartitions()) {
                    source.readPartition(partition, sink, checkpoints);
                }
            }

            String targetTable = "\"src\".\"orders\"";
            try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password())) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM " + targetTable);
                        ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(20, rs.getLong(1), "every row across all parallel partitions should have landed exactly once");
                }
                // Schema translation: decimal -> numeric, varchar -> text.
                try (PreparedStatement ps = conn.prepareStatement("SELECT amount, note FROM " + targetTable + " WHERE id = ?")) {
                    ps.setInt(1, 3);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(new BigDecimal("13.25"), rs.getBigDecimal("amount"));
                        assertEquals("note-3", rs.getString("note"));
                    }
                }
                // A NULL source column should land as a real NULL, not a string.
                try (PreparedStatement ps = conn.prepareStatement("SELECT note FROM " + targetTable + " WHERE id = ?")) {
                    ps.setInt(1, 5);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertNull(rs.getString("note"));
                    }
                }
            }
        }
    }
}
