package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * As {@code OracleCacheInvalidationIntegrationTest}, but for a real SQL Server JDBC client
 * (mssql-jdbc, real TDS wire protocol, bind parameters sent as an RPC {@code sp_executesql} call)
 * -- proves {@code WARP_CACHE_TABLES} warming and write-side invalidation for both UPDATE and
 * DELETE when the SQL originates from mssqlwire rather than pgwire.
 */
class MssqlCacheInvalidationIntegrationTest {

    private static final String ADMIN_TOKEN = "mssql-cache-test-token";
    private static final String TABLE = "warp_cache_orders";

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private WarpProcess startWarp() throws Exception {
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_CACHE_TABLES", TABLE)
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    private String metricsSummary() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + "/api/metrics/summary"))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(5))
                .GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }

    private void assertMssqlwireCacheHitRecorded() throws Exception {
        String summary = metricsSummary();
        assertTrue(summary.contains("\"protocol\":\"mssqlwire\"") && summary.contains("\"outcome\":\"cache_hit\""),
                "expected a mssqlwire cache_hit in rttByOutcome -- got: " + summary);
    }

    @Test
    void updateThroughARealSqlServerClientInvalidatesTheCachedSelect() throws Exception {
        postgres = RealPostgres.start();
        warp = startWarp();

        try (Connection conn = connect()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, status VARCHAR(20))");
                stmt.executeUpdate("INSERT INTO " + TABLE + " (id, status) VALUES (1, 'pending')");
            }

            String selectSql = "SELECT status FROM " + TABLE + " WHERE id = ?";
            // Two separate PreparedStatement instances, not one re-executed -- see
            // OracleRepeatedQueryIsolationTest's javadoc for why orawire's cache test avoids
            // reusing a PreparedStatement across reads; kept the same shape here defensively
            // rather than relying on mssqlwire not sharing that bug.
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("pending", rs.getString(1));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("pending", rs.getString(1));
                }
            }
            assertMssqlwireCacheHitRecorded();

            try (PreparedStatement ps = conn.prepareStatement("UPDATE " + TABLE + " SET status = ? WHERE id = ?")) {
                ps.setString(1, "shipped");
                ps.setInt(2, 1);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("shipped", rs.getString(1),
                            "the post-UPDATE read must see the fresh value, not a stale cached "
                                    + "'pending' entry from before the SQL-Server-side UPDATE");
                }
            }
        }
    }

    @Test
    void deleteThroughARealSqlServerClientInvalidatesTheCachedSelect() throws Exception {
        postgres = RealPostgres.start();
        warp = startWarp();

        try (Connection conn = connect()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, status VARCHAR(20))");
                stmt.executeUpdate("INSERT INTO " + TABLE + " (id, status) VALUES (2, 'pending')");
            }

            String selectSql = "SELECT status FROM " + TABLE + " WHERE id = ?";
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }
            assertMssqlwireCacheHitRecorded();

            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + TABLE + " WHERE id = ?")) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertFalse(rs.next(),
                            "the post-DELETE read must see zero rows, not a stale cached row from "
                                    + "before the SQL-Server-side DELETE");
                }
            }
        }
    }
}
