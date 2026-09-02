package com.nexagres.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
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
 * As {@code OracleCacheInvalidationIntegrationTest}, but for a real MySQL Connector/J client
 * (real {@code COM_STMT_PREPARE}/{@code COM_STMT_EXECUTE} wire protocol) -- proves {@code
 * WARP_CACHE_TABLES} warming and write-side invalidation for both UPDATE and DELETE when the SQL
 * originates from mywire rather than pgwire.
 */
class MySqlCacheInvalidationIntegrationTest {

    private static final String ADMIN_TOKEN = "mysql-cache-test-token";
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
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_CACHE_TABLES", TABLE)
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
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

    private void assertMywireCacheHitRecorded() throws Exception {
        String summary = metricsSummary();
        assertTrue(summary.contains("\"protocol\":\"mywire\"") && summary.contains("\"outcome\":\"cache_hit\""),
                "expected a mywire cache_hit in rttByOutcome -- got: " + summary);
    }

    @Test
    void updateThroughARealMySqlClientInvalidatesTheCachedSelect() throws Exception {
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
            // rather than relying on mywire not sharing that bug.
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
            assertMywireCacheHitRecorded();

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
                                    + "'pending' entry from before the MySQL-side UPDATE");
                }
            }
        }
    }

    @Test
    void deleteThroughARealMySqlClientInvalidatesTheCachedSelect() throws Exception {
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
            assertMywireCacheHitRecorded();

            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + TABLE + " WHERE id = ?")) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertFalse(rs.next(),
                            "the post-DELETE read must see zero rows, not a stale cached row from "
                                    + "before the MySQL-side DELETE");
                }
            }
        }
    }
}
