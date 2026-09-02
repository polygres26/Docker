package com.nexagres.wire.orawire;

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
 * Real, live proof that the {@code WARP_CACHE_TABLES} arbitrary-SELECT cache tier -- {@link
 * com.nexagres.wire.cluster.CacheStage#handleCacheableSelect} and its write-side {@link
 * com.nexagres.wire.cluster.CacheStage#invalidate(String)} -- is correctly warmed and invalidated
 * when the SQL originates from a real Oracle client (ojdbc11 thin driver, real TNS/TTC wire
 * protocol) rather than pgwire. Every existing cache test in this project ({@code
 * RowCacheSharingIntegrationTest}, {@code FederationCacheAndShardingIntegrationTest}) only ever
 * drives writes through pgwire; {@link com.nexagres.wire.cluster.CacheStage#cacheKey} is built
 * from the TRANSLATED sql/bindParams that reach Postgres, so this should be dialect-agnostic by
 * construction -- this test is the actual proof, not an assumption.
 */
class OracleCacheInvalidationIntegrationTest {

    private static final String ADMIN_TOKEN = "oracle-cache-test-token";
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
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_CACHE_TABLES", TABLE)
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
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

    /** Same idea as {@code RowCacheSharingIntegrationTest}'s own summary assertion: not just "the
     * value is right" (a real uncached round trip would also be right) but specifically that a
     * {@code cache_hit} was recorded under the orawire protocol label -- proof the second read
     * actually came from {@code CacheStage}'s in-memory cache, not a lucky race with Postgres. */
    private void assertOrawireCacheHitRecorded() throws Exception {
        String summary = metricsSummary();
        assertTrue(summary.contains("\"protocol\":\"orawire\"") && summary.contains("\"outcome\":\"cache_hit\""),
                "expected an orawire cache_hit in rttByOutcome -- got: " + summary);
    }

    @Test
    void updateThroughARealOracleClientInvalidatesTheCachedSelect() throws Exception {
        postgres = RealPostgres.start();
        warp = startWarp();

        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, status VARCHAR(20))");
                stmt.executeUpdate("INSERT INTO " + TABLE + " (id, status) VALUES (1, 'pending')");
                conn.commit();
            }

            String selectSql = "SELECT status FROM " + TABLE + " WHERE id = ?";
            // First read: cache MISS, populates the cache.
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("pending", rs.getString(1));
                }
            }
            // Second, identical read -- a FRESH PreparedStatement, not the same one re-executed:
            // ojdbc11's thin driver sends the re-execute of an already-open cursor as a real,
            // distinct FUNC_REEXECUTE wire call (see RequestLoop#handleReexecute), and re-running
            // ps.executeQuery() twice on the same PreparedStatement here was found live to
            // deadlock the whole session (both client and server left waiting on each other, no
            // exception, no timeout) -- reproduced with WARP_CACHE_TABLES unset too, so it's a
            // general orawire cursor-reexecution bug, not specific to this cache tier. See
            // OracleRepeatedQueryIsolationTest for the isolated repro; tracked as a real,
            // unfixed bug, not something this test works around silently.
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("pending", rs.getString(1));
                }
            }
            assertOrawireCacheHitRecorded();

            // A real Oracle-dialect UPDATE with a bind parameter, over the same real TNS/TTC
            // connection -- must invalidate CacheStage's whole-table cache entries for this table.
            try (PreparedStatement ps = conn.prepareStatement("UPDATE " + TABLE + " SET status = ? WHERE id = ?")) {
                ps.setString(1, "shipped");
                ps.setInt(2, 1);
                ps.executeUpdate();
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("shipped", rs.getString(1),
                            "the post-UPDATE read must see the fresh value, not a stale cached "
                                    + "'pending' entry from before the Oracle-side UPDATE");
                }
            }
        }
    }

    @Test
    void deleteThroughARealOracleClientInvalidatesTheCachedSelect() throws Exception {
        postgres = RealPostgres.start();
        warp = startWarp();

        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, status VARCHAR(20))");
                stmt.executeUpdate("INSERT INTO " + TABLE + " (id, status) VALUES (2, 'pending')");
                conn.commit();
            }

            String selectSql = "SELECT status FROM " + TABLE + " WHERE id = ?";
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }
            // Warm the cache with a second identical read -- a FRESH PreparedStatement, not a
            // re-execute of the first one; see the sibling UPDATE test's comment for why.
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }
            assertOrawireCacheHitRecorded();

            // A real Oracle-dialect DELETE with a bind parameter -- must also invalidate the
            // cache (IS_WRITE_OR_DDL matches delete, not just update/insert).
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + TABLE + " WHERE id = ?")) {
                ps.setInt(1, 2);
                ps.executeUpdate();
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertFalse(rs.next(),
                            "the post-DELETE read must see zero rows, not a stale cached row from "
                                    + "before the Oracle-side DELETE");
                }
            }
        }
    }
}
