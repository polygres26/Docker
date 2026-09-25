package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the highest-risk gap fixed against ShardingSphere: a real pgwire client,
 * a real Main subprocess sharded across two independent real Postgres containers (no actual
 * hash/range/consistent-hash routing needed here -- WARP_ROUTER_SHARD_TABLES sends every
 * statement against the "orders" table through scatter-gather across both), each seeded with its
 * own slice of data so the merged result can only be correct if real cross-shard aggregation ran,
 * not a naive per-shard row concatenation. See ScatterGatherAggregateMerge and
 * RoutingBackendExecutor#executeScatterGather for the implementation this test verifies, and
 * ScatterGatherAggregateMergeTest for the unit-level coverage of the merge arithmetic itself.
 */
class ScatterGatherAggregateMergeIntegrationTest {

    private static RealPostgres shard1;
    private static RealPostgres shard2;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();

        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, region text, total numeric)");
            st.execute("INSERT INTO orders (region, total) VALUES ('east', 10), ('east', 20), ('west', 5)");
        }
        try (Connection c = DriverManager.getConnection(shard2.jdbcUrl(), shard2.username(), shard2.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, region text, total numeric)");
            st.execute("INSERT INTO orders (region, total) VALUES ('east', 30), ('south', 100)");
        }

        String backends = "shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard2=" + shard2.jdbcUrl() + "|" + shard2.username() + "|" + shard2.password();

        warp = WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_SHARD_BACKENDS", "shard1,shard2")
                .env("WARP_ROUTER_SHARD_TABLES", "public")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (shard2 != null) shard2.close();
        if (shard1 != null) shard1.close();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, shard1.username(), shard1.password());
    }

    @Test
    void countStarIsSummedAcrossShardsNotConcatenatedAsSeparateRows() throws SQLException {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM public.orders")) {
            assertEquals(true, rs.next());
            assertEquals(5, rs.getInt(1), "3 rows on shard1 + 2 on shard2 must merge into one row totalling 5");
            assertEquals(false, rs.next(), "a merged COUNT(*) must be exactly one row, not one per shard");
        }
    }

    @Test
    void sumAndAvgAreRealCrossShardArithmeticNotPerShardValues() throws SQLException {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT SUM(total), AVG(total) FROM public.orders")) {
            assertEquals(true, rs.next());
            assertEquals(165.0, rs.getDouble(1), 0.0001, "10+20+5+30+100 = 165");
            assertEquals(33.0, rs.getDouble(2), 0.0001, "165/5 = 33, not an average-of-per-shard-averages");
        }
    }

    @Test
    void groupByMergesTheSameGroupFoundOnBothShardsAndOrderByIsAppliedToTheMergedResult() throws SQLException {
        // Now that ScatterGatherOrderLimit exists, ORDER BY on the merged (post-aggregate) result
        // is real and testable by position, not just by group key.
        java.util.List<String> regionsInOrder = new java.util.ArrayList<>();
        java.util.Map<String, int[]> byRegion = new java.util.LinkedHashMap<>();
        java.util.Map<String, Double> sumByRegion = new java.util.LinkedHashMap<>();
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT region, COUNT(*), SUM(total) FROM public.orders GROUP BY region ORDER BY region")) {
            while (rs.next()) {
                regionsInOrder.add(rs.getString(1));
                byRegion.put(rs.getString(1), new int[] {rs.getInt(2)});
                sumByRegion.put(rs.getString(1), rs.getDouble(3));
            }
        }
        assertEquals(3, byRegion.size(), "must be exactly one merged row per distinct region, not one per shard");
        assertEquals(java.util.List.of("east", "south", "west"), regionsInOrder,
                "ORDER BY region must sort the fully-merged groups, not just each shard's own contribution");
        assertEquals(3, byRegion.get("east")[0], "east appears on both shards (2 rows + 1 row) and must merge into one group");
        assertEquals(60.0, sumByRegion.get("east"), 0.0001);
        assertEquals(1, byRegion.get("south")[0]);
        assertEquals(100.0, sumByRegion.get("south"), 0.0001);
        assertEquals(1, byRegion.get("west")[0]);
        assertEquals(5.0, sumByRegion.get("west"), 0.0001);
    }

    @Test
    void plainSelectOrderByLimitReturnsTheCorrectGlobalTopNNotUpToLimitTimesShardCount() throws SQLException {
        // The follow-up gap: totals across both shards are [10, 20, 5, 30, 100]. Before the fix,
        // each shard applied "ORDER BY total DESC LIMIT 3" locally and the results were just
        // concatenated -- shard1 (10,20,5) would contribute its own top 3 (20,10,5) and shard2
        // (30,100) its own top 2, for up to 5 rows total, not the correct global top 3.
        java.util.List<Double> totalsInOrder = new java.util.ArrayList<>();
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT total FROM public.orders ORDER BY total DESC LIMIT 3")) {
            while (rs.next()) {
                totalsInOrder.add(rs.getDouble(1));
            }
        }
        assertEquals(java.util.List.of(100.0, 30.0, 20.0), totalsInOrder,
                "must be the true global top 3 by total, descending, not up to 3-per-shard concatenated");
    }

    @Test
    void plainSelectLimitWithNoOrderByStillCapsTotalRowsAcrossShards() throws SQLException {
        // Even without ORDER BY, LIMIT must cap the TOTAL row count across all shards combined --
        // before the fix this could return up to limit * shardCount rows.
        int count = 0;
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT total FROM public.orders LIMIT 2")) {
            while (rs.next()) {
                count++;
            }
        }
        assertEquals(2, count, "LIMIT 2 must return exactly 2 rows total, not 2 per shard");
    }
}
