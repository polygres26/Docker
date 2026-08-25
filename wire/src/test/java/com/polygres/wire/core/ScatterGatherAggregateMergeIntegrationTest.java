package com.polygres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.polygres.wire.testsupport.PolyWireProcess;
import com.polygres.wire.testsupport.RealPostgres;
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
 * hash/range/consistent-hash routing needed here -- POLYWIRE_ROUTER_SHARD_TABLES sends every
 * statement against the "orders" table through scatter-gather across both), each seeded with its
 * own slice of data so the merged result can only be correct if real cross-shard aggregation ran,
 * not a naive per-shard row concatenation. See ScatterGatherAggregateMerge and
 * RoutingBackendExecutor#executeScatterGather for the implementation this test verifies, and
 * ScatterGatherAggregateMergeTest for the unit-level coverage of the merge arithmetic itself.
 */
class ScatterGatherAggregateMergeIntegrationTest {

    private static RealPostgres shard1;
    private static RealPostgres shard2;
    private static PolyWireProcess polywire;

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

        polywire = PolyWireProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                .env("POLYWIRE_BACKENDS", backends)
                .env("POLYWIRE_SHARD_BACKENDS", "shard1,shard2")
                .env("POLYWIRE_ROUTER_SHARD_TABLES", "public")
                .env("POLYWIRE_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (polywire != null) polywire.close();
        if (shard2 != null) shard2.close();
        if (shard1 != null) shard1.close();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres";
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
    void groupByMergesTheSameGroupFoundOnBothShards() throws SQLException {
        // Deliberately no ORDER BY -- ScatterGatherAggregateMerge's plan doesn't parse/re-apply
        // ORDER BY to the merged result (a real, separate scope boundary from what this test is
        // proving), so row order here is merge-insertion order, not a sorted order. Assert by
        // group key instead of position, same approach as ScatterGatherAggregateMergeTest.
        java.util.Map<String, int[]> byRegion = new java.util.LinkedHashMap<>();
        java.util.Map<String, Double> sumByRegion = new java.util.LinkedHashMap<>();
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT region, COUNT(*), SUM(total) FROM public.orders GROUP BY region")) {
            while (rs.next()) {
                byRegion.put(rs.getString(1), new int[] {rs.getInt(2)});
                sumByRegion.put(rs.getString(1), rs.getDouble(3));
            }
        }
        assertEquals(3, byRegion.size(), "must be exactly one merged row per distinct region, not one per shard");
        assertEquals(3, byRegion.get("east")[0], "east appears on both shards (2 rows + 1 row) and must merge into one group");
        assertEquals(60.0, sumByRegion.get("east"), 0.0001);
        assertEquals(1, byRegion.get("south")[0]);
        assertEquals(100.0, sumByRegion.get("south"), 0.0001);
        assertEquals(1, byRegion.get("west")[0]);
        assertEquals(5.0, sumByRegion.get("west"), 0.0001);
    }
}
