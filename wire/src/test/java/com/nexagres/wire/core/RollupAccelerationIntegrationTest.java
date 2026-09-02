package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The only existing rollup-related test in this project ({@code RollupSuggestionDraftIntegrationTest})
 * covers the AI suggestion-DRAFTING endpoint -- it never proves {@link RollupStage}/{@code
 * RollupRefreshJob}/{@code RollupDefinition} (the actual acceleration mechanism, real Calcite-based
 * materialized-view matching against a real {@code warp_rollup_*} table) does anything at all. This
 * is the first functional test of the real mechanism.
 *
 * <p>Proof technique: materialize the rollup at Warp startup against a known dataset, then insert
 * an EXTRA row directly into the source table -- bypassing Warp entirely, so the rollup table is
 * now provably stale relative to the live source. A client query matching the rollup's shape then
 * gets ONE of two possible answers: the OLD (rollup) total, or the NEW (live) total including the
 * extra row. Getting back the OLD total is only possible if {@link RollupStage} actually
 * substituted the query against {@code warp_rollup_daily_totals} instead of running it live --
 * correctness alone (either answer would be "a number") can't distinguish acceleration happening
 * from it silently never engaging.
 */
class RollupAccelerationIntegrationTest {

    private RealPostgres postgres;
    private WarpProcess warp;
    private Path rollupYamlFile;

    @AfterEach
    void stopInfra() throws IOException {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
        if (rollupYamlFile != null) Files.deleteIfExists(rollupYamlFile);
    }

    @Test
    void aClientQueryMatchingARollupsShapeIsAcceleratedFromTheMaterializedTableNotRecomputedLive() throws Exception {
        postgres = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER, quantity INTEGER)");
            st.execute("INSERT INTO orders (id, item_id, quantity) VALUES (1, 1, 2), (2, 1, 3), (3, 2, 5)");
        }

        rollupYamlFile = Files.createTempFile("warp-rollup-test", ".yaml");
        Files.writeString(rollupYamlFile, """
                rollups:
                  - name: daily_totals
                    backend: default
                    source_table: orders
                    group_by: [item_id]
                    aggregations: ["SUM(quantity) AS total_quantity"]
                    refresh_interval_minutes: 60
                    max_staleness_minutes: 60
                """);

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_ROLLUP_DEFINITIONS_FILE", rollupYamlFile.toString())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        // Confirm the rollup table was really materialized at startup with the ORIGINAL data --
        // item 1: 2+3=5, item 2: 5.
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT item_id, total_quantity FROM warp_rollup_daily_totals ORDER BY item_id")) {
            Map<Integer, Integer> byItem = new LinkedHashMap<>();
            while (rs.next()) {
                byItem.put(rs.getInt(1), rs.getInt(2));
            }
            assertEquals(Map.of(1, 5, 2, 5), byItem, "rollup table must be materialized at startup with the original data");
        }

        // Now make the rollup provably STALE: a real INSERT straight into the source table,
        // bypassing Warp entirely -- item 1's real total is now 2+3+100=105, but the rollup table
        // (untouched) still says 5.
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement()) {
            st.execute("INSERT INTO orders (id, item_id, quantity) VALUES (4, 1, 100)");
        }

        // A client query matching the rollup's own shape (same GROUP BY, same aggregation) --
        // if RollupStage's Calcite-based substitution engaged, this reads from the STALE
        // warp_rollup_daily_totals table and must return the OLD total (5), not the live one (105).
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT item_id, SUM(quantity) AS total_quantity FROM orders GROUP BY item_id ORDER BY item_id")) {
            Map<Integer, Integer> byItem = new LinkedHashMap<>();
            while (rs.next()) {
                byItem.put(rs.getInt(1), rs.getInt(2));
            }
            assertTrue(byItem.containsKey(1), "expected a row for item_id=1");
            assertEquals(5, byItem.get(1),
                    "expected the STALE rollup total (5), not the live total (105) -- getting 105 "
                            + "here would mean RollupStage never substituted the query at all, just "
                            + "ran it live against the real (already-updated) orders table");
        }
    }
}
