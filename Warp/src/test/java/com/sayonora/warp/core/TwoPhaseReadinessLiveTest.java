package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.LocalPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Two real Postgres servers, one with prepared transactions on and one with them off. Opt-in: WARP_TEST_BROWNOUT_PG_BIN. */
class TwoPhaseReadinessLiveTest {

    @Test
    void readsMaxPreparedTransactionsFromEachShard() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "needs WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("twophase");
        LocalPostgres on = LocalPostgres.primary(bin, dir, "on", LocalPostgres.freePort());
        LocalPostgres off = LocalPostgres.primary(bin, dir, "off", LocalPostgres.freePort());
        try {
            try (Connection c = off.conn(); Statement st = c.createStatement()) {
                st.execute("ALTER SYSTEM SET max_prepared_transactions = 0");
            }
            off.stop("fast");
            off.start();
            BackendRegistry registry = BackendRegistry.fromConfig("default=" + on.url() + "|warp|x;a=" + on.url() + "|warp|x;b=" + off.url() + "|warp|x", null);
            var rule = new RouterStage.TableShardRule("orders", Pattern.compile("\\borders\\b"), "id", ShardingStrategy.hash(List.of("a", "b")));
            var reports = new TwoPhaseReadiness(registry, () -> List.of(rule)).check();
            assertEquals(TwoPhaseReadiness.State.READY, reports.get(0).state(), reports.get(0).detail());
            assertEquals(TwoPhaseReadiness.State.NOT_READY, reports.get(1).state(), reports.get(1).detail());
            assertTrue(reports.get(1).detail().contains("max_prepared_transactions=0"), reports.get(1).detail());
            try (var warp = com.sayonora.warp.testsupport.WarpProcess.builder().pgBackend("127.0.0.1", on.port(), "postgres", "warp", "x")
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + on.url() + "|warp|x;a=" + on.url() + "|warp|x;b=" + off.url() + "|warp|x")
                    .env("WARP_TABLE_SHARDS", "orders:hash:id:a,b")
                    .env("WARP_ADMIN_TOKEN", com.sayonora.warp.testsupport.BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:" + warp.metricsPort() + "/api/sharding/2pc"))
                        .header("Authorization", "Bearer " + com.sayonora.warp.testsupport.BrownoutHarness.TOKEN).build();
                String body = java.net.http.HttpClient.newHttpClient().send(req, java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                assertTrue(body.contains("\"backend\":\"b\"") && body.contains("NOT_READY") && body.contains("restart Postgres"), body);
            }
        } finally {
            on.stop("immediate");
            off.stop("immediate");
        }
    }
}
