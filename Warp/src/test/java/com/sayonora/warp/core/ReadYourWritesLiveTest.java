package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * A session must read its own writes from a replica. The replica's replay is paused, so it lags by more than the old fixed window: with
 * WARP_READ_YOUR_WRITES=false the session's read after the window is routed to the stale replica and misses its own row; with it on, the read
 * stays on the primary until the replica has applied the write, then goes back to the replica. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN.
 */
class ReadYourWritesLiveTest {

    private static long routedReads(WarpProcess warp, int replicaPort) throws Exception {
        String json = BrownoutHarness.http("GET", "http://localhost:" + warp.metricsPort() + "/api/replicas", null);
        long total = 0;
        for (var p : com.google.gson.JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("primaries")) {
            for (var r : p.getAsJsonObject().getAsJsonArray("replicas")) {
                if (r.getAsJsonObject().get("url").getAsString().contains(":" + replicaPort + "/")) {
                    total += r.getAsJsonObject().get("routedReads").getAsLong();
                }
            }
        }
        return total;
    }

    private static void exec(LocalPostgres pg, String sql) throws Exception {
        try (Connection c = pg.conn(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private void run(boolean readYourWrites) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("ryw");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(bin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        try {
            replica = LocalPostgres.replicaOf(bin, dir, "replica", primary, LocalPostgres.freePort());
            exec(primary, "create table orders (id int primary key, customer_id int, amount int)");
            String spec = "default=" + cfg.url() + "|warp|secret;pg=" + primary.url() + "|warp|secret||" + replica.url() + "~120";
            try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", spec)
                    .env("WARP_TABLE_SHARDS", "orders:hash:customer_id:pg")
                    .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                    .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "300")
                    .env("WARP_READ_YOUR_WRITES", String.valueOf(readYourWrites))
                    .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                Thread.sleep(4000); // replica sampled
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    // the replica is serving reads before the test starts to hold it back
                    long before = routedReads(warp, replica.port());
                    for (int i = 0; i < 4; i++) {
                        st.executeQuery("select count(*) from orders where customer_id = 1").close();
                    }
                    assertTrue(routedReads(warp, replica.port()) > before, "reads reach the replica while it is current");

                    exec(replica, "select pg_wal_replay_pause()");
                    Thread.sleep(500);
                    assertEquals(1, st.executeUpdate("insert into orders (id, customer_id, amount) values (1, 1, 10)"));
                    Thread.sleep(1500); // longer than the 300 ms window the old routing relied on
                    int seen;
                    try (ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = 1")) {
                        rs.next();
                        seen = rs.getInt(1);
                    }
                    if (readYourWrites) {
                        assertEquals(1, seen, "the session reads its own write while the replica is behind");
                    } else {
                        assertEquals(0, seen, "control: the old window routes the read to the stale replica");
                    }

                    exec(replica, "select pg_wal_replay_resume()");
                    Thread.sleep(1500);
                    if (readYourWrites) {
                        long mid = routedReads(warp, replica.port());
                        for (int i = 0; i < 6; i++) {
                            try (ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = 1")) {
                                rs.next();
                                assertEquals(1, rs.getInt(1));
                            }
                        }
                        assertTrue(routedReads(warp, replica.port()) > mid, "once the replica has applied the write, reads go back to it");
                    }
                }
            }
        } finally {
            primary.stop("immediate");
            if (replica != null) {
                replica.stop("immediate");
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void aSessionReadsItsOwnWriteWhileTheReplicaIsBehind() throws Exception {
        run(true);
    }

    @Test
    void withoutItTheOldWindowServesAStaleReadFromTheLaggingReplica() throws Exception {
        run(false);
    }
}
