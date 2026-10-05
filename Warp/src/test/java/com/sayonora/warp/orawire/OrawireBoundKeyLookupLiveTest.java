package com.sayonora.warp.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * A primary-key lookup with an Oracle bind variable must use the key's index. orawire used to hand the NUMBER bind to
 * Postgres as {@code numeric}, which turns {@code id = ?} on a bigint column into {@code id::numeric = $1}: a sequential
 * scan on every call (7.3 ms instead of 0.37 ms on 300,000 rows). Needs a Postgres ({@code WARP_TEST_ORAWIRE_PG_PORT},
 * superuser "warp", trust auth); it creates and drops its own table. Asserts on the ratio of bound to literal lookup time
 * (Postgres's scan counters were tried first but other backends flush them seconds late, so they cannot be read reliably).
 */
class OrawireBoundKeyLookupLiveTest {

    @Test
    void aBoundKeyLookupUsesTheIndexInsteadOfScanningTheTable() throws Exception {
        String pgPort = System.getenv("WARP_TEST_ORAWIRE_PG_PORT");
        Assumptions.assumeTrue(pgPort != null);
        try (Connection pg = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + pgPort + "/postgres", "warp", "secret");
                var st = pg.createStatement()) {
            st.execute("drop table if exists warp_config cascade"); // so the QoS env below is honoured
            st.execute("drop table if exists bind_idx");
            st.execute("create table bind_idx (id bigint primary key, n int)");
            st.execute("insert into bind_idx select g, g from generate_series(1, 200000) g");
            st.execute("analyze bind_idx");
            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                    .frontend("orawire", "WARP_ORAWIRE_PORT")
                    .env("WARP_QOS_RATE_PER_SEC", "1000000")
                    .env("WARP_QOS_BURST", "1000000")
                    .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {
                Properties p = new Properties();
                p.setProperty("user", "warp");
                p.setProperty("password", "secret");
                int lookups = 300;
                try (Connection c = DriverManager.getConnection("jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/pg", p)) {
                    double literalMs = timeLookups(c, lookups, false);
                    double boundMs = timeLookups(c, lookups, true);
                    // before the fix a bound lookup scanned all 200,000 rows (about 20x the literal's time); with the index it
                    // is about the same or faster. The generous bound keeps a noisy machine from failing it.
                    assertTrue(boundMs < literalMs * 4 + 0.5, String.format(
                            "bound key lookup %.2f ms per call vs %.2f ms with a literal: it is not using the index", boundMs, literalMs));
                }
            } finally {
                st.execute("drop table if exists bind_idx");
            }
        }
    }

    /** Average milliseconds per primary-key lookup, with a bind variable or a literal. */
    private static double timeLookups(Connection c, int lookups, boolean bound) throws Exception {
        for (int i = 1; i <= 50; i++) { // warm-up
            lookup(c, i * 3L, bound);
        }
        long t0 = System.nanoTime();
        for (int i = 1; i <= lookups; i++) {
            lookup(c, i * 500L, bound);
        }
        return (System.nanoTime() - t0) / 1e6 / lookups;
    }

    private static void lookup(Connection c, long id, boolean bound) throws Exception {
        if (bound) {
            try (var ps = c.prepareStatement("select n from bind_idx where id = ?")) {
                ps.setLong(1, id);
                try (var rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(id, rs.getInt(1));
                }
            }
        } else {
            try (var st = c.createStatement(); var rs = st.executeQuery("select n from bind_idx where id = " + id)) {
                assertTrue(rs.next());
                assertEquals(id, rs.getInt(1));
            }
        }
    }

    private static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
